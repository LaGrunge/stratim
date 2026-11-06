// Copyright 2026 LaGrunge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package woodpecker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"time"
)

// The defaults of EventsFollowOptions.
const (
	// DefaultEventsMaxAttempts is how many attempts in a row to connect may
	// fail before a follower gives up.
	DefaultEventsMaxAttempts = 10
	// DefaultEventsRetry is the delay to reconnect after until the server
	// tells its own.
	DefaultEventsRetry = 3 * time.Second
	// DefaultEventsMaxBackoff bounds the delay to reconnect after.
	DefaultEventsMaxBackoff = 30 * time.Second
	// DefaultEventsIdleTimeout is how long a stream may stay without a
	// frame before it is taken for dead: three pings.
	DefaultEventsIdleTimeout = 45 * time.Second
	// DefaultEventsStableAfter is how long a stream has to stay up for the
	// attempts to count from zero again.
	DefaultEventsStableAfter = 30 * time.Second
)

// errStreamDropped ends a stream the follower dropped.
var errStreamDropped = errors.New("the stream was dropped")

// eventsUnknownReason stands for the reason of a frame without one.
const eventsUnknownReason = "unknown"

// EventsFollowOptions tell what an EventsFollower follows and how it
// reconnects; the zero values are the defaults.
type EventsFollowOptions struct {
	// PipelineEventsOptions are the filters, and in LastEventID the cursor
	// to start from.
	PipelineEventsOptions
	// MaxAttempts is how many attempts in a row to connect may fail, or
	// streams end before they were stable, before the follower gives up.
	MaxAttempts int
	// MaxBackoff bounds the delay to reconnect after.
	MaxBackoff time.Duration
	// IdleTimeout is how long a stream may stay without a frame, pings
	// included, before it is closed and reconnected to.
	IdleTimeout time.Duration
	// StableAfter is how long a stream has to stay up, to its last frame,
	// for the attempts to count from zero again; a stream that gave a
	// pipeline event is stable too. A stream taken for dead never is.
	StableAfter time.Duration
	// Jitter takes the delay to reconnect after and returns the one to
	// wait; nil waits a random delay from 0 to it ("full jitter").
	Jitter func(time.Duration) time.Duration
	// OnReconnect is told each delay before reconnecting, and why.
	OnReconnect func(delay time.Duration, cause error)
}

// FollowedFrameKind tells what a FollowedFrame is.
type FollowedFrameKind int

const (
	// FollowedHello starts each stream, also after a reconnect.
	FollowedHello FollowedFrameKind = iota + 1
	// FollowedReset tells that events were missed: load the state.
	FollowedReset
	// FollowedEvent is a pipeline event.
	FollowedEvent
	// FollowedUndecodable is a frame that should be a pipeline event but
	// does not decode: load the state, it may have been one that matters.
	FollowedUndecodable
)

// FollowedFrame is a frame an EventsFollower read, decoded.
type FollowedFrame struct {
	Kind FollowedFrameKind
	// ID is the frame's id, if it has one.
	ID string
	// Name is the name of the frame, e.g. hello or step.failed.
	Name string
	// Raw is the frame's JSON.
	Raw []byte

	Hello *EventsHello
	// Reset has a reason, "unknown" when the frame gave none.
	Reset *EventsReset
	Event *PipelineEvent
	// Err is why an undecodable frame did not decode.
	Err error
}

// EventsEndedError is a stream the server ended for good, e.g. with the
// reason unauthorized.
type EventsEndedError struct {
	Reason string
}

func (e *EventsEndedError) Error() string {
	return "the server ended the event stream: " + e.Reason
}

// EventsGaveUpError is a follower that gave up reconnecting.
type EventsGaveUpError struct {
	Attempts int
	Err      error
}

func (e *EventsGaveUpError) Error() string {
	return fmt.Sprintf("gave up on the event stream after %d attempts in a row: %v", e.Attempts, e.Err)
}

func (e *EventsGaveUpError) Unwrap() error { return e.Err }

// IsRetryableEventsError tells whether an error to connect to a stream, or
// to wait, may go away on its own: an error of the connection, a timeout,
// 408, 429 or a 5xx. A 4xx such as 401 or 404 does not.
func IsRetryableEventsError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrEventsFrameTooLarge) {
		return false
	}
	if clientErr, ok := errors.AsType[*ClientError](err); ok {
		return clientErr.StatusCode == http.StatusRequestTimeout || clientErr.StatusCode == http.StatusTooManyRequests ||
			clientErr.StatusCode >= http.StatusInternalServerError
	}
	if _, ok := errors.AsType[*EventsEndedError](err); ok {
		return false
	}
	if _, ok := errors.AsType[*EventsGaveUpError](err); ok {
		return false
	}
	// an error of the connection: refused, reset, a timeout, an end
	return true
}

// EventsBackoff is the delay before the attempt to reconnect after the
// failures in a row, before jitter: the server's retry delay, doubled for
// each failure after the first, at most max.
func EventsBackoff(retry time.Duration, failures int, maxDelay time.Duration) time.Duration {
	if retry <= 0 {
		retry = DefaultEventsRetry
	}
	delay := retry
	for i := 1; i < failures && delay < maxDelay; i++ {
		delay *= 2
	}
	return min(delay, maxDelay)
}

// FullJitter waits a random delay from 0 to the one given.
func FullJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return rand.N(d + 1) //nolint:gosec // a delay, not a secret
}

// EventsFollower follows a stream of pipeline events across reconnects: it
// goes on from the last id it read, after the server's retry delay with an
// exponential backoff and jitter, takes a stream without a frame for
// IdleTimeout for dead, and gives up after MaxAttempts failures in a row.
// It is used from one goroutine.
type EventsFollower struct {
	client Client
	opt    EventsFollowOptions

	cursor string
	retry  time.Duration

	// failures are the attempts in a row that failed; tooSlow the streams
	// the server ended for a client too slow, which only grows.
	failures, tooSlow int
	// cause is why the stream is to be reconnected to, after wait.
	cause error
	wait  time.Duration
	// gaveUp ends the follower.
	gaveUp error

	conn        *followedConn
	connectedAt time.Time
	// lastFrameAt is when the stream gave its last frame, pings included.
	lastFrameAt time.Time
	delivered   bool
}

// followedConn is a stream being read. It is ended by canceling its
// request, never by closing its body while it is read: its reader closes it.
type followedConn struct {
	cancel context.CancelCauseFunc
	frames chan frameOrError
	done   chan struct{}
}

type frameOrError struct {
	frame *EventsFrame
	err   error
}

// NewEventsFollower follows the pipeline events of the client.
func NewEventsFollower(client Client, opt EventsFollowOptions) *EventsFollower {
	if opt.MaxAttempts <= 0 {
		opt.MaxAttempts = DefaultEventsMaxAttempts
	}
	if opt.MaxBackoff <= 0 {
		opt.MaxBackoff = DefaultEventsMaxBackoff
	}
	if opt.IdleTimeout <= 0 {
		opt.IdleTimeout = DefaultEventsIdleTimeout
	}
	if opt.StableAfter <= 0 {
		opt.StableAfter = DefaultEventsStableAfter
	}
	if opt.Jitter == nil {
		opt.Jitter = FullJitter
	}
	return &EventsFollower{client: client, opt: opt, cursor: opt.LastEventID}
}

// Cursor is the id of the last frame read that had one.
func (f *EventsFollower) Cursor() string { return f.cursor }

// Next reads the next frame, reconnecting as needed. It fails with the
// context's error, an *EventsEndedError, an *EventsGaveUpError, a
// *ClientError the server answered that a retry does not help, or
// ErrEventsFrameTooLarge.
func (f *EventsFollower) Next(ctx context.Context) (*FollowedFrame, error) {
	for {
		if f.gaveUp != nil {
			return nil, f.gaveUp
		}
		if f.conn == nil {
			if err := f.connect(ctx); err != nil {
				return nil, err
			}
		}
		idle := time.NewTimer(f.opt.IdleTimeout)
		var got frameOrError
		select {
		case <-ctx.Done():
			idle.Stop()
			f.Close()
			return nil, ctx.Err()
		case <-idle.C:
			f.ended(fmt.Errorf("no frame for %s", f.opt.IdleTimeout), false)
			continue
		case got = <-f.conn.frames:
			idle.Stop()
		}
		if got.err != nil {
			if errors.Is(got.err, ErrEventsFrameTooLarge) {
				f.Close()
				return nil, got.err
			}
			if errors.Is(got.err, io.EOF) {
				got.err = errors.New("the stream ended")
			}
			f.ended(got.err, true)
			continue
		}
		frame, err := f.take(got.frame)
		if err != nil || frame != nil {
			return frame, err
		}
	}
}

// take handles a frame read: it returns it decoded, or none for a frame
// about the stream that the follower handles itself.
func (f *EventsFollower) take(raw *EventsFrame) (*FollowedFrame, error) {
	f.lastFrameAt = time.Now()
	if raw.ID != "" {
		f.cursor = raw.ID
	}
	if raw.Retry > 0 {
		f.retry = raw.Retry
	}
	frame := &FollowedFrame{ID: raw.ID, Name: raw.Event, Raw: raw.Data}
	switch raw.Event {
	case "":
		return nil, nil // a ping or the retry delay
	case EventsFrameHello:
		frame.Kind, frame.Hello = FollowedHello, new(EventsHello)
		_ = raw.Decode(frame.Hello)
		return frame, nil
	case EventsFrameReset:
		frame.Kind, frame.Reset = FollowedReset, new(EventsReset)
		if raw.Decode(frame.Reset) != nil || frame.Reset.Reason == "" {
			frame.Reset.Reason = eventsUnknownReason
		}
		return frame, nil
	case EventsFrameError:
		var e EventsError
		if raw.Decode(&e) != nil || e.Reason == "" {
			e.Reason = eventsUnknownReason
		}
		if e.Reason == EventsErrorTooSlow {
			f.tooSlow++
			f.drop()
			f.cause = errors.New("the server dropped the stream as too slow")
			f.wait = 0
			if f.tooSlow > 1 {
				f.wait = f.opt.Jitter(EventsBackoff(f.retry, f.tooSlow-1, f.opt.MaxBackoff))
			}
			return nil, nil
		}
		f.Close()
		return nil, &EventsEndedError{Reason: e.Reason}
	}
	if len(raw.Data) == 0 {
		return nil, nil
	}
	frame.Kind, frame.Event = FollowedEvent, new(PipelineEvent)
	if err := decodePipelineEvent(raw.Data, frame.Event); err != nil {
		frame.Kind, frame.Event, frame.Err = FollowedUndecodable, nil, err
		return frame, nil
	}
	f.delivered = true
	return frame, nil
}

// connect connects, after the delay to reconnect after if the stream is
// reconnected to.
func (f *EventsFollower) connect(ctx context.Context) error {
	for {
		if f.cause != nil {
			if f.opt.OnReconnect != nil {
				f.opt.OnReconnect(f.wait, f.cause)
			}
			t := time.NewTimer(f.wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}
		opt := f.opt.PipelineEventsOptions
		opt.LastEventID = f.cursor
		connCtx, cancel := context.WithCancelCause(ctx)
		reader, err := f.client.PipelineEvents(connCtx, opt)
		if err != nil {
			cancel(nil)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !IsRetryableEventsError(err) {
				return err
			}
			if f.failed(err); f.gaveUp != nil {
				return f.gaveUp
			}
			continue
		}
		conn := &followedConn{cancel: cancel, frames: make(chan frameOrError), done: make(chan struct{})}
		go func() {
			defer reader.Close()
			for {
				frame, err := reader.Next()
				select {
				case conn.frames <- frameOrError{frame, err}:
				case <-conn.done:
					return
				}
				if err != nil {
					return
				}
			}
		}()
		f.conn, f.connectedAt, f.lastFrameAt, f.delivered = conn, time.Now(), time.Time{}, false
		f.cause = nil
		return nil
	}
}

// failed counts a failure in a row and sets the delay to reconnect after,
// or gives up after too many.
func (f *EventsFollower) failed(cause error) {
	f.failures++
	if f.failures >= f.opt.MaxAttempts {
		f.gaveUp = &EventsGaveUpError{Attempts: f.failures, Err: cause}
		return
	}
	f.cause = cause
	f.wait = f.opt.Jitter(EventsBackoff(f.retry, f.failures, f.opt.MaxBackoff))
}

// ended takes a stream that ended, or went quiet when not alive: one that
// was stable counts the failures from zero again. A stream that gave no
// frame, or went quiet, never was.
func (f *EventsFollower) ended(cause error, alive bool) {
	stable := alive && !f.lastFrameAt.IsZero() &&
		(f.delivered || f.lastFrameAt.Sub(f.connectedAt) >= f.opt.StableAfter)
	f.drop()
	if stable {
		f.failures = 0
	}
	f.failed(cause)
}

// drop closes the stream to reconnect to it.
func (f *EventsFollower) drop() {
	if f.conn == nil {
		return
	}
	close(f.conn.done)
	f.conn.cancel(errStreamDropped)
	f.conn = nil
}

// Close ends the stream; Next connects again.
func (f *EventsFollower) Close() {
	f.drop()
}

// decodePipelineEvent decodes an event of the stream. A severity it does not
// know, of a newer server, reads as the highest known one, error, so that
// the event is neither lost nor hidden below a least severity.
func decodePipelineEvent(data []byte, e *PipelineEvent) error {
	err := json.Unmarshal(data, e)
	if err == nil {
		return nil
	}
	var lenient struct {
		PipelineEvent
		Severity string `json:"severity"`
	}
	if json.Unmarshal(data, &lenient) != nil {
		return fmt.Errorf("decode event: %w", err)
	}
	*e = lenient.PipelineEvent
	e.Severity = SeverityError
	if severity, err := ParseEventSeverity(lenient.Severity); err == nil {
		e.Severity = severity
	}
	return nil
}
