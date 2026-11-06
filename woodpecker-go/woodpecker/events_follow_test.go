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
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedStream is what a connection to the stream gets: a status other
// than 200, or the body, kept open after it when hold.
type scriptedStream struct {
	status int
	body   string
	hold   bool
}

// streamServer serves the scripted streams one per connection, the last one
// again and again, and records the Last-Event-ID of each.
type streamServer struct {
	mu          sync.Mutex
	script      []scriptedStream
	lastEventID []string
}

func (s *streamServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	n := len(s.lastEventID)
	s.lastEventID = append(s.lastEventID, r.Header.Get("Last-Event-ID"))
	step := s.script[min(n, len(s.script)-1)]
	s.mu.Unlock()
	if step.status != 0 {
		http.Error(w, "scripted", step.status)
		return
	}
	_, _ = w.Write([]byte(step.body))
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	if step.hold {
		<-r.Context().Done()
	}
}

func (s *streamServer) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.lastEventID)
}

const (
	testHello = "retry: 10\n\nid: 40\nevent: hello\ndata: {\"v\":1,\"server\":\"s\"}\n\n"
	testEvent = "id: 41\nevent: step.failed\ndata: {\"v\":1,\"id\":41,\"type\":\"step.failed\",\"severity\":\"error\",\"pipeline_id\":812,\"status\":\"failure\"}\n\n"
)

// follow follows the scripted server, recording the delays to reconnect
// after.
func follow(t *testing.T, srv *streamServer, opt EventsFollowOptions) (*EventsFollower, *[]time.Duration) {
	t.Helper()
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	var mu sync.Mutex
	delays := new([]time.Duration)
	opt.Jitter = func(d time.Duration) time.Duration { return d }
	if opt.MaxBackoff == 0 {
		opt.MaxBackoff = 40 * time.Millisecond
	}
	opt.OnReconnect = func(d time.Duration, _ error) {
		mu.Lock()
		defer mu.Unlock()
		*delays = append(*delays, d)
	}
	f := NewEventsFollower(NewClient(ts.URL, ts.Client()), opt)
	t.Cleanup(f.Close)
	return f, delays
}

func nextKind(t *testing.T, f *EventsFollower) *FollowedFrame {
	t.Helper()
	frame, err := f.Next(t.Context())
	require.NoError(t, err)
	return frame
}

func TestEventsFollowerReconnects(t *testing.T) {
	srv := &streamServer{script: []scriptedStream{
		{body: testHello + testEvent},   // ends after an event: stable
		{status: http.StatusBadGateway}, // a deploy
		{status: http.StatusBadGateway}, //
		{body: testHello},               // ends at once: not stable
		{body: testHello + testEvent, hold: true},
	}}
	f, delays := follow(t, srv, EventsFollowOptions{PipelineID: 812, LastEventID: "39"})

	assert.Equal(t, FollowedHello, nextKind(t, f).Kind)
	frame := nextKind(t, f)
	assert.Equal(t, FollowedEvent, frame.Kind)
	assert.Equal(t, StepFailed, frame.Event.Type)
	assert.Equal(t, "41", f.Cursor())
	assert.Equal(t, FollowedHello, nextKind(t, f).Kind, "reconnected")
	assert.Equal(t, FollowedHello, nextKind(t, f).Kind, "reconnected again")
	assert.Equal(t, FollowedEvent, nextKind(t, f).Kind)

	assert.Equal(t, []string{"39", "41", "41", "41", "40"}, srv.requests(), "goes on from the cursor")
	// the server's 10ms, doubled per failure in a row; the stream that ended
	// at once counted on
	assert.Equal(t, []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond}, *delays)
}

func TestEventsFollowerGivesUp(t *testing.T) {
	srv := &streamServer{script: []scriptedStream{{status: http.StatusServiceUnavailable}}}
	f, delays := follow(t, srv, EventsFollowOptions{MaxAttempts: 4})
	_, err := f.Next(t.Context())
	gaveUp, ok := errors.AsType[*EventsGaveUpError](err)
	require.True(t, ok, "%v", err)
	assert.Equal(t, 4, gaveUp.Attempts)
	assert.Len(t, srv.requests(), 4, "as many requests as attempts")
	assert.Len(t, *delays, 3)
	assert.Contains(t, err.Error(), "after 4 attempts in a row: client error 503")

	_, err = f.Next(t.Context())
	assert.ErrorAs(t, err, &gaveUp, "for good")
	assert.Len(t, srv.requests(), 4)
}

func TestEventsFollowerFatal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		script scriptedStream
		check  func(t *testing.T, err error)
	}{
		{"not found", scriptedStream{status: http.StatusNotFound}, func(t *testing.T, err error) {
			clientErr, ok := errors.AsType[*ClientError](err)
			require.True(t, ok)
			assert.Equal(t, http.StatusNotFound, clientErr.StatusCode)
		}},
		{"unauthorized", scriptedStream{body: testHello + "event: error\ndata: {\"v\":1,\"reason\":\"unauthorized\"}\n\n"}, func(t *testing.T, err error) {
			assert.Equal(t, &EventsEndedError{Reason: EventsErrorUnauthorized}, err)
		}},
		{"malformed error", scriptedStream{body: testHello + "event: error\ndata: {\n\n"}, func(t *testing.T, err error) {
			assert.Equal(t, &EventsEndedError{Reason: "unknown"}, err)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &streamServer{script: []scriptedStream{tc.script}}
			f, _ := follow(t, srv, EventsFollowOptions{})
			var err error
			for err == nil {
				_, err = f.Next(t.Context())
			}
			tc.check(t, err)
			assert.False(t, IsRetryableEventsError(err))
			assert.Len(t, srv.requests(), 1)
		})
	}
}

func TestEventsFollowerIdle(t *testing.T) {
	srv := &streamServer{script: []scriptedStream{{body: testHello, hold: true}}}
	f, delays := follow(t, srv, EventsFollowOptions{IdleTimeout: 50 * time.Millisecond})
	assert.Equal(t, FollowedHello, nextKind(t, f).Kind)
	assert.Equal(t, FollowedHello, nextKind(t, f).Kind, "a quiet stream is taken for dead and reconnected to")
	assert.Equal(t, []string{"", "40"}, srv.requests())
	assert.Len(t, *delays, 1)
}

func TestEventsFollowerGivesUpOnSilence(t *testing.T) {
	// a stream that never gives a frame, e.g. behind a proxy that holds it
	srv := &streamServer{script: []scriptedStream{{hold: true}}}
	f, delays := follow(t, srv, EventsFollowOptions{MaxAttempts: 3, IdleTimeout: 20 * time.Millisecond, StableAfter: time.Millisecond})
	_, err := f.Next(t.Context())
	gaveUp, ok := errors.AsType[*EventsGaveUpError](err)
	require.True(t, ok, "%v", err)
	assert.Equal(t, 3, gaveUp.Attempts)
	assert.Contains(t, err.Error(), "no frame for 20ms")
	assert.Len(t, srv.requests(), 3)
	assert.Len(t, *delays, 2)
}

func TestEventsFollowerQuietAfterHello(t *testing.T) {
	// a stream that says hello and then nothing is taken for dead each time
	srv := &streamServer{script: []scriptedStream{{body: testHello, hold: true}}}
	f, _ := follow(t, srv, EventsFollowOptions{MaxAttempts: 3, IdleTimeout: 20 * time.Millisecond, StableAfter: time.Millisecond})
	var err error
	for err == nil {
		_, err = f.Next(t.Context())
	}
	assert.ErrorAs(t, err, new(*EventsGaveUpError))
	assert.Len(t, srv.requests(), 3)
}

func TestEventsFollowerTooSlow(t *testing.T) {
	tooSlow := testHello + testEvent + "event: error\ndata: {\"v\":1,\"reason\":\"too_slow\"}\n\n"
	srv := &streamServer{script: []scriptedStream{{body: tooSlow}, {body: tooSlow}, {body: tooSlow}, {body: testHello, hold: true}}}
	f, delays := follow(t, srv, EventsFollowOptions{})
	for range 3 {
		assert.Equal(t, FollowedHello, nextKind(t, f).Kind)
		assert.Equal(t, FollowedEvent, nextKind(t, f).Kind)
	}
	assert.Equal(t, FollowedHello, nextKind(t, f).Kind)
	assert.Equal(t, []time.Duration{0, 10 * time.Millisecond, 20 * time.Millisecond}, *delays, "at once, then backed off")
}

func TestEventsFollowerFrames(t *testing.T) {
	srv := &streamServer{script: []scriptedStream{{body: testHello +
		"event: reset\ndata: {\"v\":1}\n\n" +
		"id: 42\nevent: step.failed\ndata: {\"v\":1,\"id\":\"not a number\"}\n\n" +
		"id: 43\nevent: pipeline.finished\ndata: {\"v\":1,\"id\":43,\"type\":\"pipeline.finished\",\"severity\":\"critical\",\"pipeline_id\":812,\"status\":\"success\"}\n\n" +
		"id: 44\n: ping\n\n" +
		"event: step.started\n\n", hold: true}}}
	f, _ := follow(t, srv, EventsFollowOptions{IdleTimeout: time.Hour})
	assert.Equal(t, "s", nextKind(t, f).Hello.Server)
	assert.Equal(t, EventsResetReason("unknown"), nextKind(t, f).Reset.Reason, "a reset without a reason")
	frame := nextKind(t, f)
	assert.Equal(t, FollowedUndecodable, frame.Kind)
	assert.Equal(t, "step.failed", frame.Name)
	require.Error(t, frame.Err)
	frame = nextKind(t, f)
	assert.Equal(t, FollowedEvent, frame.Kind)
	assert.Equal(t, SeverityError, frame.Event.Severity, "a severity of a newer server reads as the highest")
	assert.Equal(t, StatusSuccess, frame.Event.Status)
	assert.Equal(t, PipelineFinished, frame.Event.Type)
	assert.Equal(t, "43", f.Cursor())
}

func TestEventsBackoff(t *testing.T) {
	for _, tc := range []struct {
		retry    time.Duration
		failures int
		want     time.Duration
	}{
		{0, 1, 3 * time.Second},
		{0, 2, 6 * time.Second},
		{0, 5, 30 * time.Second},
		{3 * time.Second, 1, 3 * time.Second},
		{3 * time.Second, 4, 24 * time.Second},
		{3 * time.Second, 5, 30 * time.Second},
		{3 * time.Second, 50, 30 * time.Second},
		{time.Second, 3, 4 * time.Second},
	} {
		assert.Equal(t, tc.want, EventsBackoff(tc.retry, tc.failures, 30*time.Second), "retry %s, failures %d", tc.retry, tc.failures)
	}
	for range 100 {
		d := FullJitter(3 * time.Second)
		assert.GreaterOrEqual(t, d, time.Duration(0))
		assert.LessOrEqual(t, d, 3*time.Second)
	}
	assert.Zero(t, FullJitter(0))
}

func TestIsRetryableEventsError(t *testing.T) {
	for status, want := range map[int]bool{400: false, 401: false, 403: false, 404: false, 408: true, 429: true, 500: true, 502: true, 503: true} {
		assert.Equal(t, want, IsRetryableEventsError(&ClientError{StatusCode: status}), strconv.Itoa(status))
	}
	assert.True(t, IsRetryableEventsError(errors.New("connection refused")))
	assert.False(t, IsRetryableEventsError(ErrEventsFrameTooLarge))
	assert.False(t, IsRetryableEventsError(nil))
}
