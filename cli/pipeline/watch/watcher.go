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

// Package watch follows a pipeline over /api/v2: one line per event of it,
// until a condition is met, and an exit code that tells how it went.
package watch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// The exit codes of a watch.
const (
	// ExitSuccess is a pipeline that succeeded, or that ended without
	// failing when watched for a failure.
	ExitSuccess = 0
	// ExitFailed is a pipeline that failed or timed out, or a step or
	// workflow that failed when watched for a failure.
	ExitFailed = 1
	// ExitStopped is a pipeline that ended killed, canceled, declined or in
	// an error.
	ExitStopped = 2
	// ExitError is a wrong usage, a pipeline not found, a server that cannot
	// be reached or does not serve /api/v2, or a login it refuses.
	ExitError = 3
	// ExitTimeout is a watch whose --timeout passed first.
	ExitTimeout = 4
	// ExitInterrupted is a watch stopped by Ctrl-C or a termination signal.
	ExitInterrupted = 130
)

// The conditions a watch ends on.
const (
	// ExitOnFinished ends the watch when the pipeline ended.
	ExitOnFinished = woodpecker.PipelineWaitFinished
	// ExitOnFailed ends the watch on the first failure that is not ignored,
	// or when the pipeline ended.
	ExitOnFailed = woodpecker.PipelineWaitFailed
)

// The levels of detail.
const (
	// LevelLine prints a line per event.
	LevelLine = "line"
	// LevelSummary prints the summary of the pipeline when it changed.
	LevelSummary = "summary"
)

// checkTimeoutS is how long a check of the state of the pipeline waits for
// the condition, in seconds.
const checkTimeoutS = 1

// checkSlack is how much longer than that a check may take before it is
// taken for failed.
const checkSlack = 30 * time.Second

// defaultSummaryInterval is the least time between two checks for the
// summary.
const defaultSummaryInterval = 5 * time.Second

// Options tell how to watch.
type Options struct {
	// ExitOn is ExitOnFinished or ExitOnFailed.
	ExitOn string
	// Level is LevelLine or LevelSummary.
	Level string
	// Severity is the least severity of the events printed at LevelLine.
	Severity woodpecker.EventSeverity
	// Timeout bounds the whole watch; 0 is none.
	Timeout time.Duration
	// CauseWait is how long to wait for the cause of the failure that ends
	// a watch for a failure; 0 does not wait.
	CauseWait time.Duration

	// Out gets the lines, Err the notes about the connection.
	Out, Err io.Writer
	// Location is the time zone of the times printed; nil is UTC.
	Location *time.Location
	// Now is the clock of the lines that are not events; nil is time.Now.
	Now func() time.Time

	// SummaryInterval is the least time between two checks of the state
	// for the summary; 0 is 5 seconds.
	SummaryInterval time.Duration
	// MaxAttempts, MaxBackoff, IdleTimeout and Jitter tell how to
	// reconnect, as in woodpecker.EventsFollowOptions; MaxAttempts and the
	// backoff bound the retries of a check of the state too.
	MaxAttempts int
	MaxBackoff  time.Duration
	IdleTimeout time.Duration
	Jitter      func(time.Duration) time.Duration
}

// Target is the pipeline to watch.
type Target struct {
	PipelineID int64
	// Name tells which pipeline it is in the header, e.g. "owner/name #3".
	Name string
	// Status is the status of the pipeline when it was looked up.
	Status string
}

// Error is a watch that could not run to its condition.
type Error struct {
	Code int
	Err  error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

// ExitCode is the exit code of the watch.
func (e *Error) ExitCode() int { return e.Code }

var errTimeout = errors.New("the watch timed out")

// withTimeout bounds the context by the timeout, if any, with errTimeout as
// its cause.
func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		ctx, cancel := context.WithCancelCause(ctx)
		return ctx, func() { cancel(nil) }
	}
	return context.WithTimeoutCause(ctx, timeout, errTimeout)
}

// exitFor is the exit code and error of a watch that could not go on: of
// the context that ended, else of the error.
func exitFor(ctx context.Context, timeout time.Duration, err error) (int, error) {
	if ctx.Err() != nil {
		if errors.Is(context.Cause(ctx), errTimeout) {
			return ExitTimeout, &Error{Code: ExitTimeout, Err: fmt.Errorf("%w after %s", errTimeout, timeout)}
		}
		return ExitInterrupted, &Error{Code: ExitInterrupted}
	}
	return ExitError, &Error{Code: ExitError, Err: errors.New(describe(err))}
}

// Watch follows the pipeline until the condition of the options is met and
// returns the exit code. An error tells why a watch could not run to its
// condition, with its code.
func Watch(ctx context.Context, client woodpecker.Client, target Target, opt Options) (int, error) {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	if opt.Location == nil {
		opt.Location = time.UTC
	}
	if opt.MaxAttempts <= 0 {
		opt.MaxAttempts = woodpecker.DefaultEventsMaxAttempts
	}
	if opt.MaxBackoff <= 0 {
		opt.MaxBackoff = woodpecker.DefaultEventsMaxBackoff
	}
	if opt.Jitter == nil {
		opt.Jitter = woodpecker.FullJitter
	}
	if opt.SummaryInterval <= 0 {
		opt.SummaryInterval = defaultSummaryInterval
	}
	if opt.Out == nil {
		opt.Out = io.Discard
	}
	if opt.Err == nil {
		opt.Err = io.Discard
	}
	ctx, cancel := withTimeout(ctx, opt.Timeout)
	defer cancel()

	w := &watcher{client: client, target: target, opt: opt, checks: make(chan checkResult, 1)}
	w.header()
	code, err := w.run(ctx)
	if err != nil || code < 0 {
		return exitFor(ctx, opt.Timeout, err)
	}
	return code, nil
}

type watcher struct {
	client woodpecker.Client
	target Target
	opt    Options

	// errMu guards Err, written to by the follower too.
	errMu sync.Mutex

	// summary is the summary printed last.
	summary string

	// checks gets the result of the check in flight.
	checks   chan checkResult
	checking bool
	// lastCheck is when the last check started.
	lastCheck time.Time
	// recheck asks for another check once the one in flight is done; at
	// once when recheckNow, else no sooner than SummaryInterval.
	recheck, recheckNow bool
	// throttled fires for a check put off for SummaryInterval.
	throttled <-chan time.Time
	// retry fires for a check to try again; checkFailures are the ones
	// that failed in a row.
	retry         <-chan time.Time
	checkFailures int

	// failed is the failure that ends the watch once its cause came or
	// the time to wait for it passed.
	failed                     *woodpecker.PipelineEvent
	failedStatus, failedReason string
	causeTimer                 <-chan time.Time
}

type checkResult struct {
	result *woodpecker.PipelineWaitResult
	err    error
}

type followed struct {
	frame *woodpecker.FollowedFrame
	err   error
}

// run follows the stream until the condition is met. A negative code is a
// context that ended.
func (w *watcher) run(ctx context.Context) (int, error) {
	runCtx, stop := context.WithCancelCause(ctx)
	minSeverity := w.streamSeverity()
	follower := woodpecker.NewEventsFollower(w.client, woodpecker.EventsFollowOptions{
		PipelineID:  w.target.PipelineID,
		MinSeverity: &minSeverity,
		MaxAttempts: w.opt.MaxAttempts,
		MaxBackoff:  w.opt.MaxBackoff,
		IdleTimeout: w.opt.IdleTimeout,
		Jitter:      w.opt.Jitter,
		OnReconnect: func(delay time.Duration, cause error) {
			w.logf("reconnecting in %s: %s", delay.Round(time.Millisecond), describe(cause))
		},
	})
	frames := make(chan followed)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer follower.Close()
		for {
			frame, err := follower.Next(runCtx)
			select {
			case frames <- followed{frame, err}:
			case <-runCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		stop(nil)
		<-readerDone
	}()

	for {
		select {
		case <-ctx.Done():
			return w.stopped(ctx, nil)
		case <-w.causeTimer:
			return w.finishFailed(nil), nil
		case <-w.throttled:
			w.throttled = nil
			w.wantCheck(runCtx, true)
		case <-w.retry:
			w.retry = nil
			w.startCheck(runCtx)
		case res := <-w.checks:
			w.checking = false
			if code, end, err := w.checked(runCtx, res); end {
				return code, nil
			} else if err != nil {
				return w.stopped(ctx, err)
			}
		case got := <-frames:
			if got.err != nil {
				return w.stopped(ctx, streamError(got.err))
			}
			if code, end := w.frame(runCtx, got.frame); end {
				return code, nil
			}
		}
	}
}

// frame handles a frame of the stream, telling whether it ends the watch.
func (w *watcher) frame(ctx context.Context, f *woodpecker.FollowedFrame) (int, bool) {
	switch f.Kind {
	case woodpecker.FollowedHello:
		// the state when the stream started: met already, or the summary
		// to start from
		w.wantCheck(ctx, false)
	case woodpecker.FollowedReset:
		w.line(w.now(), "reset", "", "", "events missed ("+string(f.Reset.Reason)+"), reading the state of the pipeline")
		w.wantCheck(ctx, false)
	case woodpecker.FollowedUndecodable:
		// it may have been the end of the pipeline
		w.logf("cannot read a %s event, reading the state of the pipeline: %s", f.Name, describe(f.Err))
		w.wantCheck(ctx, false)
	case woodpecker.FollowedEvent:
		if f.Event.PipelineID == w.target.PipelineID {
			return w.event(ctx, f.Event)
		}
	}
	return 0, false
}

// event handles a pipeline event, telling whether it ends the watch.
func (w *watcher) event(ctx context.Context, e *woodpecker.PipelineEvent) (int, bool) {
	if w.opt.Level == LevelLine && e.Severity >= w.opt.Severity {
		w.printEvent(e)
	}

	if w.failed != nil {
		// a failure ended the watch: only its cause is waited for
		if e.Type == woodpecker.StepExplained && (w.failed.StepID == 0 || e.StepID == w.failed.StepID) {
			if w.opt.Level == LevelLine && e.Severity < w.opt.Severity {
				w.printEvent(e)
			}
			return w.finishFailed(e), true
		}
		return 0, false
	}

	switch {
	case e.Type == woodpecker.PipelineFinished:
		return w.finish(statusCode(e.Status), e.Status, e.Summary), true
	case w.opt.ExitOn == ExitOnFailed && e.Type == woodpecker.StepFailed && !e.Ignored:
		return w.startFailed(e, e.Status, e.Summary)
	case w.opt.ExitOn == ExitOnFailed && e.Type == woodpecker.WorkflowFinished && woodpecker.IsFailureStatus(e.Status):
		return w.finish(ExitFailed, e.Status, e.Summary), true
	}
	if w.opt.Level == LevelSummary {
		w.wantCheck(ctx, true)
	}
	return 0, false
}

// startFailed takes a failure: it ends the watch at once or once its cause
// came.
func (w *watcher) startFailed(e *woodpecker.PipelineEvent, status, reason string) (int, bool) {
	w.failed, w.failedStatus, w.failedReason = e, status, reason
	if w.opt.CauseWait <= 0 || e.StepID == 0 {
		return w.finishFailed(nil), true
	}
	w.causeTimer = time.After(w.opt.CauseWait)
	return 0, false
}

// finishFailed ends the watch for a failure, with its cause if it came.
func (w *watcher) finishFailed(explained *woodpecker.PipelineEvent) int {
	if explained != nil && explained.Cause != nil && w.opt.Level == LevelSummary {
		w.printCause(explained)
	}
	return w.finish(ExitFailed, w.failedStatus, w.failedReason)
}

// wantCheck asks for a check of the state of the pipeline: whether the
// condition is met already, and its summary. A throttled one comes no sooner
// than SummaryInterval after the last.
func (w *watcher) wantCheck(ctx context.Context, throttled bool) {
	if w.checking {
		w.recheck = true
		w.recheckNow = w.recheckNow || !throttled
		return
	}
	if w.retry != nil {
		return // the retry checks
	}
	if throttled {
		if wait := time.Until(w.lastCheck.Add(w.opt.SummaryInterval)); wait > 0 {
			if w.throttled == nil {
				w.throttled = time.After(wait)
			}
			return
		}
	}
	w.startCheck(ctx)
}

// startCheck reads the state of the pipeline in the background.
func (w *watcher) startCheck(ctx context.Context) {
	w.checking, w.recheck, w.recheckNow = true, false, false
	w.throttled = nil
	w.lastCheck = time.Now()
	until := w.opt.ExitOn
	go func() {
		checkCtx, cancel := context.WithTimeout(ctx, checkTimeoutS*time.Second+checkSlack)
		defer cancel()
		res, err := w.client.PipelineWaitContext(checkCtx, w.target.PipelineID, woodpecker.PipelineWaitOptions{Until: until, TimeoutS: checkTimeoutS})
		select {
		case w.checks <- checkResult{res, err}:
		case <-ctx.Done():
		}
	}()
}

// checked handles the result of a check.
func (w *watcher) checked(ctx context.Context, res checkResult) (int, bool, error) {
	if res.err != nil {
		if ctx.Err() != nil || w.failed != nil {
			return 0, false, nil // the cause is waited for
		}
		return 0, false, w.checkFailed(res.err)
	}
	w.checkFailures = 0
	r := res.result
	if r.TimedOut {
		w.printSummary("", r.Summary)
		if w.recheck {
			w.wantCheck(ctx, !w.recheckNow)
		}
		return 0, false, nil
	}
	if w.failed != nil {
		return 0, false, nil // the cause is waited for
	}
	status := r.Pipeline.Status
	// the failure of a step or workflow before the end ends a watch for a
	// failure; the end of the pipeline gives the code of its status
	if r.Met && w.opt.ExitOn == ExitOnFailed && r.Reason != woodpecker.PipelineWaitReasonPipelineFailed {
		if r.Reason == woodpecker.PipelineWaitReasonStepFailed && r.Event != nil {
			code, end := w.startFailed(r.Event, r.Event.Status, r.Summary)
			return code, end, nil
		}
		w.failed = &woodpecker.PipelineEvent{}
		w.failedStatus, w.failedReason = status, r.Summary
		return w.finishFailed(nil), true, nil
	}
	return w.finish(statusCode(status), status, r.Summary), true, nil
}

// stopped ends a watch whose context ended, or whose stream or check
// failed for good. A failure seen already ends it as one, also when the
// timeout passed while its cause was waited for, but not on Ctrl-C. A
// negative code is a context that ended.
func (w *watcher) stopped(ctx context.Context, err error) (int, error) {
	if w.failed != nil && (ctx.Err() == nil || errors.Is(context.Cause(ctx), errTimeout)) {
		if err != nil {
			w.logf("no cause of the failure: %s", describe(err))
		}
		return w.finishFailed(nil), nil
	}
	if ctx.Err() != nil {
		return -1, nil
	}
	return 0, err
}

// checkFailed takes a check that failed: one that may go away is tried
// again after a backoff, up to MaxAttempts in a row.
func (w *watcher) checkFailed(err error) error {
	if clientErr, ok := errors.AsType[*woodpecker.ClientError](err); ok && clientErr.StatusCode == http.StatusNotFound {
		return fmt.Errorf("the server does not serve /api/v2/pipelines/%d/wait (404): watch needs a server with the pipeline events API", w.target.PipelineID)
	}
	if !woodpecker.IsRetryableEventsError(err) {
		return fmt.Errorf("cannot read the state of the pipeline: %w", err)
	}
	w.checkFailures++
	if w.checkFailures >= w.opt.MaxAttempts {
		return fmt.Errorf("cannot read the state of the pipeline after %d attempts in a row: %w", w.checkFailures, err)
	}
	delay := w.opt.Jitter(woodpecker.EventsBackoff(0, w.checkFailures, w.opt.MaxBackoff))
	w.logf("cannot read the state of the pipeline, again in %s: %s", delay.Round(time.Millisecond), describe(err))
	w.retry = time.After(delay)
	return nil
}

// finish prints the last line and returns the exit code.
func (w *watcher) finish(code int, status, summary string) int {
	w.line(w.now(), "done", "", status, fmt.Sprintf("exit %d: %s", code, summary))
	return code
}

// streamSeverity is the least severity to ask the stream for: the events
// printed, and the ones the condition is decided on, which are notice or
// more; info of a summary, to follow the steps that succeed.
func (w *watcher) streamSeverity() woodpecker.EventSeverity {
	if w.opt.Level == LevelSummary {
		return woodpecker.SeverityInfo
	}
	return min(w.opt.Severity, woodpecker.SeverityNotice)
}

// note prints a note about the connection.
func (w *watcher) logf(format string, args ...any) {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	fmt.Fprintf(w.opt.Err, "%s  %s\n", w.clock(w.now()), clean(fmt.Sprintf(format, args...)))
}

// streamError words an error that ended the stream for good.
func streamError(err error) error {
	if clientErr, ok := errors.AsType[*woodpecker.ClientError](err); ok {
		switch clientErr.StatusCode {
		case http.StatusNotFound:
			return errors.New("the server does not serve /api/v2/events (404): watch needs a server with the pipeline events API")
		case http.StatusUnauthorized, http.StatusForbidden:
			return fmt.Errorf("the server refused the login to the event stream (%d)", clientErr.StatusCode)
		}
		return fmt.Errorf("the server refused the event stream: %w", err)
	}
	return err
}

// statusCode is the exit code of a pipeline that ended with the status.
func statusCode(status string) int {
	switch status {
	case woodpecker.StatusSuccess:
		return ExitSuccess
	case woodpecker.StatusFailure, woodpecker.StatusTimeout:
		return ExitFailed
	}
	return ExitStopped
}

func (w *watcher) now() time.Time { return w.opt.Now() }
