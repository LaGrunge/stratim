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

package watch

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

var update = flag.Bool("update", false, "rewrite the golden files of the watch")

// waitAnswer is an answer of the wait route: a status and a body, or the
// summary of a 204.
type waitAnswer struct {
	status  int
	body    string
	summary string
}

// fakeServer serves recorded event streams, one per connection, and the
// answers of the wait route, one per request; the last of each is repeated.
type fakeServer struct {
	t *testing.T
	// streams are the files of testdata served, or an HTTP status as "503".
	streams []string
	// hold keeps the last stream open until the client goes.
	hold  bool
	waits []waitAnswer

	mu          sync.Mutex
	lastEventID []string
	queries     []string
	waitQueries []string
}

// got are the Last-Event-ID headers and queries of the streams and the
// queries of the waits so far.
func (s *fakeServer) got() (lastEventID, queries, waitQueries []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.lastEventID), slices.Clone(s.queries), slices.Clone(s.waitQueries)
}

func (s *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.URL.Path {
	case "/api/v2/events":
		n := len(s.lastEventID)
		s.lastEventID = append(s.lastEventID, r.Header.Get("Last-Event-ID"))
		s.queries = append(s.queries, r.URL.RawQuery)
		name := s.streams[min(n, len(s.streams)-1)]
		if name == "400evil" {
			http.Error(w, evilBody, http.StatusBadRequest)
			return
		}
		if !strings.HasSuffix(name, ".sse") {
			http.Error(w, "unavailable", map[string]int{"404": http.StatusNotFound, "503": http.StatusServiceUnavailable, "401": http.StatusUnauthorized}[name])
			return
		}
		data, err := os.ReadFile(filepath.Join("testdata", name))
		require.NoError(s.t, err)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(data)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		if (s.hold && n >= len(s.streams)-1) || name == "hold.sse" {
			s.mu.Unlock()
			<-r.Context().Done()
			s.mu.Lock()
		}
	case "/api/v2/pipelines/812/wait":
		n := len(s.waitQueries)
		s.waitQueries = append(s.waitQueries, r.URL.RawQuery)
		answer := waitAnswer{status: http.StatusNoContent}
		if len(s.waits) > 0 {
			answer = s.waits[min(n, len(s.waits)-1)]
		}
		if answer.summary != "" {
			w.Header().Set(woodpecker.PipelineWaitSummaryHeader, answer.summary)
		}
		w.WriteHeader(answer.status)
		_, _ = w.Write([]byte(answer.body))
	default:
		http.NotFound(w, r)
	}
}

// evilBody is an answer of a server that tries to drive the terminal.
const evilBody = "\x1b[2Jboom\u202e\tnow\nsecond line"

// watchResult is how a watch went.
type watchResult struct {
	code     int
	err      error
	out, log string
}

// runWatch watches with the severity notice, unless the options give one
// above.
func runWatch(ctx context.Context, t *testing.T, srv *fakeServer, opt Options) (watchResult, *fakeServer) {
	t.Helper()
	if opt.Severity == woodpecker.SeverityDebug {
		opt.Severity = woodpecker.SeverityNotice
	}
	return runWatchSeverity(ctx, t, srv, opt)
}

func runWatchSeverity(ctx context.Context, t *testing.T, srv *fakeServer, opt Options) (watchResult, *fakeServer) {
	t.Helper()
	srv.t = t
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	var out, log bytes.Buffer
	opt.Out, opt.Err = &out, &log
	opt.Now = func() time.Time { return time.Date(2026, 11, 6, 3, 9, 59, 0, time.UTC) }
	opt.MaxBackoff = 10 * time.Millisecond
	opt.Jitter = func(d time.Duration) time.Duration { return d }
	if opt.SummaryInterval == 0 {
		opt.SummaryInterval = time.Millisecond
	}
	if opt.ExitOn == "" {
		opt.ExitOn = ExitOnFinished
	}
	if opt.Level == "" {
		opt.Level = LevelLine
	}
	client := woodpecker.NewClient(ts.URL, ts.Client())
	code, err := Watch(ctx, client, Target{PipelineID: 812, Name: "owner/name #345", Status: "running"}, opt)
	return watchResult{code: code, err: err, out: out.String(), log: log.String()}, srv
}

// golden compares the lines with testdata/<name>.golden.
func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	if *update {
		require.NoError(t, os.WriteFile(path, []byte(got), 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err, "golden file; run the test with -update to make it")
	assert.Equal(t, string(want), got)
}

const metFinished = `{"v":1,"met":true,"reason":"pipeline_finished","until":"finished","pipeline":{"id":812,"repo_id":7,"repo":"owner/name","number":345,"status":"%s","steps":3,"steps_ok":%d,"steps_failed":0},"summary":"#345 %s · %d/3 ok"}`

func finishedAnswer(status string, ok int) waitAnswer {
	return waitAnswer{status: http.StatusOK, body: fmt.Sprintf(metFinished, status, ok, status, ok)}
}

func TestWatchFinished(t *testing.T) {
	got, srv := runWatch(t.Context(), t, &fakeServer{streams: []string{"success.sse"}, hold: true}, Options{})
	require.NoError(t, got.err)
	assert.Equal(t, ExitSuccess, got.code)
	golden(t, "success", got.out)
	_, queries, _ := srv.got()
	assert.Equal(t, []string{"min_severity=notice&pipeline=812"}, queries)
}

func TestWatchFailed(t *testing.T) {
	got, _ := runWatch(t.Context(), t, &fakeServer{streams: []string{"failed.sse"}, hold: true}, Options{ExitOn: ExitOnFailed, CauseWait: time.Minute})
	require.NoError(t, got.err)
	assert.Equal(t, ExitFailed, got.code)
	golden(t, "failed", got.out)
	assert.NotContains(t, got.out, "Expected equality", "only the first line of the excerpt")
	assert.NotContains(t, got.out, "\x1b", "no escape sequence of the log")
}

func TestWatchFailedWithoutCause(t *testing.T) {
	got, _ := runWatch(t.Context(), t, &fakeServer{streams: []string{"failed.sse"}, hold: true}, Options{ExitOn: ExitOnFailed, Level: LevelSummary})
	require.NoError(t, got.err)
	assert.Equal(t, ExitFailed, got.code)
	golden(t, "failed-summary", got.out)
}

func TestWatchFailedIgnored(t *testing.T) {
	got, _ := runWatch(t.Context(), t, &fakeServer{streams: []string{"ignored.sse"}, hold: true}, Options{ExitOn: ExitOnFailed, CauseWait: time.Minute})
	require.NoError(t, got.err)
	assert.Equal(t, ExitSuccess, got.code, "an ignored failure is no failure")
	golden(t, "ignored", got.out)
}

func TestWatchDebugSeverity(t *testing.T) {
	got, srv := runWatchSeverity(t.Context(), t, &fakeServer{streams: []string{"success.sse"}, hold: true}, Options{Severity: woodpecker.SeverityDebug})
	require.NoError(t, got.err)
	assert.Equal(t, ExitSuccess, got.code)
	_, queries, _ := srv.got()
	assert.Equal(t, []string{"min_severity=debug&pipeline=812"}, queries)

	got, srv = runWatch(t.Context(), t, &fakeServer{streams: []string{"success.sse"}, hold: true}, Options{Severity: woodpecker.SeverityError})
	require.NoError(t, got.err)
	assert.Equal(t, ExitSuccess, got.code, "the end of the pipeline is followed below the severity printed")
	_, queries, _ = srv.got()
	assert.Equal(t, []string{"min_severity=notice&pipeline=812"}, queries)
	golden(t, "severity-error", got.out)
}

func TestWatchReconnect(t *testing.T) {
	got, srv := runWatch(t.Context(), t, &fakeServer{streams: []string{"reconnect-1.sse", "503", "reconnect-2.sse"}, hold: true}, Options{})
	require.NoError(t, got.err)
	assert.Equal(t, ExitFailed, got.code)
	golden(t, "reconnect", got.out)
	lastEventID, _, _ := srv.got()
	assert.Equal(t, []string{"", "1793934000000101", "1793934000000101"}, lastEventID, "resumed from the last event")
	assert.Contains(t, got.log, "reconnecting in 10ms: the stream ended")
	assert.Contains(t, got.log, "reconnecting in 10ms: the server answered 503: unavailable")
}

func TestWatchTooSlow(t *testing.T) {
	got, srv := runWatch(t.Context(), t, &fakeServer{streams: []string{"too-slow.sse", "reconnect-2.sse"}, hold: true}, Options{})
	require.NoError(t, got.err)
	assert.Equal(t, ExitFailed, got.code)
	lastEventID, _, _ := srv.got()
	assert.Equal(t, []string{"", "1793934000000041"}, lastEventID)
	assert.Equal(t, "03:09:59  reconnecting in 0s: the server dropped the stream as too slow\n", got.log, "reconnected at once")
}

func TestWatchReset(t *testing.T) {
	got, srv := runWatch(t.Context(), t, &fakeServer{
		streams: []string{"reset.sse"}, hold: true,
		waits: []waitAnswer{{status: http.StatusNoContent}, finishedAnswer("killed", 1)},
	}, Options{})
	require.NoError(t, got.err)
	assert.Equal(t, ExitStopped, got.code)
	golden(t, "reset", got.out)
	_, _, waitQueries := srv.got()
	assert.Len(t, waitQueries, 2, "the state read again after the reset")
}

func TestWatchFinishedAlready(t *testing.T) {
	got, _ := runWatch(t.Context(), t, &fakeServer{streams: []string{"hold.sse"}, hold: true, waits: []waitAnswer{finishedAnswer("success", 3)}}, Options{})
	require.NoError(t, got.err)
	assert.Equal(t, ExitSuccess, got.code)
	golden(t, "finished-already", got.out)
}

func TestWatchFailedAlready(t *testing.T) {
	body := `{"v":1,"met":true,"reason":"step_failed","until":"failed","pipeline":{"id":812,"repo_id":7,"repo":"owner/name","number":345,"status":"running","steps":3,"steps_ok":1,"steps_failed":1},"summary":"#345 running · 1/3 ok · 1 failed"}`
	got, srv := runWatch(t.Context(), t, &fakeServer{streams: []string{"hold.sse"}, hold: true, waits: []waitAnswer{{status: http.StatusOK, body: body}}},
		Options{ExitOn: ExitOnFailed, CauseWait: time.Minute})
	require.NoError(t, got.err)
	assert.Equal(t, ExitFailed, got.code)
	golden(t, "failed-already", got.out)
	_, _, waitQueries := srv.got()
	assert.Equal(t, "timeout=1&until=failed", waitQueries[0])
}

func TestWatchSummary(t *testing.T) {
	got, srv := runWatch(t.Context(), t, &fakeServer{
		streams: []string{"summary.sse"}, hold: true,
		waits: []waitAnswer{{status: http.StatusNoContent, summary: "#345 running · 1/3 ok · ETA 2m"}, finishedAnswer("success", 3)},
	}, Options{Level: LevelSummary})
	require.NoError(t, got.err)
	assert.Equal(t, ExitSuccess, got.code)
	golden(t, "summary", got.out)
	_, queries, _ := srv.got()
	assert.Equal(t, []string{"min_severity=info&pipeline=812"}, queries)
}

func TestWatchErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		srv     *fakeServer
		message string
	}{
		{"no v2", &fakeServer{streams: []string{"404"}}, "does not serve /api/v2/events (404)"},
		{"login", &fakeServer{streams: []string{"401"}}, "refused the login"},
		{"unauthorized", &fakeServer{streams: []string{"unauthorized.sse"}}, "ended the event stream: unauthorized"},
		{"no wait", &fakeServer{streams: []string{"hold.sse"}, hold: true, waits: []waitAnswer{{status: http.StatusNotFound}}}, "does not serve /api/v2/pipelines/812/wait"},
		{"gone", &fakeServer{streams: []string{"503"}}, "gave up on the event stream after 10 attempts in a row: the server answered 503: unavailable"},
		{"refused", &fakeServer{streams: []string{"400evil"}}, "the server refused the event stream: the server answered 400: ?[2Jboom? now"},
		{
			"no state", &fakeServer{streams: []string{"hold.sse"}, hold: true, waits: []waitAnswer{{status: http.StatusBadGateway, body: evilBody}}},
			"cannot read the state of the pipeline after 10 attempts in a row: the server answered 502: ?[2Jboom? now",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := runWatch(t.Context(), t, tc.srv, Options{})
			assert.Equal(t, ExitError, got.code)
			require.Error(t, got.err)
			assert.Contains(t, got.err.Error(), tc.message)
			var watchErr *Error
			require.ErrorAs(t, got.err, &watchErr)
			assert.Equal(t, ExitError, watchErr.ExitCode())
			assert.NotContains(t, got.err.Error(), "\x1b")
			assert.NotContains(t, got.err.Error(), "\n")
			assert.NotContains(t, got.err.Error(), "second line", "the first line of an answer")
			assert.NotContains(t, got.log, "\x1b")
		})
	}

	srv := &fakeServer{streams: []string{"503"}}
	_, _ = runWatch(t.Context(), t, srv, Options{})
	requests, _, _ := srv.got()
	assert.Len(t, requests, 10, "as many requests as attempts")
}

func TestWatchTimeout(t *testing.T) {
	got, _ := runWatch(t.Context(), t, &fakeServer{streams: []string{"hold.sse"}, hold: true}, Options{Timeout: 100 * time.Millisecond})
	assert.Equal(t, ExitTimeout, got.code)
	require.Error(t, got.err)
	assert.Equal(t, "the watch timed out after 100ms", got.err.Error())
}

func TestWatchInterrupted(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	got, _ := runWatch(ctx, t, &fakeServer{streams: []string{"hold.sse"}, hold: true}, Options{Timeout: time.Minute})
	assert.Equal(t, ExitInterrupted, got.code)
	require.Error(t, got.err)
	assert.Empty(t, got.err.Error(), "nothing to tell")
}

func TestClean(t *testing.T) {
	assert.Equal(t, "plain · text", clean("plain · text"))
	assert.Equal(t, "?[31mred?[0m a b  c", clean("\x1b[31mred\x1b[0m a\tb\r\nc"))
	assert.Equal(t, "?evil", clean("\u202eevil"))
	assert.Equal(t, "abc…", cut("abcdef", 4))
	assert.Equal(t, "abc", cut("abc", 4))
}

func TestWatchCheckRetried(t *testing.T) {
	got, srv := runWatch(t.Context(), t, &fakeServer{
		streams: []string{"hold.sse"}, hold: true,
		waits: []waitAnswer{{status: http.StatusServiceUnavailable}, {status: http.StatusTooManyRequests}, finishedAnswer("success", 3)},
	}, Options{})
	require.NoError(t, got.err)
	assert.Equal(t, ExitSuccess, got.code)
	_, _, waits := srv.got()
	assert.Len(t, waits, 3)
	assert.Contains(t, got.log, "cannot read the state of the pipeline, again in 10ms: the server answered 503")
}

func TestWatchIdleStream(t *testing.T) {
	got, srv := runWatch(t.Context(), t, &fakeServer{streams: []string{"hold.sse", "reconnect-2.sse"}, hold: true}, Options{IdleTimeout: 50 * time.Millisecond})
	require.NoError(t, got.err)
	assert.Equal(t, ExitFailed, got.code)
	lastEventID, _, _ := srv.got()
	assert.Equal(t, []string{"", "1793934000000040"}, lastEventID, "a quiet stream is taken for dead and reconnected to")
	assert.Contains(t, got.log, "reconnecting in 10ms: no frame for 50ms")
}

func TestWatchCauseWaitPasses(t *testing.T) {
	got, _ := runWatch(t.Context(), t, &fakeServer{streams: []string{"failed-no-cause.sse"}, hold: true}, Options{ExitOn: ExitOnFailed, CauseWait: 50 * time.Millisecond})
	require.NoError(t, got.err)
	assert.Equal(t, ExitFailed, got.code)
	golden(t, "failed-no-cause", got.out)
}

func TestWatchSummaryCause(t *testing.T) {
	got, _ := runWatch(t.Context(), t, &fakeServer{streams: []string{"failed.sse"}, hold: true}, Options{ExitOn: ExitOnFailed, Level: LevelSummary, CauseWait: time.Minute})
	require.NoError(t, got.err)
	assert.Equal(t, ExitFailed, got.code)
	golden(t, "failed-summary-cause", got.out)
}

func TestWatchWorkflowFailed(t *testing.T) {
	got, _ := runWatch(t.Context(), t, &fakeServer{streams: []string{"workflow-failed.sse"}, hold: true}, Options{ExitOn: ExitOnFailed, CauseWait: time.Minute})
	require.NoError(t, got.err)
	assert.Equal(t, ExitFailed, got.code)
	golden(t, "workflow-failed", got.out)
}

func TestWatchFailedAlreadyStatuses(t *testing.T) {
	for _, tc := range []struct {
		reason, status string
		met            bool
		code           int
	}{
		{woodpecker.PipelineWaitReasonPipelineFailed, woodpecker.StatusTimeout, true, ExitFailed},
		{woodpecker.PipelineWaitReasonPipelineFailed, woodpecker.StatusError, true, ExitStopped},
		{woodpecker.PipelineWaitReasonPipelineFinished, woodpecker.StatusKilled, false, ExitStopped},
		{woodpecker.PipelineWaitReasonWorkflowFailed, woodpecker.StatusRunning, true, ExitFailed},
		{woodpecker.PipelineWaitReasonStepFailed, woodpecker.StatusError, true, ExitFailed},
	} {
		t.Run(tc.reason+" "+tc.status, func(t *testing.T) {
			body := fmt.Sprintf(`{"v":1,"met":%t,"reason":%q,"until":"failed","pipeline":{"id":812,"repo_id":7,"repo":"owner/name","number":345,"status":%q,"steps":3,"steps_ok":1,"steps_failed":1},"summary":"#345 %s · 1/3 ok · 1 failed"}`,
				tc.met, tc.reason, tc.status, tc.status)
			got, _ := runWatch(t.Context(), t, &fakeServer{streams: []string{"hold.sse"}, hold: true, waits: []waitAnswer{{status: http.StatusOK, body: body}}},
				Options{ExitOn: ExitOnFailed, CauseWait: time.Minute})
			require.NoError(t, got.err)
			assert.Equal(t, tc.code, got.code)
			assert.Contains(t, got.out, fmt.Sprintf("  done               -  %s  exit %d: #345 %s · 1/3 ok · 1 failed\n", tc.status, tc.code, tc.status))
		})
	}
}

func TestWatchPipelineEndCodes(t *testing.T) {
	// the end of the pipeline gives the code of its status, also when
	// watched for a failure
	for status, code := range map[string]int{"error": ExitStopped, "timeout": ExitFailed, "killed": ExitStopped} {
		for _, exitOn := range []string{ExitOnFinished, ExitOnFailed} {
			t.Run(status+" "+exitOn, func(t *testing.T) {
				got, _ := runWatch(t.Context(), t, &fakeServer{streams: []string{"finished-" + status + ".sse"}, hold: true}, Options{ExitOn: exitOn})
				require.NoError(t, got.err)
				assert.Equal(t, code, got.code)
				assert.Contains(t, got.out, fmt.Sprintf("  done               -  %s  exit %d: #345 %s after 122s\n", status, code, status))
			})
		}
	}
}

func TestWatchFailureSeenThenStopped(t *testing.T) {
	// a failure seen ends the watch as one while its cause is waited for
	got, _ := runWatch(t.Context(), t, &fakeServer{streams: []string{"failed-then-unauthorized.sse"}}, Options{ExitOn: ExitOnFailed, CauseWait: time.Minute})
	require.NoError(t, got.err)
	assert.Equal(t, ExitFailed, got.code, "the stream ended for good")
	assert.Contains(t, got.out, "  done               -  failure  exit 1: #345 build/test failure after 31s, exit 1\n")
	assert.Contains(t, got.log, "no cause of the failure: the server ended the event stream: unauthorized")

	got, _ = runWatch(t.Context(), t, &fakeServer{streams: []string{"failed-no-cause.sse"}, hold: true},
		Options{ExitOn: ExitOnFailed, CauseWait: time.Minute, Timeout: 200 * time.Millisecond})
	require.NoError(t, got.err)
	assert.Equal(t, ExitFailed, got.code, "the timeout passed")
	assert.Contains(t, got.out, "  done               -  timeout  exit 1: #345 build/test timeout after 60s\n")

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	got, _ = runWatch(ctx, t, &fakeServer{streams: []string{"failed-no-cause.sse"}, hold: true}, Options{ExitOn: ExitOnFailed, CauseWait: time.Minute})
	assert.Equal(t, ExitInterrupted, got.code, "Ctrl-C")
}

func TestWatchEvilType(t *testing.T) {
	got, _ := runWatch(t.Context(), t, &fakeServer{streams: []string{"evil-type.sse"}, hold: true}, Options{})
	require.NoError(t, got.err)
	assert.Equal(t, ExitSuccess, got.code)
	assert.NotContains(t, got.out, "\x1b")
	assert.NotContains(t, got.out, "\u202e")
	assert.Contains(t, got.out, "03:11:00  step.?[2Jodd?      -  success  #345 odd\n")
}

func TestWatchOddFrames(t *testing.T) {
	// a reset without a reason, an event of another pipeline, an end that
	// does not decode: the state read again ends the watch
	got, srv := runWatch(t.Context(), t, &fakeServer{
		streams: []string{"odd.sse"}, hold: true,
		waits: []waitAnswer{{status: http.StatusNoContent}, finishedAnswer("success", 3)},
	}, Options{})
	require.NoError(t, got.err)
	assert.Equal(t, ExitSuccess, got.code)
	golden(t, "odd", got.out)
	assert.Contains(t, got.log, "cannot read a pipeline.finished event, reading the state of the pipeline")
	_, _, waits := srv.got()
	assert.GreaterOrEqual(t, len(waits), 2)
}

func TestWatchSummaryThrottled(t *testing.T) {
	got, srv := runWatch(t.Context(), t, &fakeServer{
		streams: []string{"many.sse"}, hold: true,
		waits: []waitAnswer{
			{status: http.StatusNoContent, summary: "#345 running · 0/30 ok"},
			{status: http.StatusNoContent, summary: "#345 running · 30/30 ok"},
			finishedAnswer("success", 3),
		},
	}, Options{Level: LevelSummary, SummaryInterval: 300 * time.Millisecond, Timeout: time.Second})
	assert.Equal(t, ExitTimeout, got.code)
	_, _, waits := srv.got()
	assert.Len(t, waits, 2, "30 events: the check at the start and one put off after it")
	assert.Contains(t, got.out, "#345 running · 30/30 ok")
}
