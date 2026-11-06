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

//go:build test

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/summary"
)

// running stores a pipeline of the repo that runs the step "test" of the
// workflow "build", and gives its snapshots to the hub.
func (env *eventsV2Env) running(t *testing.T, repo *model.Repo, number int64) *model.Pipeline {
	t.Helper()
	p := env.pipeline(t, repo, number)
	p.Status, p.Started = model.StatusRunning, time.Now().Add(-10*time.Minute).Unix()
	require.NoError(t, env.store.UpdatePipeline(p))
	require.NoError(t, env.store.WorkflowsCreate([]*model.Workflow{{
		PipelineID: p.ID, PID: 1, Name: "build", State: model.StatusRunning, Started: p.Started,
		Children: []*model.Step{{
			UUID: fmt.Sprintf("%d-1", p.ID), PipelineID: p.ID, PID: 2, PPID: 1, Name: "test",
			State: model.StatusRunning, Started: p.Started, Failure: model.FailureFail,
		}},
	}}))
	env.observe(t, repo, p.ID)
	return p
}

// observe gives the stored pipeline to the hub.
func (env *eventsV2Env) observe(t *testing.T, repo *model.Repo, id int64) {
	t.Helper()
	p, err := env.store.GetPipeline(id)
	require.NoError(t, err)
	p.Workflows, err = env.store.WorkflowGetTree(p)
	require.NoError(t, err)
	env.hub.Observe(repo, p)
}

// failStep stores the end of the step "test" of the pipeline as a failure,
// the pipeline still running.
func (env *eventsV2Env) failStep(t *testing.T, p *model.Pipeline) {
	t.Helper()
	env.endStep(t, p, model.StatusFailure, 1)
}

// endStep stores the end of the step "test" of the pipeline, the pipeline
// still running.
func (env *eventsV2Env) endStep(t *testing.T, p *model.Pipeline, state model.StatusValue, exitCode int) {
	t.Helper()
	workflows, err := env.store.WorkflowGetTree(p)
	require.NoError(t, err)
	step := workflows[0].Children[0]
	step.State, step.ExitCode, step.Finished = state, exitCode, time.Now().Unix()
	require.NoError(t, env.store.StepUpdate(step))
}

// finish stores the end of the pipeline and its workflow.
func (env *eventsV2Env) finish(t *testing.T, p *model.Pipeline, status model.StatusValue) {
	t.Helper()
	workflows, err := env.store.WorkflowGetTree(p)
	require.NoError(t, err)
	now := time.Now().Unix()
	for _, w := range workflows {
		w.State, w.Finished = status, now
		require.NoError(t, env.store.WorkflowUpdate(w))
		for _, s := range w.Children {
			if !s.State.IsDone() {
				s.State, s.Finished = status, now
				require.NoError(t, env.store.StepUpdate(s))
			}
		}
	}
	p.Status, p.Finished = status, now
	require.NoError(t, env.store.UpdatePipeline(p))
}

// wait asks to wait for the pipeline with the query.
func (env *eventsV2Env) wait(ctx context.Context, t *testing.T, id int64, query string, header http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/api/v2/pipelines/%d/wait?%s", env.srv.URL, id, query), nil)
	require.NoError(t, err)
	maps.Copy(req.Header, header)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// waitAsync waits for the pipeline in the background and returns once the
// wait subscribed to the hub.
func (env *eventsV2Env) waitAsync(ctx context.Context, t *testing.T, id int64, query string) <-chan *http.Response {
	t.Helper()
	before := env.hub.Subscribers()
	done := make(chan *http.Response, 1)
	go func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/api/v2/pipelines/%d/wait?%s", env.srv.URL, id, query), nil)
		if err != nil {
			close(done)
			return
		}
		maps.Copy(req.Header, loggedIn)
		resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // closed by who receives it
		if err != nil {
			close(done)
			return
		}
		done <- resp
	}()
	require.Eventually(t, func() bool { return env.hub.Subscribers() > before }, 5*time.Second, 5*time.Millisecond, "the wait subscribes")
	return done
}

// waitResult waits for the pipeline with the query, which must end with 200.
func (env *eventsV2Env) waitResult(t *testing.T, id int64, query string) WaitResult {
	t.Helper()
	resp := env.wait(t.Context(), t, id, query, loggedIn)
	defer resp.Body.Close()
	return result(t, resp)
}

// result is the result of a wait, which must end with 200. It closes the
// body.
func result(t *testing.T, resp *http.Response) WaitResult {
	t.Helper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))
	var r WaitResult
	require.NoError(t, json.Unmarshal(body, &r), "%s", body)
	assert.LessOrEqual(t, len(body), 1024)
	return r
}

func received(t *testing.T, done <-chan *http.Response) *http.Response {
	t.Helper()
	select {
	case resp, ok := <-done:
		require.True(t, ok, "the request failed")
		return resp
	case <-time.After(5 * time.Second):
		t.Fatal("the wait did not return")
		return nil
	}
}

func TestWaitV2NeedsALogin(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.pipeline(t, env.mine, 1)
	resp := env.wait(t.Context(), t, p.ID, "", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestWaitV2BadRequests(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.pipeline(t, env.mine, 1)
	theirs := env.pipeline(t, env.theirs, 1)
	for _, tt := range []struct {
		id     int64
		query  string
		status int
	}{
		{id: p.ID, query: "until=done", status: http.StatusBadRequest},
		{id: p.ID, query: "until=step:", status: http.StatusBadRequest},
		{id: p.ID, query: "timeout=abc", status: http.StatusBadRequest},
		{id: p.ID, query: "timeout=0", status: http.StatusBadRequest},
		{id: p.ID, query: "timeout=-5s", status: http.StatusBadRequest},
		{id: p.ID, query: "timeout=26m", status: http.StatusBadRequest},
		{id: p.ID, query: "timeout=1501", status: http.StatusBadRequest},
		{id: 0, status: http.StatusBadRequest},
		{id: theirs.ID, status: http.StatusNotFound},
		{id: 99999, status: http.StatusNotFound},
	} {
		t.Run(fmt.Sprint(tt.id, tt.query), func(t *testing.T) {
			resp := env.wait(t.Context(), t, tt.id, tt.query, loggedIn)
			defer resp.Body.Close()
			assert.Equal(t, tt.status, resp.StatusCode)
		})
	}
}

func TestWaitV2MetAlready(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.running(t, env.mine, 1)
	env.finish(t, p, model.StatusSuccess)

	r := env.waitResult(t, p.ID, "")
	assert.True(t, r.Met)
	assert.Equal(t, events.ReasonPipelineFinished, r.Reason)
	assert.Equal(t, "finished", r.Until)
	assert.Nil(t, r.Event, "no event: it was so already")
	assert.Equal(t, "#1 success · 1/1 ok", r.Summary)
	assert.Equal(t, p.ID, r.Pipeline.ID)
	assert.Equal(t, env.mine.FullName, r.Pipeline.Repo)
	assert.Equal(t, "success", r.Pipeline.Status)

	// failed can no longer be
	r = env.waitResult(t, p.ID, "until=failed")
	assert.False(t, r.Met)
	assert.Equal(t, events.ReasonPipelineFinished, r.Reason)

	r = env.waitResult(t, p.ID, "until=step:build/test")
	assert.True(t, r.Met)
	assert.Equal(t, events.ReasonStepFinished, r.Reason)

	r = env.waitResult(t, p.ID, "until=step:deploy")
	assert.False(t, r.Met)
	assert.Equal(t, events.ReasonNoSuchStep, r.Reason)
}

func TestWaitV2PublicRepo(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.running(t, env.open, 1)
	env.finish(t, p, model.StatusFailure)
	r := env.waitResult(t, p.ID, "until=failed")
	assert.True(t, r.Met)
	assert.Equal(t, events.ReasonStepFailed, r.Reason)
}

func TestWaitV2MetByAnEvent(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.running(t, env.mine, 1)

	done := env.waitAsync(t.Context(), t, p.ID, "until=failed&timeout=10s")
	env.failStep(t, p)
	env.observe(t, env.mine, p.ID)

	r := result(t, received(t, done)) //nolint:bodyclose // result closes it
	assert.True(t, r.Met)
	assert.Equal(t, events.ReasonStepFailed, r.Reason)
	assert.Equal(t, "running", r.Pipeline.Status, "the pipeline still runs")
	assert.Equal(t, 1, r.Pipeline.StepsFailed)
	assert.Equal(t, "#1 running · 0/1 ok · 1 failed", r.Summary)
	require.NotNil(t, r.Event)
	var e events.Event
	require.NoError(t, json.Unmarshal(r.Event, &e))
	assert.Equal(t, events.StepFailed, e.Type)
	assert.Equal(t, "test", e.Step)
	assert.Equal(t, p.ID, e.PipelineID)
}

func TestWaitV2NotMetByOtherEvents(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.running(t, env.mine, 1)
	other := env.running(t, env.mine, 2)

	done := env.waitAsync(t.Context(), t, p.ID, "until=finished&timeout=10s")
	// another pipeline ends, and a step of this one fails: not the end of it
	env.finish(t, other, model.StatusSuccess)
	env.observe(t, env.mine, other.ID)
	env.failStep(t, p)
	env.observe(t, env.mine, p.ID)
	select {
	case resp := <-done:
		resp.Body.Close()
		t.Fatal("the wait returned before the pipeline finished")
	case <-time.After(200 * time.Millisecond):
	}

	env.finish(t, p, model.StatusFailure)
	env.observe(t, env.mine, p.ID)
	r := result(t, received(t, done)) //nolint:bodyclose // result closes it
	assert.True(t, r.Met)
	assert.Equal(t, events.ReasonPipelineFinished, r.Reason)
	var e events.Event
	require.NoError(t, json.Unmarshal(r.Event, &e))
	assert.Equal(t, events.PipelineFinished, e.Type)
}

func TestWaitV2Timeout(t *testing.T) {
	env := newEventsV2Env(t)
	// the last successful run of the branch took 35 minutes
	last := env.pipeline(t, env.mine, 1)
	last.Status, last.Started, last.Finished = model.StatusSuccess, 1000, 1000+35*60
	require.NoError(t, env.store.UpdatePipeline(last))
	p := env.running(t, env.mine, 2)

	started := time.Now()
	resp := env.wait(t.Context(), t, p.ID, "timeout=200ms", loggedIn)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.GreaterOrEqual(t, time.Since(started), 200*time.Millisecond)
	assert.Equal(t, "#2 running · 0/1 ok · ETA 25m", resp.Header.Get(WaitSummaryHeader))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Empty(t, body)
}

func TestWaitV2TimeoutTellsThePipelineAsItIsThen(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.running(t, env.mine, 1)

	done := env.waitAsync(t.Context(), t, p.ID, "until=finished&timeout=300ms")
	// a step that succeeds does not end the wait
	env.endStep(t, p, model.StatusSuccess, 0)
	env.observe(t, env.mine, p.ID)
	resp := received(t, done)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "#1 running · 1/1 ok", resp.Header.Get(WaitSummaryHeader))
}

func TestWaitV2DefaultTimeout(t *testing.T) {
	env := newEventsV2Env(t)
	defaultTimeout := waitDefaultTimeout
	waitDefaultTimeout = 100 * time.Millisecond
	t.Cleanup(func() { waitDefaultTimeout = defaultTimeout })
	p := env.running(t, env.mine, 1)

	resp := env.wait(t.Context(), t, p.ID, "", loggedIn)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "#1 running · 0/1 ok", resp.Header.Get(WaitSummaryHeader))
}

func TestWaitV2ClientGone(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.running(t, env.mine, 1)

	ctx, cancel := context.WithCancelCause(t.Context())
	done := env.waitAsync(ctx, t, p.ID, "timeout=10m") //nolint:bodyclose // closed below
	cancel(nil)
	require.Eventually(t, func() bool { return env.hub.Subscribers() == 0 }, 5*time.Second, 5*time.Millisecond,
		"the wait of a client that is gone ends")
	if resp, ok := <-done; ok {
		resp.Body.Close()
	}
}

func TestWaitV2ServerStops(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.running(t, env.mine, 1)

	done := env.waitAsync(t.Context(), t, p.ID, "timeout=10m")
	env.stop()
	resp := received(t, done)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "#1 running · 0/1 ok", resp.Header.Get(WaitSummaryHeader))
}

func TestWaitV2ServerStopsWithoutTrustedAccess(t *testing.T) {
	env := newEventsV2Env(t)
	v2AccessTTL = 0 // nothing looked up is trusted
	p := env.running(t, env.mine, 1)

	done := env.waitAsync(t.Context(), t, p.ID, "timeout=10m")
	env.stop()
	resp := received(t, done)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	_, ok := resp.Header[WaitSummaryHeader]
	assert.False(t, ok, "no summary without an access that is still trusted")
}

// blockingStore holds the first read of the workflows of a pipeline until
// it is released.
type blockingStore struct {
	store.Store
	once             sync.Once
	entered, release chan struct{}
}

func (s *blockingStore) WorkflowGetTree(p *model.Pipeline) ([]*model.Workflow, error) {
	first := false
	s.once.Do(func() { first = true })
	if first {
		close(s.entered)
		<-s.release
	}
	return s.Store.WorkflowGetTree(p)
}

// TestWaitV2NoStaleSharedRead has one wait's read of the pipeline hang
// while the pipeline finishes: a wait that comes after the end does not take
// that read, and returns at once.
func TestWaitV2NoStaleSharedRead(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.running(t, env.mine, 1)

	blocking := &blockingStore{Store: env.store, entered: make(chan struct{}), release: make(chan struct{})}
	e := gin.New()
	e.Use(func(c *gin.Context) {
		c.Set("store", blocking)
		user, err := env.store.GetUser(env.user.ID)
		if err == nil {
			c.Set("user", user)
		}
	})
	e.GET("/api/v2/pipelines/:pipeline_id/wait", session.MustUser(), WaitPipelineV2(env.hub, env.durations))
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(blocking.release) }) }
	t.Cleanup(release) // before the server closes, should the test fail
	get := func() <-chan *http.Response {
		done := make(chan *http.Response, 1)
		go func() {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
				fmt.Sprintf("%s/api/v2/pipelines/%d/wait?timeout=10s", srv.URL, p.ID), nil)
			if err != nil {
				close(done)
				return
			}
			resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // closed by who receives it
			if err != nil {
				close(done)
				return
			}
			done <- resp
		}()
		return done
	}

	first := get()
	<-blocking.entered // its read hangs, the pipeline read as running
	env.finish(t, p, model.StatusSuccess)
	env.observe(t, env.mine, p.ID)

	second := get()
	r := result(t, received(t, second)) //nolint:bodyclose // result closes it
	assert.True(t, r.Met, "the read after the end is not the one that hangs")
	assert.Equal(t, "success", r.Pipeline.Status)

	release()
	r = result(t, received(t, first)) //nolint:bodyclose // result closes it
	assert.True(t, r.Met, "the hanging wait reads again after the event")
	assert.NotNil(t, r.Event)
}

func TestEventsV2StreamEndsWhenTheServerStops(t *testing.T) {
	env := newEventsV2Env(t)
	stream := env.connect(t, "", loggedIn)
	assert.Equal(t, "hello", stream.next(t).event)
	env.stop()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-stream.frames:
			if !ok {
				return // ended
			}
		case <-timeout:
			t.Fatal("the stream did not end")
		}
	}
}

func TestWaitV2TimeoutBounds(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.running(t, env.mine, 1)
	env.finish(t, p, model.StatusSuccess)
	for _, ok := range []string{"1", "1500", "25m", "1500s", "250ms"} {
		r := env.waitResult(t, p.ID, "timeout="+ok)
		assert.True(t, r.Met, ok)
	}
	for _, bad := range []string{"0", "-1", "1501", "25m1s", "9223372036854775807", "99999999999999999999", "1e3", "abc"} {
		resp := env.wait(t.Context(), t, p.ID, "timeout="+bad, loggedIn)
		resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, bad)
	}
	assert.Equal(t, http.StatusBadRequest, func() int {
		resp := env.wait(t.Context(), t, p.ID, "until=step:"+strings.Repeat("x", events.MaxUntilStepBytes+1), loggedIn)
		resp.Body.Close()
		return resp.StatusCode
	}(), "a step name too long")
}

func TestWaitV2ETAOfTheSameBranchAndEvent(t *testing.T) {
	env := newEventsV2Env(t)
	// a pull request on main and a push on dev took 35 minutes: no baseline
	// of a push on main
	pull := env.pipeline(t, env.mine, 1)
	pull.Event, pull.Status, pull.Started, pull.Finished = model.EventPull, model.StatusSuccess, 1000, 1000+35*60
	require.NoError(t, env.store.UpdatePipeline(pull))
	dev := env.pipeline(t, env.mine, 2)
	dev.Branch, dev.Status, dev.Started, dev.Finished = "dev", model.StatusSuccess, 1000, 1000+35*60
	require.NoError(t, env.store.UpdatePipeline(dev))
	p := env.running(t, env.mine, 3)

	resp := env.wait(t.Context(), t, p.ID, "timeout=50ms", loggedIn)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "#3 running · 0/1 ok", resp.Header.Get(WaitSummaryHeader), "no ETA")
}

func TestWaitV2FallsBehindTheEvents(t *testing.T) {
	env := newEventsV2EnvWith(t, events.Options{QueueLimit: 1})
	resubscribed := make(chan struct{}, 1)
	onWaitResubscribe = func() {
		select {
		case resubscribed <- struct{}{}:
		default:
		}
	}
	t.Cleanup(func() { onWaitResubscribe = func() {} })
	p := env.running(t, env.mine, 1)

	done := env.waitAsync(t.Context(), t, p.ID, "timeout=10s")
	// more events than the wait's queue at once, none of them its end
	flood := make([]events.Event, 1000)
	for i := range flood {
		flood[i] = events.Event{Type: events.PipelineFinished, RepoID: env.mine.ID, PipelineID: p.ID, Status: "running"}
	}
	env.hub.Append(flood...)
	select {
	case <-resubscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("the wait did not fall behind")
	}

	env.finish(t, p, model.StatusSuccess)
	env.observe(t, env.mine, p.ID)
	r := result(t, received(t, done)) //nolint:bodyclose // result closes it
	assert.True(t, r.Met)
	assert.Equal(t, events.ReasonPipelineFinished, r.Reason)
}

func TestWaitV2StepStatus(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.running(t, env.mine, 1)
	env.failStep(t, p)
	r := env.waitResult(t, p.ID, "until=step:test")
	assert.True(t, r.Met, "a step that failed ended")
	assert.Equal(t, events.ReasonStepFinished, r.Reason)
	assert.Equal(t, "failure", r.StepStatus)

	r = env.waitResult(t, p.ID, "until=failed")
	assert.Empty(t, r.StepStatus, "only of a step")
}

func TestWaitV2ResultIsSmall(t *testing.T) {
	long := strings.Repeat("ü", 300)
	repo := &model.Repo{ID: 7, FullName: long}
	p := &model.Pipeline{
		ID: 1 << 62, Number: 1 << 62, Branch: long, Status: model.StatusDeclined, Started: 1 << 40, Finished: 1 << 41,
		Workflows: []*model.Workflow{{Children: []*model.Step{{State: model.StatusFailure}, {State: model.StatusSuccess}}}},
	}
	w := newWaitPipeline(repo, p, 0, time.Now())
	assert.LessOrEqual(t, len(w.Repo), 48)
	assert.LessOrEqual(t, len(w.Branch), 48)

	event, err := events.Encode(events.Event{
		Type: events.StepFailed, Repo: long, Branch: long, Workflow: long, Step: long, Status: "failure", Summary: long,
	})
	require.NoError(t, err)
	result := WaitResult{
		V: events.SchemaVersion, Met: true, Reason: events.ReasonNoSuchStep,
		Until: "step:" + strings.Repeat("x", events.MaxUntilStepBytes), StepStatus: string(model.StatusDeclined),
		Pipeline: w, Summary: summary.Line(p, 0, time.Now()), Event: event,
	}
	data, err := json.Marshal(result)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(data), 1024, "%s", data)
}

func TestWaitV2AccessLostWhileWaiting(t *testing.T) {
	env := newEventsV2Env(t)
	v2AccessTTL = 0 // the access is looked up again before the answer
	p := env.running(t, env.mine, 1)

	done := env.waitAsync(t.Context(), t, p.ID, "timeout=10s")
	env.perm(t, env.mine, false)
	env.finish(t, p, model.StatusSuccess)
	env.observe(t, env.mine, p.ID)
	resp := received(t, done)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
