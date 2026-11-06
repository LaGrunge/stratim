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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/explain"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// stepsV2Env serves the step routes of /api/v2 over a sqlite store: a user
// who may read the private repo "mine", not the private repo "theirs", and
// the public repo "open", each with a pipeline of one failed step.
type stepsV2Env struct {
	store              store.Store
	user               *model.User
	mine, theirs, open *model.Step
	mineRepo           *model.Repo
	// explainer and windows serve the routes; nil when they are not
	// available.
	explainer *explain.Explainer
	windows   *explain.Windows
	srv       *httptest.Server
	// stopping, if set, is the context of the server: done once it stops.
	stopping context.Context
}

// stepsV2MineLog is the log of the step of "mine": a gtest failure, colored.
var stepsV2MineLog = []string{
	"[ RUN      ] Suite.Name",
	"/src/t.cc:12: Failure",
	"\x1b[31mExpected equality\x1b[0m",
	"[  FAILED  ] Suite.Name (1 ms)",
	"long " + strings.Repeat("x", 5000),
	"[  FAILED  ] 1 test, listed below:",
	"[  FAILED  ] Suite.Name",
}

func newStepsV2Env(t *testing.T) *stepsV2Env {
	t.Helper()
	gin.SetMode(gin.TestMode)
	s := newTestStore(t)
	env := &stepsV2Env{store: s}

	env.user = &model.User{Login: "me", ForgeRemoteID: "u1", Hash: "h1"}
	require.NoError(t, s.CreateUser(env.user))
	other := &model.User{Login: "other", ForgeRemoteID: "u2", Hash: "h2"}
	require.NoError(t, s.CreateUser(other))

	failedStep := func(name string, owner *model.User, visibility model.RepoVisibility, log []string) (*model.Step, *model.Repo) {
		r := &model.Repo{
			UserID: owner.ID, ForgeRemoteID: model.ForgeRemoteID("r-" + name), Owner: owner.Login, Name: name,
			FullName: owner.Login + "/" + name, IsActive: true, Visibility: visibility,
		}
		require.NoError(t, s.CreateRepo(r))
		p := &model.Pipeline{RepoID: r.ID, Number: 1, Status: model.StatusFailure}
		step := &model.Step{UUID: "uuid-" + name, PID: 1, Name: "test", State: model.StatusFailure, ExitCode: 1, Started: 1, Finished: 2}
		require.NoError(t, s.CreatePipeline(p, step))
		var entries []*model.LogEntry
		for i, l := range log {
			entries = append(entries, &model.LogEntry{StepID: step.ID, Line: i, Data: []byte(l)})
		}
		if len(entries) > 0 {
			require.NoError(t, s.LogAppend(t.Context(), step, entries))
		}
		return step, r
	}
	env.mine, env.mineRepo = failedStep("mine", env.user, model.VisibilityPrivate, stepsV2MineLog)
	env.theirs, _ = failedStep("theirs", other, model.VisibilityPrivate, []string{"secret"})
	env.open, _ = failedStep("open", other, model.VisibilityPublic, []string{"a.cc:1:2: error: boom"})
	require.NoError(t, s.PermUpsert(&model.Perm{UserID: env.user.ID, RepoID: env.mineRepo.ID, Pull: true, Synced: time.Now().Unix()}))

	manager := server.Config.Services.Manager
	server.Config.Services.Manager = nil // no forge to sync a permission with, unless a test sets one
	t.Cleanup(func() { server.Config.Services.Manager = manager })

	env.explainer = explain.New(s, explain.Options{})
	env.windows = explain.NewWindows(s)
	return env
}

func (env *stepsV2Env) serve(t *testing.T) {
	t.Helper()
	e := gin.New()
	if env.stopping != nil {
		e.Use(middleware.Stopping(env.stopping))
	}
	e.Use(func(c *gin.Context) {
		c.Set("store", env.store)
		if c.GetHeader("X-Test-User") != "" {
			c.Set("user", env.user)
		}
	})
	e.GET("/api/v2/steps/:step_id/explain", session.MustUser(), StepExplainV2(env.explainer))
	e.GET("/api/v2/steps/:step_id/log", session.MustUser(), StepLogV2(env.windows))
	env.srv = httptest.NewServer(e)
	t.Cleanup(env.srv.Close)
}

// get gets the path of /api/v2 as the user, or as nobody, and gives the
// status and the body.
func (env *stepsV2Env) get(t *testing.T, path string, loggedIn bool) (int, string) {
	t.Helper()
	if env.srv == nil {
		env.serve(t)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, env.srv.URL+"/api/v2/"+path, nil)
	require.NoError(t, err)
	if loggedIn {
		req.Header.Set("X-Test-User", "1")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func (env *stepsV2Env) window(t *testing.T, step *model.Step, query string) explain.Window {
	t.Helper()
	status, body := env.get(t, fmt.Sprintf("steps/%d/log?%s", step.ID, query), true)
	require.Equal(t, http.StatusOK, status, body)
	var w explain.Window
	require.NoError(t, json.Unmarshal([]byte(body), &w))
	assert.True(t, w.Untrusted)
	assert.Equal(t, step.ID, w.StepID)
	return w
}

func lineNumbers(w explain.Window) []int {
	out := []int{}
	for _, l := range w.Lines {
		out = append(out, l.N)
	}
	return out
}

func TestStepsV2Access(t *testing.T) {
	env := newStepsV2Env(t)
	env.serve(t) // for the subtests
	for _, route := range []string{"explain", "log"} {
		t.Run(route, func(t *testing.T) {
			status, _ := env.get(t, fmt.Sprintf("steps/%d/%s", env.mine.ID, route), false)
			assert.Equal(t, http.StatusUnauthorized, status, "a login is needed")

			status, _ = env.get(t, fmt.Sprintf("steps/%d/%s", env.mine.ID, route), true)
			assert.Equal(t, http.StatusOK, status, "a step of a repo the user may pull")
			status, _ = env.get(t, fmt.Sprintf("steps/%d/%s", env.open.ID, route), true)
			assert.Equal(t, http.StatusOK, status, "a step of a public repo")

			status, body := env.get(t, fmt.Sprintf("steps/%d/%s", env.theirs.ID, route), true)
			assert.Equal(t, http.StatusNotFound, status, "a step of a repo the user may not read")
			assert.NotContains(t, body, "secret")
			status, _ = env.get(t, fmt.Sprintf("steps/%d/%s", env.open.ID+100, route), true)
			assert.Equal(t, http.StatusNotFound, status, "a step that is not there")

			for _, id := range []string{"x", "0", "-1"} {
				status, _ = env.get(t, "steps/"+id+"/"+route, true)
				assert.Equal(t, http.StatusBadRequest, status, "step id %s", id)
			}
		})
	}
}

// The permission is checked as the rest of the API checks it: an old one is
// synced from the forge first.
func TestStepsV2AccessSyncsAnOldPermission(t *testing.T) {
	env := newStepsV2Env(t)
	require.NoError(t, env.store.PermUpsert(&model.Perm{UserID: env.user.ID, RepoID: env.mineRepo.ID, Pull: true, Synced: 1}))
	forge := forge_mocks.NewMockForge(t)
	forge.On("Repo", mock.Anything, mock.Anything, env.mineRepo.ForgeRemoteID, env.mineRepo.Owner, env.mineRepo.Name).
		Return(&model.Repo{Perm: &model.Perm{Pull: false}}, nil)
	manager := manager_mocks.NewMockManager(t)
	manager.On("ForgeFromRepo", mock.Anything).Return(forge, nil)
	server.Config.Services.Manager = manager

	status, _ := env.get(t, fmt.Sprintf("steps/%d/log", env.mine.ID), true)
	assert.Equal(t, http.StatusNotFound, status, "the forge took the pull away")
	perm, err := env.store.PermFind(env.user, env.mineRepo)
	require.NoError(t, err)
	assert.False(t, perm.Pull)
	assert.Greater(t, perm.Synced, int64(1), "synced")
}

func TestStepsV2Explain(t *testing.T) {
	env := newStepsV2Env(t)
	status, body := env.get(t, fmt.Sprintf("steps/%d/explain", env.mine.ID), true)
	require.Equal(t, http.StatusOK, status, body)
	var r explain.Result
	require.NoError(t, json.Unmarshal([]byte(body), &r))
	assert.True(t, r.Untrusted)
	assert.Equal(t, env.mine.ID, r.StepID)
	assert.Equal(t, "failure", r.Status)
	assert.Equal(t, len(stepsV2MineLog), r.Lines)
	require.NotEmpty(t, r.Causes)
	assert.Equal(t, "gtest", r.Causes[0].Kind)
	assert.Equal(t, 2, r.Causes[0].Line)
	assert.Equal(t, explain.Line{N: 3, Text: "Expected equality"}, r.Causes[0].Excerpt[1], "without escape sequences")
}

func TestStepsV2Unavailable(t *testing.T) {
	env := newStepsV2Env(t)
	env.explainer, env.windows = nil, nil
	for _, route := range []string{"explain", "log"} {
		status, _ := env.get(t, fmt.Sprintf("steps/%d/%s", env.mine.ID, route), true)
		assert.Equal(t, http.StatusServiceUnavailable, status, route)
	}
}

// Logs are read a few at a time; a request beyond them is answered 503.
func TestStepsV2LogReadsAreBounded(t *testing.T) {
	env := newStepsV2Env(t)
	for range MaxStepLogReadsV2 {
		stepLogReadsV2 <- struct{}{}
	}
	t.Cleanup(func() {
		for range MaxStepLogReadsV2 {
			<-stepLogReadsV2
		}
	})
	for _, route := range []string{"explain", "log"} {
		status, _ := env.get(t, fmt.Sprintf("steps/%d/%s", env.mine.ID, route), true)
		assert.Equal(t, http.StatusServiceUnavailable, status, route)
	}
}

func TestStepsV2LogWindows(t *testing.T) {
	env := newStepsV2Env(t)
	all := len(stepsV2MineLog)

	w := env.window(t, env.mine, "")
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7}, lineNumbers(w), "all, from the first")
	assert.Zero(t, w.Next, "the end of the log")
	assert.Equal(t, "Expected equality", w.Lines[2].Text, "without escape sequences")
	assert.Len(t, w.Lines[4].Text, explain.MaxWindowLineBytes, "a long line is cut")
	assert.True(t, strings.HasSuffix(w.Lines[4].Text, "…"))

	w = env.window(t, env.mine, "from=2&lines=3")
	assert.Equal(t, []int{2, 3, 4}, lineNumbers(w))
	assert.Equal(t, 5, w.Next)
	w = env.window(t, env.mine, "from=5&lines=3")
	assert.Equal(t, []int{5, 6, 7}, lineNumbers(w))
	assert.Zero(t, w.Next, "the window ends with the log")

	w = env.window(t, env.mine, fmt.Sprintf("from=%d", all+1))
	assert.Empty(t, lineNumbers(w), "after the end")
	assert.Zero(t, w.Next)

	w = env.window(t, env.mine, "from=-2")
	assert.Equal(t, []int{6, 7}, lineNumbers(w), "the last lines")
	w = env.window(t, env.mine, "from=-100&lines=2")
	assert.Equal(t, []int{1, 2}, lineNumbers(w), "from the end, at most to the start")
	assert.Equal(t, 3, w.Next)

	w = env.window(t, env.mine, "grep=FAILED")
	assert.Equal(t, []int{4, 6, 7}, lineNumbers(w))
	w = env.window(t, env.mine, "grep=failed")
	assert.Empty(t, lineNumbers(w), "case matters")
	w = env.window(t, env.mine, "grep=failed&ignore_case=true&lines=2")
	assert.Equal(t, []int{4, 6}, lineNumbers(w))
	assert.Equal(t, 7, w.Next)
	w = env.window(t, env.mine, "grep=failed&ignore_case=true&from=7")
	assert.Equal(t, []int{7}, lineNumbers(w))
	w = env.window(t, env.mine, "grep=.*")
	assert.Empty(t, lineNumbers(w), "a text, not a pattern")

	w = env.window(t, env.mine, "lines=500")
	assert.Len(t, w.Lines, all)
}

func TestStepsV2LogWindowBadRequests(t *testing.T) {
	env := newStepsV2Env(t)
	for _, query := range []string{
		"from=0", "from=x", "lines=0", "lines=501", "lines=x",
		"grep=" + strings.Repeat("g", explain.MaxGrepBytes+1), "ignore_case=maybe",
	} {
		status, _ := env.get(t, fmt.Sprintf("steps/%d/log?%s", env.mine.ID, query), true)
		assert.Equal(t, http.StatusBadRequest, status, query)
	}
	status, _ := env.get(t, fmt.Sprintf("steps/%d/log?grep=%s", env.mine.ID, strings.Repeat("g", explain.MaxGrepBytes)), true)
	assert.Equal(t, http.StatusOK, status)
}

// slowLogs hands out the first line, then waits for its context.
type slowLogs struct{}

func (slowLogs) LogLastLine(ctx context.Context, _ *model.Step) (int, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func (slowLogs) LogStreamFrom(ctx context.Context, _ *model.Step, from int, fn func(*model.LogEntry) error) error {
	if from == 0 {
		if err := fn(&model.LogEntry{Line: 0, Data: []byte("first")}); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

// A read ends once the server stops, without waiting for its timeout.
func TestStepsV2StopsWithTheServer(t *testing.T) {
	env := newStepsV2Env(t)
	stopping, stop := context.WithCancelCause(t.Context())
	env.stopping = stopping
	env.windows = explain.NewWindows(slowLogs{})
	env.explainer = explain.New(slowLogs{}, explain.Options{})
	stop(nil)

	start := time.Now()
	for _, route := range []string{"explain", "log?from=-1"} {
		status, _ := env.get(t, fmt.Sprintf("steps/%d/%s", env.mine.ID, route), true)
		assert.Equal(t, http.StatusServiceUnavailable, status, route)
	}
	assert.Less(t, time.Since(start), stepLogV2Timeout/2)
}

// A client that went away mid-read is not answered, nor is it an error.
func TestStepsV2ClientGone(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(nil)
	c.Request = httptest.NewRequestWithContext(ctx, http.MethodGet, "/api/v2/steps/1/log", nil)
	stepLogV2Error(c, ctx, &model.Step{ID: 1}, context.Canceled)
	assert.False(t, c.Writer.Written(), "nothing written")
	assert.Empty(t, c.Errors, "no error")
	assert.True(t, c.IsAborted())
}

func TestStepsV2LogInTime(t *testing.T) {
	env := newStepsV2Env(t)
	timeout := stepLogV2Timeout
	stepLogV2Timeout = 50 * time.Millisecond
	t.Cleanup(func() { stepLogV2Timeout = timeout })
	env.windows = explain.NewWindows(slowLogs{})
	env.explainer = explain.New(slowLogs{}, explain.Options{})

	w := env.window(t, env.mine, "")
	assert.Equal(t, []int{1}, lineNumbers(w), "what was found in time")
	assert.Equal(t, 2, w.Next, "and where to go on from")

	status, _ := env.get(t, fmt.Sprintf("steps/%d/log?from=5", env.mine.ID), true)
	assert.Equal(t, http.StatusServiceUnavailable, status, "nothing of the window in time")
	status, _ = env.get(t, fmt.Sprintf("steps/%d/log?from=-1", env.mine.ID), true)
	assert.Equal(t, http.StatusServiceUnavailable, status, "the end not found in time")
	status, _ = env.get(t, fmt.Sprintf("steps/%d/explain", env.mine.ID), true)
	assert.Equal(t, http.StatusServiceUnavailable, status, "no explanation in time")
}
