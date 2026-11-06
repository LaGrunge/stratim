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

package scenarios

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/cli/pipeline/watch"
	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/summary"
	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// watchResult is how a watch of the CLI went.
type watchResult struct {
	code int
	err  error
	out  string
}

// startWatch watches the pipeline as "woodpecker-cli pipeline watch" does,
// through the test server's /api/v2, and returns once the watch follows the
// stream, unless it may end at once.
func startWatch(t *testing.T, env *setup.ServerEnv, created *model.Pipeline, opt watch.Options, mayEnd bool) <-chan watchResult {
	t.Helper()
	subscribers := env.Events.Subscribers()
	done := make(chan watchResult, 1)
	go func() {
		var out bytes.Buffer
		opt.Out = &out
		opt.Timeout = time.Minute
		target := watch.Target{PipelineID: created.ID, Name: env.Fixtures.Repo.FullName + " #1", Status: string(created.Status)}
		code, err := watch.Watch(t.Context(), env.WaitV2Client(t), target, opt)
		done <- watchResult{code, err, out.String()}
	}()
	if mayEnd {
		return done
	}
	require.Eventually(t, func() bool { return env.Events.Subscribers() > subscribers }, 10*time.Second, 5*time.Millisecond, "the watch follows the stream")
	return done
}

func watchEnded(t *testing.T, done <-chan watchResult) watchResult {
	t.Helper()
	select {
	case got := <-done:
		t.Logf("watch, exit %d:\n%s", got.code, got.out)
		return got
	case <-time.After(time.Minute):
		t.Fatal("the watch did not end")
		return watchResult{}
	}
}

// typesOf are the types of the lines of a watch.
func typesOf(out string) []string {
	var types []string
	for line := range strings.Lines(out) {
		if fields := strings.Fields(line); len(fields) > 1 && !strings.HasPrefix(line, " ") {
			types = append(types, fields[1])
		}
	}
	return types
}

// TestWatchPipeline watches a pipeline that waits for approval until it
// finished: a line per event, exit 0; watched again once finished, the
// watch ends at once.
func TestWatchPipeline(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: simpleSuccessYAML},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	env.Fixtures.Repo.RequireApproval = model.RequireApprovalAllEvents
	require.NoError(t, env.Store.UpdateRepo(env.Fixtures.Repo))
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	require.Equal(t, model.StatusBlocked, created.Status)

	done := startWatch(t, env, created, watch.Options{ExitOn: watch.ExitOnFinished, Level: watch.LevelLine, Severity: woodpecker.SeverityInfo}, false)
	_, err = pipeline.Approve(t.Context(), env.Store, created, env.Fixtures.Owner, env.Fixtures.Repo)
	require.NoError(t, err)

	got := watchEnded(t, done)
	require.NoError(t, got.err)
	assert.Equal(t, watch.ExitSuccess, got.code)
	types := typesOf(got.out)
	assert.Equal(t, "watch", types[0])
	assert.Contains(t, types, "pipeline.started")
	assert.Contains(t, types, "step.finished", "info and above")
	assert.NotContains(t, types, "step.started", "debug")
	assert.Equal(t, []string{"pipeline.finished", "done"}, types[len(types)-2:])
	assert.Contains(t, got.out, "  done               -  success  exit 0: #1 success")

	// finished already: the state read at the start ends the watch
	again := startWatch(t, env, created, watch.Options{ExitOn: watch.ExitOnFailed, Level: watch.LevelSummary}, true)
	got = watchEnded(t, again)
	require.NoError(t, got.err)
	assert.Equal(t, watch.ExitSuccess, got.code)
	assert.Equal(t, []string{"watch", "done"}, typesOf(got.out))
	assert.Contains(t, got.out, "exit 0: #1 success · 3/3 ok")
}

// TestWatchFailure watches a pipeline for a failure: the watch ends with
// exit 1 once the step failed and its cause came, with the first line of the
// log the cause quotes.
func TestWatchFailure(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{{Name: ".woodpecker.yaml", Data: []byte(gtestFailure)}})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	env.Fixtures.Repo.RequireApproval = model.RequireApprovalAllEvents
	require.NoError(t, env.Store.UpdateRepo(env.Fixtures.Repo))
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)

	done := startWatch(t, env, created, watch.Options{
		ExitOn: watch.ExitOnFailed, Level: watch.LevelLine, Severity: woodpecker.SeverityNotice, CauseWait: 30 * time.Second,
	}, false)
	_, err = pipeline.Approve(t.Context(), env.Store, created, env.Fixtures.Owner, env.Fixtures.Repo)
	require.NoError(t, err)

	got := watchEnded(t, done)
	require.NoError(t, got.err)
	assert.Equal(t, watch.ExitFailed, got.code)
	types := typesOf(got.out)
	assert.Contains(t, types, "step.failed")
	assert.Equal(t, []string{"step.explained", "done"}, types[len(types)-2:])
	assert.Contains(t, got.out, "  step.explained     woodpecker/unit  failure  cause gtest/high: gtest: ColumnstoreRowgroup.SerializeNulls failed at rowgroup-tests.cpp:211\n")
	assert.Contains(t, got.out, "\n          untrusted log| /src/tests/rowgroup-tests.cpp:211: Failure\n")
	assert.NotContains(t, got.out, "Expected equality", "only the first line of the log")
}

// TestWatchStatusRules checks that the statuses of woodpecker-go are the
// server's and that a failure is the one the server counts.
func TestWatchStatusRules(t *testing.T) {
	for _, pair := range [][2]string{
		{string(model.StatusSuccess), woodpecker.StatusSuccess},
		{string(model.StatusFailure), woodpecker.StatusFailure},
		{string(model.StatusTimeout), woodpecker.StatusTimeout},
		{string(model.StatusKilled), woodpecker.StatusKilled},
		{string(model.StatusError), woodpecker.StatusError},
		{string(model.StatusCanceled), woodpecker.StatusCanceled},
		{string(model.StatusDeclined), woodpecker.StatusDeclined},
	} {
		assert.Equal(t, pair[0], pair[1])
	}
	for _, status := range []model.StatusValue{
		model.StatusSkipped, model.StatusPending, model.StatusRunning, model.StatusSuccess, model.StatusFailure, model.StatusTimeout,
		model.StatusKilled, model.StatusCanceled, model.StatusError, model.StatusBlocked, model.StatusDeclined, model.StatusCreated,
	} {
		assert.Equal(t, summary.Failed(status), woodpecker.IsFailureStatus(string(status)), "%s", status)
	}
}
