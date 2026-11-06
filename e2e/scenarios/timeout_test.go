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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

// TestStepTimeout: a step that runs past its own timeout ends as timeout,
// with exit code 124 and its error, well before the workflow's timeout; the
// steps after it go on as after any failure.
func TestStepTimeout(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: too-long
    image: dummy
    commands: [echo sleeping]
    timeout: 1s
    failure: ignore
    environment:
      SLEEP: 30s
  - name: after
    image: dummy
    commands: [echo after]
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	start := time.Now()
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)
	assert.Less(t, time.Since(start), 20*time.Second, "the step's timeout should have stopped the 30s step")

	assert.Equal(t, model.StatusSuccess, finished.Status, "the timed-out step's failure is ignored")
	step := setup.WaitForStep(t, env.Store, finished, "too-long")
	assert.Equal(t, model.StatusTimeout, step.State)
	assert.Equal(t, 124, step.ExitCode)
	assert.Contains(t, step.Error, "step timed out after 1s")
	assert.Equal(t, model.StatusSuccess, setup.WaitForStep(t, env.Store, finished, "after").State)
}

// TestStepTimeoutStatus: a step's timeout counts as a failure for the steps
// after it (status: failure matches) and is named over a plain failure in
// the workflow's and the pipeline's status.
func TestStepTimeoutStatus(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: too-long
    image: dummy
    commands: [echo sleeping]
    timeout: 1s
    environment:
      SLEEP: 30s
  - name: on-failure
    image: dummy
    commands: [echo on failure]
    when:
      status: failure
  - name: fails
    image: dummy
    commands: [echo fails]
    when:
      status: failure
    environment:
      STEP_EXIT_CODE: '1'
  - name: on-success
    image: dummy
    commands: [echo on success]
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	assert.Equal(t, model.StatusTimeout, finished.Status, "a timeout is named over a failure")
	assert.Equal(t, model.StatusTimeout, setup.WaitForStep(t, env.Store, finished, "too-long").State)
	assert.Equal(t, model.StatusSuccess, setup.WaitForStep(t, env.Store, finished, "on-failure").State,
		"status: failure matches after a timeout")
	assert.Equal(t, model.StatusFailure, setup.WaitForStep(t, env.Store, finished, "fails").State)
	assert.Equal(t, model.StatusSkipped,
		setup.WaitForStepStatus(t, env.Store, finished, "on-success", model.StatusSkipped, 30*time.Second).State)
	workflows := setup.WaitForWorkflowsDone(t, env.Store, finished)
	require.Len(t, workflows, 1)
	assert.Equal(t, model.StatusTimeout, workflows[0].State)
}

// TestStepExitCode124: a step exiting with timeout(1)'s code by itself
// failed, it did not time out.
func TestStepExitCode124(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: exits-124
    image: dummy
    commands: [timeout 5 make]
    timeout: 1m
    environment:
      STEP_EXIT_CODE: '124'
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	assert.Equal(t, model.StatusFailure, finished.Status)
	step := setup.WaitForStep(t, env.Store, finished, "exits-124")
	assert.Equal(t, model.StatusFailure, step.State)
	assert.Equal(t, 124, step.ExitCode)
}

// TestWorkflowTimeoutFromConfig: a workflow's own timeout takes the place of
// the repository's, and ends the workflow, its running step and the
// pipeline as timeout. The agent counts in seconds here, so 2 is two
// seconds.
func TestWorkflowTimeoutFromConfig(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
timeout: 2
steps:
  - name: too-long
    image: dummy
    commands: [echo sleeping]
    environment:
      SLEEP: 30s
`)},
	})
	repo := env.Fixtures.Repo
	repo.Timeout = 60
	require.NoError(t, env.Store.UpdateRepo(repo), "set repository timeout")

	agent := setup.StartAgent(t, env.GRPCAddr, setup.WithTimeoutUnit(time.Second))
	setup.WaitForAgentRegistered(t, env.Store, agent)

	start := time.Now()
	created, err := pipeline.Create(t.Context(), env.Store, repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	elapsed := time.Since(start)
	assert.Equal(t, model.StatusTimeout, finished.Status)
	assert.GreaterOrEqual(t, elapsed, 2*time.Second, "the workflow ended before its timeout")
	assert.Less(t, elapsed, 20*time.Second, "the workflow's own timeout, not the repository's 60, should have stopped it")

	step := setup.WaitForStepStatus(t, env.Store, finished, "too-long", model.StatusTimeout, 30*time.Second)
	assert.Equal(t, model.StatusTimeout, step.State)
	workflows := setup.WaitForWorkflowsDone(t, env.Store, finished)
	require.Len(t, workflows, 1)
	assert.Equal(t, model.StatusTimeout, workflows[0].State)
}

// TestWorkflowTimeoutPendingStep: a step that had not started when its
// workflow timed out is canceled, not killed: no one killed it.
func TestWorkflowTimeoutPendingStep(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
timeout: 2
steps:
  - name: too-long
    image: dummy
    commands: [echo sleeping]
    environment:
      SLEEP: 30s
  - name: next
    image: dummy
    commands: [echo next]
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr, setup.WithTimeoutUnit(time.Second))
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	assert.Equal(t, model.StatusTimeout, finished.Status)
	assert.Equal(t, model.StatusTimeout,
		setup.WaitForStepStatus(t, env.Store, finished, "too-long", model.StatusTimeout, 30*time.Second).State)
	assert.Equal(t, model.StatusCanceled,
		setup.WaitForStepStatus(t, env.Store, finished, "next", model.StatusCanceled, 30*time.Second).State)
}

// TestStepTimeoutFailureCancel: a step with failure: cancel that times out
// cancels the pipeline as a failure does: the step stays timeout, the step
// after it is canceled before it starts and the pipeline ends killed.
func TestStepTimeoutFailureCancel(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: too-long
    image: dummy
    commands: [echo sleeping]
    timeout: 1s
    failure: cancel
    environment:
      SLEEP: 30s
  - name: on-failure
    image: dummy
    commands: [echo on failure]
    when:
      status: failure
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	assert.Equal(t, model.StatusKilled, finished.Status)
	assert.Equal(t, model.StatusTimeout, setup.WaitForStep(t, env.Store, finished, "too-long").State)
	assert.Equal(t, model.StatusCanceled,
		setup.WaitForStepStatus(t, env.Store, finished, "on-failure", model.StatusCanceled, 30*time.Second).State)
	workflows := setup.WaitForWorkflowsDone(t, env.Store, finished)
	require.Len(t, workflows, 1)
	assert.Equal(t, model.StatusKilled, workflows[0].State)
}

// TestDependsOnTimedOutWorkflow: a workflow that timed out failed for the
// workflows depending on it: one running on failure runs, one running on
// success (the default) does not.
func TestDependsOnTimedOutWorkflow(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker/a.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: too-long
    image: dummy
    commands: [echo sleeping]
    timeout: 1s
    environment:
      SLEEP: 30s
`)},
		{Name: ".woodpecker/b.yaml", Data: []byte(`
skip_clone: true
depends_on: [a]
runs_on: [failure]
steps:
  - name: on-failure
    image: dummy
    commands: [echo on failure]
`)},
		{Name: ".woodpecker/c.yaml", Data: []byte(`
skip_clone: true
depends_on: [a]
steps:
  - name: on-success
    image: dummy
    commands: [echo on success]
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	assert.Equal(t, model.StatusTimeout, finished.Status)
	workflows := setup.WaitForWorkflowsDone(t, env.Store, finished)
	byName := make(map[string]model.StatusValue, len(workflows))
	for _, w := range workflows {
		byName[w.Name] = w.State
	}
	assert.Equal(t, map[string]model.StatusValue{
		"a": model.StatusTimeout,
		"b": model.StatusSuccess,
		"c": model.StatusSkipped,
	}, byName)
}
