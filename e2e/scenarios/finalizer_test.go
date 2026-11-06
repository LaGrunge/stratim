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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

// finalizers are the on_finish steps the scenarios below end with: one that
// always runs and prints the outcome it saw, one for failures only.
const finalizers = `
  - name: logs
    image: dummy
    commands: [echo collecting logs]
    on_finish: true
    environment:
      ECHO_ENV: STRATIM_PIPELINE_STATUS,CI_PIPELINE_STATUS
  - name: on-failure
    image: dummy
    commands: [echo reporting the failure]
    on_finish: true
    when:
      status: failure
`

// stepLog waits for a step's log to print STRATIM_PIPELINE_STATUS and returns it.
func stepLog(t *testing.T, env *setup.ServerEnv, step *model.Step) string {
	const want = "STRATIM_PIPELINE_STATUS="
	t.Helper()
	var log string
	require.Eventually(t, func() bool {
		entries, err := env.Store.LogFind(t.Context(), step)
		require.NoError(t, err)
		var b strings.Builder
		for _, e := range entries {
			b.Write(e.Data)
			b.WriteByte('\n')
		}
		log = b.String()
		return strings.Contains(log, want)
	}, 10*time.Second, 50*time.Millisecond, "the log of step %s has %q", step.Name, want)
	return log
}

// TestFinalizerAfterSuccess: the finalizer runs after the other steps
// succeeded and sees success; the one for failures is skipped.
func TestFinalizerAfterSuccess(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: build
    image: dummy
    commands: [echo build]
` + finalizers)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	assert.Equal(t, model.StatusSuccess, finished.Status)
	logs := setup.WaitForStep(t, env.Store, finished, "logs")
	assert.Equal(t, model.StatusSuccess, logs.State)
	log := stepLog(t, env, logs)
	assert.Contains(t, log, "STRATIM_PIPELINE_STATUS=success")
	assert.Contains(t, log, "CI_PIPELINE_STATUS=success")
	assert.Equal(t, model.StatusSkipped,
		setup.WaitForStepStatus(t, env.Store, finished, "on-failure", model.StatusSkipped, 30*time.Second).State)
	assert.Equal(t, []string{"build", "logs", "on-failure"}, stepNames(t, env, finished), "finalizers come last")
}

// TestFinalizerAfterFailure: after a failed step the finalizer for failures
// runs too, and both see failure.
func TestFinalizerAfterFailure(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: build
    image: dummy
    commands: [echo build]
    environment:
      STEP_EXIT_CODE: '1'
  - name: test
    image: dummy
    commands: [echo test]
` + finalizers)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	assert.Equal(t, model.StatusFailure, finished.Status)
	assert.Equal(t, model.StatusSkipped,
		setup.WaitForStepStatus(t, env.Store, finished, "test", model.StatusSkipped, 30*time.Second).State)
	logs := setup.WaitForStep(t, env.Store, finished, "logs")
	assert.Equal(t, model.StatusSuccess, logs.State)
	assert.Contains(t, stepLog(t, env, logs), "STRATIM_PIPELINE_STATUS=failure")
	assert.Equal(t, model.StatusSuccess, setup.WaitForStep(t, env.Store, finished, "on-failure").State)
}

// TestFinalizerAfterCancel: a workflow canceled mid-step still runs its
// finalizers, which see killed; the step after the canceled one never
// starts and the pipeline stays killed.
func TestFinalizerAfterCancel(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: long-running
    image: dummy
    commands: [echo starting long job]
    environment:
      SLEEP: 30s
  - name: after-cancel
    image: dummy
    commands: [echo this should never run]
` + finalizers + `
  - name: slow-finalizer
    image: dummy
    commands: [echo uploading]
    on_finish: true
    environment:
      SLEEP: 2s
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	setup.WaitForStepRunning(t, env.Store, created.ID, "long-running")

	forge, err := env.Manager.ForgeByID(env.Fixtures.Forge.ID)
	require.NoError(t, err)
	running, err := env.Store.GetPipeline(created.ID)
	require.NoError(t, err)
	require.NoError(t, pipeline.Cancel(t.Context(), forge, env.Store, env.WorkflowEnd, env.Fixtures.Repo, env.Fixtures.Owner, running, nil))

	// the pipeline is killed at once, its workflow runs on for its
	// finalizers, held by its agent
	setup.WaitForStepRunning(t, env.Store, created.ID, "slow-finalizer")
	killed, err := env.Store.GetPipeline(created.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, killed.Status)
	assert.NotZero(t, killed.Finished)
	workflows, err := env.Store.WorkflowGetTree(killed)
	require.NoError(t, err)
	require.Len(t, workflows, 1)
	assert.Equal(t, model.StatusRunning, workflows[0].State)
	tasks := env.Queue.Info(t.Context()).Running
	require.Len(t, tasks, 1, "its agent still holds the workflow")

	finished := setup.WaitForPipeline(t, env.Store, created.ID)
	assert.Equal(t, model.StatusSuccess,
		setup.WaitForStepStatus(t, env.Store, finished, "slow-finalizer", model.StatusSuccess, 30*time.Second).State)
	require.Eventually(t, func() bool {
		workflow, err := env.Store.WorkflowLoad(workflows[0].ID)
		require.NoError(t, err)
		return workflow.State == model.StatusKilled
	}, 30*time.Second, 50*time.Millisecond, "the workflow ends killed")
	final, err := env.Store.GetPipeline(created.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, final.Status, "and the pipeline stays killed")
	assert.Equal(t, killed.Finished, final.Finished, "its end is not written again")

	logs := setup.WaitForStepStatus(t, env.Store, finished, "logs", model.StatusSuccess, 30*time.Second)
	log := stepLog(t, env, logs)
	assert.Contains(t, log, "STRATIM_PIPELINE_STATUS=killed")
	assert.Contains(t, log, "CI_PIPELINE_STATUS=failure")
	assert.Equal(t, model.StatusSuccess,
		setup.WaitForStepStatus(t, env.Store, finished, "on-failure", model.StatusSuccess, 30*time.Second).State,
		"status: failure matches a kill")
	assert.Equal(t, model.StatusKilled,
		setup.WaitForStepStatus(t, env.Store, finished, "long-running", model.StatusKilled, 30*time.Second).State)
	assert.Equal(t, model.StatusCanceled,
		setup.WaitForStepStatus(t, env.Store, finished, "after-cancel", model.StatusCanceled, 30*time.Second).State)
}

// orphanedConfig is a workflow of a step and a finalizer.
const orphanedConfig = "skip_clone: true\nsteps:\n  - name: build\n    image: dummy\n    commands: [echo]\n  - name: logs\n    image: dummy\n    commands: [echo]\n    on_finish: true\n"

// orphanedRunningWorkflow is a pipeline running a workflow the queue has no
// task of, as after a server restart: made running by hand, with no agent,
// its build step running, its finalizer pending.
func orphanedRunningWorkflow(t *testing.T, env *setup.ServerEnv) (*model.Pipeline, *model.Workflow) {
	t.Helper()
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	workflows, err := env.Store.WorkflowGetTree(created)
	require.NoError(t, err)
	require.Len(t, workflows, 1)
	workflow := workflows[0]
	require.NoError(t, env.Queue.ErrorAtOnce(t.Context(), []string{fmt.Sprint(workflow.ID)}, errors.New("gone")))
	workflow.State, workflow.Started = model.StatusRunning, time.Now().Unix()
	require.NoError(t, env.Store.WorkflowUpdate(workflow))
	build := workflow.Children[0]
	build.State, build.Started = model.StatusRunning, time.Now().Unix()
	require.NoError(t, env.Store.StepUpdate(build))
	running, err := env.Store.GetPipeline(created.ID)
	require.NoError(t, err)
	running.Status = model.StatusRunning
	require.NoError(t, env.Store.UpdatePipeline(running))
	return running, workflow
}

// killedLongAgo kills pipeline as a cancel did, at canceledAt, leaving its
// workflow running: its agent ran the finalizers when the server restarted.
func killedLongAgo(t *testing.T, env *setup.ServerEnv, pipeline *model.Pipeline, canceledAt time.Time) {
	t.Helper()
	pipeline.Status = model.StatusKilled
	pipeline.Finished = canceledAt.Unix()
	pipeline.CancelInfo = &model.CancelInfo{CanceledByUser: env.Fixtures.Owner.Login}
	require.NoError(t, env.Store.UpdatePipeline(pipeline))
}

// TestSweepCanceledWorkflow: a workflow left running in a pipeline canceled
// longer ago than its finalizers may run (a restart lost its queue task, its
// agent never reported) is killed by the server's sweep, its steps with it,
// its pipeline staying killed.
func TestSweepCanceledWorkflow(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{{Name: ".woodpecker.yaml", Data: []byte(orphanedConfig)}})
	created, _ := orphanedRunningWorkflow(t, env)
	killedLongAgo(t, env, created, time.Now().Add(-pipeline.CanceledWorkflowsBound-time.Minute))

	workflows := setup.WaitForWorkflowsDone(t, env.Store, created)
	require.Len(t, workflows, 1)
	assert.Equal(t, model.StatusKilled, workflows[0].State)
	// the sweep ends the workflow first and its steps right after
	setup.WaitForStepStatus(t, env.Store, created, "build", model.StatusKilled, 30*time.Second)
	setup.WaitForStepStatus(t, env.Store, created, "logs", model.StatusCanceled, 30*time.Second)
	steps, err := env.Store.StepList(created.ID)
	require.NoError(t, err)
	byName := make(map[string]model.StatusValue, len(steps))
	for _, step := range steps {
		byName[step.Name] = step.State
	}
	assert.Equal(t, map[string]model.StatusValue{"build": model.StatusKilled, "logs": model.StatusCanceled}, byName)
	final, err := env.Store.GetPipeline(created.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, final.Status)
	assert.Equal(t, created.Finished, final.Finished, "its end is not written again")
}

// TestSweepLeavesFreshCancel: a workflow left running by a recent cancel
// is not swept: its agent may still be running its finalizers.
func TestSweepLeavesFreshCancel(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{{Name: ".woodpecker.yaml", Data: []byte(orphanedConfig)}})
	created, workflow := orphanedRunningWorkflow(t, env)
	killedLongAgo(t, env, created, time.Now().Add(-time.Minute))

	time.Sleep(10 * setup.SweepInterval)
	running, err := env.Store.WorkflowLoad(workflow.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusRunning, running.State, "not swept")
}

// TestCancelsAtOnce: two first cancels at once (a newer pipeline's and a
// user's) are one cancel: the finalizers still run.
func TestCancelsAtOnce(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: long-running
    image: dummy
    commands: [echo starting long job]
    environment:
      SLEEP: 30s
  - name: slow-finalizer
    image: dummy
    commands: [echo uploading]
    on_finish: true
    environment:
      SLEEP: 1s
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	setup.WaitForStepRunning(t, env.Store, created.ID, "long-running")

	forge, err := env.Manager.ForgeByID(env.Fixtures.Forge.ID)
	require.NoError(t, err)
	running, err := env.Store.GetPipeline(created.ID)
	require.NoError(t, err)
	errs := make(chan error, 2)
	for _, info := range []*model.CancelInfo{{SupersededBy: 99}, {CanceledByUser: env.Fixtures.Owner.Login}} {
		go func() {
			stale := *running
			errs <- pipeline.Cancel(t.Context(), forge, env.Store, env.WorkflowEnd, env.Fixtures.Repo, env.Fixtures.Owner, &stale, info)
		}()
	}
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)

	finished := setup.WaitForPipeline(t, env.Store, created.ID)
	assert.Equal(t, model.StatusKilled, finished.Status)
	setup.WaitForWorkflowsDone(t, env.Store, finished)
	assert.Equal(t, model.StatusSuccess,
		setup.WaitForStepStatus(t, env.Store, finished, "slow-finalizer", model.StatusSuccess, 30*time.Second).State,
		"the finalizer ran to its end")
}

// TestCancelWithoutQueueEntry: a running workflow the queue has no task of
// (as after a server restart) has no agent to stop it: a cancel kills it at
// once, its steps with it.
func TestCancelWithoutQueueEntry(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte("skip_clone: true\nsteps:\n  - name: build\n    image: dummy\n    commands: [echo]\n  - name: logs\n    image: dummy\n    commands: [echo]\n    on_finish: true\n")},
	})
	created, workflow := orphanedRunningWorkflow(t, env)
	running, err := env.Store.GetPipeline(created.ID)
	require.NoError(t, err)

	forge, err := env.Manager.ForgeByID(env.Fixtures.Forge.ID)
	require.NoError(t, err)
	require.NoError(t, pipeline.Cancel(t.Context(), forge, env.Store, env.WorkflowEnd, env.Fixtures.Repo, env.Fixtures.Owner, running, nil))

	killed, err := env.Store.GetPipeline(created.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, killed.Status)
	ended, err := env.Store.WorkflowLoad(workflow.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, ended.State, "killed at once")
	steps, err := env.Store.StepList(created.ID)
	require.NoError(t, err)
	byName := make(map[string]model.StatusValue, len(steps))
	for _, step := range steps {
		byName[step.Name] = step.State
	}
	assert.Equal(t, map[string]model.StatusValue{"build": model.StatusKilled, "logs": model.StatusCanceled}, byName)
}

// TestFinalizerSecondCancel: a second cancel of a pipeline whose finalizers
// run stops them: the running one ends killed, the one after it canceled.
func TestFinalizerSecondCancel(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: long-running
    image: dummy
    commands: [echo starting long job]
    environment:
      SLEEP: 30s
  - name: hanging-finalizer
    image: dummy
    commands: [echo uploading]
    on_finish: true
    environment:
      SLEEP: 30s
  - name: last-finalizer
    image: dummy
    commands: [echo]
    on_finish: true
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	setup.WaitForStepRunning(t, env.Store, created.ID, "long-running")

	forge, err := env.Manager.ForgeByID(env.Fixtures.Forge.ID)
	require.NoError(t, err)
	cancel := func() {
		t.Helper()
		running, err := env.Store.GetPipeline(created.ID)
		require.NoError(t, err)
		require.NoError(t, pipeline.Cancel(t.Context(), forge, env.Store, env.WorkflowEnd, env.Fixtures.Repo, env.Fixtures.Owner, running, &model.CancelInfo{CanceledByUser: env.Fixtures.Owner.Login}))
	}
	cancel()
	setup.WaitForStepRunning(t, env.Store, created.ID, "hanging-finalizer")
	start := time.Now()
	cancel()

	finished := setup.WaitForPipeline(t, env.Store, created.ID)
	// the pipeline was killed by the first cancel: its workflow ends now
	setup.WaitForWorkflowsDone(t, env.Store, finished)
	assert.Less(t, time.Since(start), 20*time.Second, "the second cancel stopped the 30s finalizer")
	assert.Equal(t, model.StatusKilled, finished.Status)
	assert.Equal(t, model.StatusKilled,
		setup.WaitForStepStatus(t, env.Store, finished, "hanging-finalizer", model.StatusKilled, 30*time.Second).State)
	assert.Equal(t, model.StatusCanceled,
		setup.WaitForStepStatus(t, env.Store, finished, "last-finalizer", model.StatusCanceled, 30*time.Second).State)
}

// TestFinalizerAfterWorkflowTimeout: a workflow that timed out runs its
// finalizers, which see timeout, and ends timeout. The agent counts in
// seconds here.
func TestFinalizerAfterWorkflowTimeout(t *testing.T) {
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
` + finalizers + `
  - name: on-timeout
    image: dummy
    commands: [echo timed out]
    on_finish: true
    when:
      status: timeout
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr, setup.WithTimeoutUnit(time.Second))
	setup.WaitForAgentRegistered(t, env.Store, agent)

	start := time.Now()
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)
	assert.Less(t, time.Since(start), 20*time.Second)

	assert.Equal(t, model.StatusTimeout, finished.Status)
	assert.Equal(t, model.StatusTimeout, setup.WaitForStep(t, env.Store, finished, "too-long").State)
	logs := setup.WaitForStep(t, env.Store, finished, "logs")
	assert.Equal(t, model.StatusSuccess, logs.State)
	assert.Contains(t, stepLog(t, env, logs), "STRATIM_PIPELINE_STATUS=timeout")
	assert.Equal(t, model.StatusSuccess, setup.WaitForStep(t, env.Store, finished, "on-failure").State, "a timeout is a failure")
	assert.Equal(t, model.StatusSuccess, setup.WaitForStep(t, env.Store, finished, "on-timeout").State)
	workflows := setup.WaitForWorkflowsDone(t, env.Store, finished)
	require.Len(t, workflows, 1)
	assert.Equal(t, model.StatusTimeout, workflows[0].State)
}

// TestFinalizerFails: a failing finalizer fails a workflow that succeeded.
func TestFinalizerFails(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: build
    image: dummy
    commands: [echo build]
  - name: upload
    image: dummy
    commands: [echo upload]
    on_finish: true
    environment:
      STEP_EXIT_CODE: '1'
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	assert.Equal(t, model.StatusFailure, finished.Status)
	assert.Equal(t, model.StatusFailure, setup.WaitForStep(t, env.Store, finished, "upload").State)
}

// TestStatusSkipped: a step with status: skipped runs once an earlier step
// was skipped; a workflow with status: skipped runs once a workflow it
// depends on was skipped.
func TestStatusSkipped(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker/a.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: only-on-failure
    image: dummy
    commands: [echo]
    when:
      status: failure
  - name: after-skip
    image: dummy
    commands: [echo]
    when:
      status: skipped
`)},
		{Name: ".woodpecker/b.yaml", Data: []byte(`
skip_clone: true
depends_on: [a]
when:
  status: failure
steps:
  - name: on-failure
    image: dummy
    commands: [echo]
`)},
		{Name: ".woodpecker/c.yaml", Data: []byte(`
skip_clone: true
depends_on: [b]
when:
  status: skipped
steps:
  - name: b-skipped
    image: dummy
    commands: [echo]
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	assert.Equal(t, model.StatusSkipped,
		setup.WaitForStepStatus(t, env.Store, finished, "only-on-failure", model.StatusSkipped, 30*time.Second).State)
	assert.Equal(t, model.StatusSuccess, setup.WaitForStep(t, env.Store, finished, "after-skip").State)
	workflows := setup.WaitForWorkflowsDone(t, env.Store, finished)
	byName := make(map[string]model.StatusValue, len(workflows))
	for _, w := range workflows {
		byName[w.Name] = w.State
	}
	assert.Equal(t, map[string]model.StatusValue{
		"a": model.StatusSuccess,
		"b": model.StatusSkipped,
		"c": model.StatusSuccess,
	}, byName)
}

// TestStatusSkippedNoCascade: a workflow skipped waiting for a skipped
// dependency that did not come is no skip for the one after it waiting for
// one: A succeeds, B and C wait for a skip, neither runs.
func TestStatusSkippedNoCascade(t *testing.T) {
	waiting := func(dep string) []byte {
		return []byte("skip_clone: true\ndepends_on: [" + dep + "]\nwhen:\n  status: skipped\nsteps:\n  - name: report\n    image: dummy\n    commands: [echo]\n")
	}
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker/a.yaml", Data: []byte("skip_clone: true\nsteps:\n  - name: build\n    image: dummy\n    commands: [echo]\n")},
		{Name: ".woodpecker/b.yaml", Data: waiting("a")},
		{Name: ".woodpecker/c.yaml", Data: waiting("b")},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	workflows := setup.WaitForWorkflowsDone(t, env.Store, finished)
	byName := make(map[string]model.StatusValue, len(workflows))
	for _, w := range workflows {
		byName[w.Name] = w.State
	}
	assert.Equal(t, map[string]model.StatusValue{
		"a": model.StatusSuccess,
		"b": model.StatusSkipped,
		"c": model.StatusSkipped,
	}, byName)
}
