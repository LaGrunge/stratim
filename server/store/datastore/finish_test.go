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

package datastore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// createTestRepo creates the repository of ID 1 the pipelines belong to.
func createTestRepo(t *testing.T, store *storage) {
	t.Helper()
	require.NoError(t, store.CreateRepo(&model.Repo{UserID: 1, FullName: "octocat/finish", Owner: "octocat", Name: "finish"}))
}

// Of two ends of a workflow written at once only the first is written.
func TestWorkflowFinish(t *testing.T) {
	store, closer := newTestStore(t, new(model.Workflow), new(model.Step))
	defer closer()

	workflow := &model.Workflow{PipelineID: 1, PID: 1, Name: "a", State: model.StatusRunning, Started: 1}
	require.NoError(t, store.WorkflowsCreate([]*model.Workflow{workflow}))

	first := *workflow
	first.State, first.Finished = model.StatusKilled, 10
	won, err := store.WorkflowFinish(&first)
	require.NoError(t, err)
	assert.True(t, won)

	second := *workflow
	second.State, second.Finished = model.StatusSuccess, 20
	won, err = store.WorkflowFinish(&second)
	require.NoError(t, err)
	assert.False(t, won, "it ended already")

	got, err := store.WorkflowLoad(workflow.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, got.State)
	assert.Equal(t, int64(10), got.Finished)
	assert.Equal(t, int64(1), got.Started, "the other columns stay")
}

// A pipeline killed is not ended again; only status and finished are written.
func TestPipelineFinish(t *testing.T) {
	store, closer := newTestStore(t, new(model.Repo), new(model.Pipeline), new(model.Step))
	defer closer()

	createTestRepo(t, store)
	running := &model.Pipeline{RepoID: 1, Number: 1, Status: model.StatusRunning, Branch: "main"}
	killed := &model.Pipeline{RepoID: 1, Number: 2, Status: model.StatusKilled, Finished: 5}
	require.NoError(t, store.CreatePipeline(running))
	require.NoError(t, store.CreatePipeline(killed))

	stale := *running
	stale.Status, stale.Finished, stale.Branch = model.StatusSuccess, 30, "stale"
	won, err := store.PipelineFinish(&stale)
	require.NoError(t, err)
	assert.True(t, won)
	got, err := store.GetPipeline(running.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusSuccess, got.Status)
	assert.Equal(t, "main", got.Branch, "only status and finished are written")

	again := *killed
	again.Status, again.Finished = model.StatusSuccess, 40
	won, err = store.PipelineFinish(&again)
	require.NoError(t, err)
	assert.False(t, won)
	got, err = store.GetPipeline(killed.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, got.Status)
	assert.Equal(t, int64(5), got.Finished)
}

// The sweep finds the workflows left pending or running in pipelines killed
// or canceled long enough ago, by ID, a batch at most.
func TestWorkflowsLeftInCanceledPipelines(t *testing.T) {
	store, closer := newTestStore(t, new(model.Repo), new(model.Pipeline), new(model.Workflow), new(model.Step))
	defer closer()

	createTestRepo(t, store)
	killed := &model.Pipeline{RepoID: 1, Number: 1, Status: model.StatusKilled, Finished: 100}
	canceled := &model.Pipeline{RepoID: 1, Number: 4, Status: model.StatusCanceled, Finished: 100}
	fresh := &model.Pipeline{RepoID: 1, Number: 3, Status: model.StatusKilled, Finished: 500}
	running := &model.Pipeline{RepoID: 1, Number: 2, Status: model.StatusRunning}
	for _, p := range []*model.Pipeline{killed, canceled, fresh, running} {
		require.NoError(t, store.CreatePipeline(p))
	}
	require.NoError(t, store.WorkflowsCreate([]*model.Workflow{
		{PipelineID: killed.ID, PID: 1, Name: "finalizing", State: model.StatusRunning, Children: []*model.Step{
			{PipelineID: killed.ID, PID: 2, PPID: 1, Name: "logs", UUID: "logs", State: model.StatusRunning},
		}},
		{PipelineID: killed.ID, PID: 3, Name: "done", State: model.StatusKilled},
		{PipelineID: killed.ID, PID: 4, Name: "polled", State: model.StatusPending},
		{PipelineID: canceled.ID, PID: 1, Name: "polled-of-canceled", State: model.StatusPending},
		{PipelineID: fresh.ID, PID: 1, Name: "fresh", State: model.StatusRunning},
		{PipelineID: running.ID, PID: 1, Name: "active", State: model.StatusRunning},
	}))

	workflows, err := store.WorkflowsLeftInCanceledPipelines(200, 10)
	require.NoError(t, err)
	names := make([]string, 0, len(workflows))
	for _, w := range workflows {
		names = append(names, w.Name)
	}
	assert.Equal(t, []string{"finalizing", "polled", "polled-of-canceled"}, names, "the pipeline canceled after 200 is not old enough")
	require.Len(t, workflows[0].Children, 1)
	assert.Equal(t, "logs", workflows[0].Children[0].Name)

	workflows, err = store.WorkflowsLeftInCanceledPipelines(600, 10)
	require.NoError(t, err)
	assert.Len(t, workflows, 4)

	workflows, err = store.WorkflowsLeftInCanceledPipelines(600, 2)
	require.NoError(t, err)
	require.Len(t, workflows, 2, "a batch at most")
	assert.Equal(t, "finalizing", workflows[0].Name, "by ID")
}

// Init's writes: a pipeline is started only if still pending (a cancel
// claimed meanwhile stays), a workflow only if still pending (an Init
// retried, or a cancel's skip, stays); only status/state and started.
func TestStartWrites(t *testing.T) {
	store, closer := newTestStore(t, new(model.Repo), new(model.Pipeline), new(model.Workflow), new(model.Step))
	defer closer()

	createTestRepo(t, store)
	pending := &model.Pipeline{RepoID: 1, Number: 1, Status: model.StatusPending, Branch: "main"}
	require.NoError(t, store.CreatePipeline(pending))
	stale := *pending
	stale.Status, stale.Started, stale.Branch = model.StatusRunning, 7, "stale"
	won, err := store.PipelineStart(&stale)
	require.NoError(t, err)
	assert.True(t, won)
	got, err := store.GetPipeline(pending.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusRunning, got.Status)
	assert.Equal(t, int64(7), got.Started)
	assert.Equal(t, "main", got.Branch, "only status and started")

	claimed := &model.Pipeline{RepoID: 1, Number: 2, Status: model.StatusKilled, Finished: 5, CancelInfo: &model.CancelInfo{CanceledByUser: "octocat"}}
	require.NoError(t, store.CreatePipeline(claimed))
	start := *claimed
	start.Status, start.Started, start.CancelInfo = model.StatusRunning, 7, nil
	won, err = store.PipelineStart(&start)
	require.NoError(t, err)
	assert.False(t, won)
	got, err = store.GetPipeline(claimed.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, got.Status, "the cancel stays")
	assert.Equal(t, "octocat", got.CancelInfo.CanceledByUser)

	workflow := &model.Workflow{PipelineID: pending.ID, PID: 1, Name: "a", State: model.StatusPending, AgentID: 3}
	require.NoError(t, store.WorkflowsCreate([]*model.Workflow{workflow}))
	started := *workflow
	started.State, started.Started, started.AgentID = model.StatusRunning, 8, 0
	won, err = store.WorkflowStart(&started)
	require.NoError(t, err)
	assert.True(t, won)
	again := started
	again.Started = 9
	won, err = store.WorkflowStart(&again)
	require.NoError(t, err)
	assert.False(t, won, "running already")
	gotW, err := store.WorkflowLoad(workflow.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusRunning, gotW.State)
	assert.Equal(t, int64(8), gotW.Started)
	assert.Equal(t, int64(3), gotW.AgentID, "only state and started")
}

// The cancel's writes from the tree it read before the agents were told:
// a workflow skipped and a step ended only if still as read.
func TestCancelWrites(t *testing.T) {
	store, closer := newTestStore(t, new(model.Repo), new(model.Pipeline), new(model.Workflow), new(model.Step))
	defer closer()

	createTestRepo(t, store)
	pipeline := &model.Pipeline{RepoID: 1, Number: 1, Status: model.StatusCanceled, Finished: 5}
	require.NoError(t, store.CreatePipeline(pipeline))
	pending := &model.Workflow{PipelineID: pipeline.ID, PID: 1, Name: "pending", State: model.StatusPending, Children: []*model.Step{
		{PipelineID: pipeline.ID, PID: 3, PPID: 1, Name: "a", UUID: "a", State: model.StatusPending},
		{PipelineID: pipeline.ID, PID: 4, PPID: 1, Name: "b", UUID: "b", State: model.StatusPending},
	}}
	started := &model.Workflow{PipelineID: pipeline.ID, PID: 2, Name: "started", State: model.StatusPending}
	require.NoError(t, store.WorkflowsCreate([]*model.Workflow{pending, started}))

	// its agent started it meanwhile
	require.NoError(t, store.WorkflowUpdate(&model.Workflow{ID: started.ID, PipelineID: pipeline.ID, PID: 2, Name: "started", State: model.StatusRunning, Started: 3}))
	skip := *started
	skip.State, skip.Finished = model.StatusSkipped, 6
	won, err := store.WorkflowSkip(&skip)
	require.NoError(t, err)
	assert.False(t, won)
	got, err := store.WorkflowLoad(started.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusRunning, got.State, "its agent's start stays")

	skip = *pending
	skip.State, skip.Finished = model.StatusSkipped, 6
	won, err = store.WorkflowSkip(&skip)
	require.NoError(t, err)
	assert.True(t, won)

	steps, err := store.StepListFromWorkflowFind(pending)
	require.NoError(t, err)
	require.Len(t, steps, 2)
	// step b: its agent ran it meanwhile
	ran := *steps[1]
	ran.State, ran.Started, ran.Finished = model.StatusSuccess, 3, 4
	require.NoError(t, store.StepUpdate(&ran))
	for _, step := range steps {
		canceled := *step
		canceled.State = model.StatusCanceled
		won, err := store.StepFinish(&canceled, model.StatusPending)
		require.NoError(t, err)
		assert.Equal(t, step.Name == "a", won, "step %s", step.Name)
	}
	steps, err = store.StepListFromWorkflowFind(pending)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCanceled, steps[0].State)
	assert.Equal(t, model.StatusSuccess, steps[1].State, "its agent's end stays")
	assert.Equal(t, int64(3), steps[1].Started)

	// a pipeline canceled whose polled workflow runs: killed, once
	killed := *pipeline
	killed.Status, killed.Finished = model.StatusKilled, 99
	won, err = store.PipelineCanceledKilled(&killed)
	require.NoError(t, err)
	assert.True(t, won)
	won, err = store.PipelineCanceledKilled(&killed)
	require.NoError(t, err)
	assert.False(t, won)
	gotP, err := store.GetPipeline(pipeline.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, gotP.Status)
	assert.Equal(t, int64(5), gotP.Finished, "only the status")
}

// Of a cancel and an end, or two cancels, only the first is written: only
// status, finished and cancel_info, and only from pending, running or
// blocked.
func TestPipelineCancel(t *testing.T) {
	store, closer := newTestStore(t, new(model.Repo), new(model.Pipeline), new(model.Step))
	defer closer()

	createTestRepo(t, store)
	running := &model.Pipeline{RepoID: 1, Number: 1, Status: model.StatusRunning, Branch: "main"}
	require.NoError(t, store.CreatePipeline(running))

	first := *running
	first.Status, first.Finished, first.CancelInfo, first.Branch = model.StatusKilled, 10, &model.CancelInfo{CanceledByUser: "first"}, "stale"
	won, err := store.PipelineCancel(&first)
	require.NoError(t, err)
	assert.True(t, won)

	second := *running
	second.Status, second.Finished, second.CancelInfo = model.StatusKilled, 20, &model.CancelInfo{SupersededBy: 2}
	won, err = store.PipelineCancel(&second)
	require.NoError(t, err)
	assert.False(t, won, "canceled already")

	got, err := store.GetPipeline(running.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, got.Status)
	assert.Equal(t, int64(10), got.Finished)
	assert.Equal(t, "first", got.CancelInfo.CanceledByUser)
	assert.Equal(t, "main", got.Branch, "only the cancel's columns are written")

	ended := &model.Pipeline{RepoID: 1, Number: 2, Status: model.StatusSuccess, Finished: 5}
	require.NoError(t, store.CreatePipeline(ended))
	late := *ended
	late.Status, late.Finished = model.StatusKilled, 30
	won, err = store.PipelineCancel(&late)
	require.NoError(t, err)
	assert.False(t, won, "a pipeline that ended stays as it ended")
	got, err = store.GetPipeline(ended.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusSuccess, got.Status)
}

// An agent's report of a step is written only if the step is still as
// read: a late one does not overwrite the end its workflow's kill wrote,
// nor write a killed step back to running. The kill ends a step not ended
// whatever its agent reported meanwhile.
func TestStepReport(t *testing.T) {
	store, closer := newTestStore(t, new(model.Step))
	defer closer()

	step := &model.Step{PipelineID: 1, PID: 1, PPID: 1, Name: "a", UUID: "a", State: model.StatusPending}
	sess := store.engine.NewSession()
	defer sess.Close()
	require.NoError(t, store.stepCreate(sess, []*model.Step{step}))

	running := *step
	running.State, running.Started = model.StatusRunning, 3
	won, err := store.StepReport(&running, model.StatusPending)
	require.NoError(t, err)
	assert.True(t, won)

	// its workflow killed: the kill reads it pending, finds it running
	killed := *step
	killed.State, killed.Finished = model.StatusCanceled, 9
	won, err = store.StepFinish(&killed, model.StatusPending, model.StatusRunning)
	require.NoError(t, err)
	assert.True(t, won, "the kill ends a step its agent started meanwhile")

	late := running
	late.State, late.Finished, late.ExitCode = model.StatusSuccess, 10, 0
	won, err = store.StepReport(&late, model.StatusRunning)
	require.NoError(t, err)
	assert.False(t, won, "the late report loses")

	got, err := store.StepLoad(1, step.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusCanceled, got.State)
	assert.Equal(t, int64(3), got.Started, "the report's start stays")
	assert.Equal(t, int64(9), got.Finished)
}
