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

package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/compiler"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// recordedEvents is a publisher recording the pipelines it was told of.
type recordedEvents struct{ pipelines []*model.Pipeline }

func (r *recordedEvents) PublishPipelineEvent(_ context.Context, _ *model.Repo, pipeline *model.Pipeline) error {
	r.pipelines = append(r.pipelines, pipeline)
	return nil
}

// The end of a step left pending or running, by how its workflow ended.
func TestStepEndState(t *testing.T) {
	started := &model.Step{Started: 1}
	unstarted := &model.Step{}
	for _, tt := range []struct {
		end                StepEnd
		started, unstarted model.StatusValue
	}{
		{StepEndWorkflowDone, model.StatusSuccess, model.StatusKilled},
		{StepEndWorkflowStopped, model.StatusSuccess, model.StatusCanceled},
		{StepEndKilled, model.StatusKilled, model.StatusCanceled},
	} {
		assert.Equal(t, tt.started, StepEndState(started, tt.end), "started, end %d", tt.end)
		assert.Equal(t, tt.unstarted, StepEndState(unstarted, tt.end), "never started, end %d", tt.end)
	}
}

// The sweep kills, through the end shared with Done, the workflows running
// in pipelines canceled longer ago than the finalizers' bound; its pipeline
// stays killed, its subscribers are told.
func TestSweepCanceledWorkflows(t *testing.T) {
	left := &model.Workflow{ID: 1, PipelineID: 7, State: model.StatusRunning, Children: []*model.Step{
		{ID: 2, State: model.StatusRunning, Started: 1, Finalizer: true},
		{ID: 3, State: model.StatusPending, Finalizer: true},
	}}
	now := time.Now()
	s := store_mocks.NewMockStore(t)
	s.EXPECT().WorkflowsLeftInCanceledPipelines(now.Add(-CanceledWorkflowsBound).Unix(), SweepBatch).Return([]*model.Workflow{left}, nil).Once()
	s.EXPECT().WorkflowFinish(killedWorkflow(1)).Return(true, nil).Once()
	s.EXPECT().StepFinish(stepIs(2, model.StatusKilled), []model.StatusValue{model.StatusPending, model.StatusRunning}).Return(true, nil).Once()
	s.EXPECT().StepFinish(stepIs(3, model.StatusCanceled), []model.StatusValue{model.StatusPending, model.StatusRunning}).Return(true, nil).Once()
	s.EXPECT().GetPipeline(int64(7)).RunAndReturn(func(int64) (*model.Pipeline, error) {
		return &model.Pipeline{ID: 7, RepoID: 8, Status: model.StatusKilled}, nil
	})
	s.EXPECT().GetRepo(int64(8)).Return(&model.Repo{ID: 8}, nil).Once()
	s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{{ID: 1, State: model.StatusKilled}}, nil).Once()
	s.EXPECT().PipelineFinish(mock.Anything).Return(false, nil).Once()
	events := &recordedEvents{}

	require.NoError(t, SweepCanceledWorkflows(t.Context(), WorkflowEnd{Store: s, Publisher: events}, now))
	assert.Equal(t, model.StatusKilled, left.State)
	require.Len(t, events.pipelines, 1)
	assert.Equal(t, model.StatusKilled, events.pipelines[0].Status, "the pipeline stays killed")
}

// A workflow the sweep cannot kill does not stop it: the next is killed,
// and the sweep goes on (it tries again on its next pass).
func TestSweepGoesOnAfterAFailure(t *testing.T) {
	first := &model.Workflow{ID: 1, PipelineID: 7, State: model.StatusRunning}
	second := &model.Workflow{ID: 2, PipelineID: 7, State: model.StatusRunning}
	now := time.Now()
	s := store_mocks.NewMockStore(t)
	s.EXPECT().WorkflowsLeftInCanceledPipelines(mock.Anything, SweepBatch).Return([]*model.Workflow{first, second}, nil).Once()
	s.EXPECT().WorkflowFinish(killedWorkflow(1)).Return(false, errors.New("store unavailable")).Once()
	s.EXPECT().WorkflowFinish(killedWorkflow(2)).Return(true, nil).Once()
	s.EXPECT().GetPipeline(int64(7)).RunAndReturn(func(int64) (*model.Pipeline, error) {
		return &model.Pipeline{ID: 7, RepoID: 8, Status: model.StatusKilled}, nil
	})
	s.EXPECT().GetRepo(int64(8)).Return(&model.Repo{ID: 8}, nil).Once()
	s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{{ID: 1, State: model.StatusRunning}, {ID: 2, State: model.StatusKilled}}, nil).Once()
	events := &recordedEvents{}

	assert.NoError(t, SweepCanceledWorkflows(t.Context(), WorkflowEnd{Store: s, Publisher: events}, now))
	assert.Equal(t, model.StatusRunning, first.State, "left for the next pass")
	assert.Equal(t, model.StatusKilled, second.State)
	assert.Len(t, events.pipelines, 1)
}

// The end of a workflow ends its pipeline when it was the last one running
// and the pipeline did not end already; with another running it does not.
func TestWorkflowEndEndsPipeline(t *testing.T) {
	t.Run("last", func(t *testing.T) {
		s := store_mocks.NewMockStore(t)
		s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{{ID: 1, State: model.StatusSuccess, Finished: 9}}, nil).Once()
		s.EXPECT().PipelineFinish(mock.MatchedBy(func(p *model.Pipeline) bool {
			return p.Status == model.StatusSuccess && p.Finished == 9
		})).Return(true, nil).Once()
		events := &recordedEvents{}
		e := WorkflowEnd{Store: s, Publisher: events}
		require.NoError(t, e.Ended(t.Context(), zerolog.Nop(), &model.Repo{}, &model.Pipeline{ID: 7, Status: model.StatusRunning}, &model.Workflow{ID: 1, Finished: 9}))
		assert.Equal(t, model.StatusSuccess, events.pipelines[0].Status)
	})
	t.Run("another running", func(t *testing.T) {
		s := store_mocks.NewMockStore(t)
		s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{
			{ID: 1, State: model.StatusSuccess}, {ID: 2, State: model.StatusRunning},
		}, nil).Once()
		e := WorkflowEnd{Store: s, Publisher: &recordedEvents{}}
		require.NoError(t, e.Ended(t.Context(), zerolog.Nop(), &model.Repo{}, &model.Pipeline{ID: 7, Status: model.StatusRunning}, &model.Workflow{ID: 1}))
		s.AssertNotCalled(t, "PipelineFinish", mock.Anything)
	})
}

// recordedStatuses records what was marked for the forge.
type recordedStatuses struct {
	pipelines []int64
	workflows [][2]int64
}

func (r *recordedStatuses) Pipeline(pipelineID int64) { r.pipelines = append(r.pipelines, pipelineID) }

func (r *recordedStatuses) Workflow(pipelineID, workflowID int64) {
	r.workflows = append(r.workflows, [2]int64{pipelineID, workflowID})
}

// The end of a workflow marks its status; a deployment's, the whole
// pipeline's, whose status a deployment status carries.
func TestWorkflowEndMarksStatuses(t *testing.T) {
	for _, tt := range []struct {
		event     model.WebhookEvent
		pipelines []int64
		workflows [][2]int64
	}{
		{model.EventPush, nil, [][2]int64{{7, 1}}},
		{model.EventDeploy, []int64{7}, nil},
	} {
		s := store_mocks.NewMockStore(t)
		s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{
			{ID: 1, State: model.StatusSuccess}, {ID: 2, State: model.StatusRunning},
		}, nil).Once()
		statuses := &recordedStatuses{}
		e := WorkflowEnd{Store: s, Publisher: &recordedEvents{}, Statuses: statuses}
		require.NoError(t, e.Ended(t.Context(), zerolog.Nop(), &model.Repo{},
			&model.Pipeline{ID: 7, Event: tt.event, Status: model.StatusRunning}, &model.Workflow{ID: 1, PipelineID: 7}))
		assert.Equal(t, tt.pipelines, statuses.pipelines, "event %s", tt.event)
		assert.Equal(t, tt.workflows, statuses.workflows, "event %s", tt.event)
	}
}

// The sweep waits longer than the queue holds a canceled task: one with
// the longest finalizers' time whose agent reports their end at the latest
// the queue takes, its lease extended right before that ends.
func TestCanceledWorkflowsBound(t *testing.T) {
	assert.Greater(t, CanceledWorkflowsBound, queue.MaxCanceledHold(compiler.MaxFinalizeTimeout))
}

// killedWorkflow matches a workflow written killed.
func killedWorkflow(id int64) any {
	return mock.MatchedBy(func(w *model.Workflow) bool {
		return w.ID == id && w.State == model.StatusKilled && w.Finished != 0
	})
}

// A workflow killed: its unstarted steps canceled, its running ones killed,
// its ended ones as they are.
func TestKillWorkflow(t *testing.T) {
	workflow := &model.Workflow{ID: 1, State: model.StatusRunning, Children: []*model.Step{
		{ID: 2, State: model.StatusSuccess, Started: 1, Finished: 2},
		{ID: 3, State: model.StatusRunning, Started: 3},
		{ID: 4, State: model.StatusPending},
	}}
	s := store_mocks.NewMockStore(t)
	s.EXPECT().WorkflowFinish(killedWorkflow(1)).Return(true, nil).Once()
	// each written only if still as read
	s.EXPECT().StepFinish(stepIs(3, model.StatusKilled), []model.StatusValue{model.StatusPending, model.StatusRunning}).Return(true, nil).Once()
	s.EXPECT().StepFinish(stepIs(4, model.StatusCanceled), []model.StatusValue{model.StatusPending, model.StatusRunning}).Return(true, nil).Once()

	won, err := KillWorkflow(s, workflow)
	require.NoError(t, err)
	assert.True(t, won)
	assert.Equal(t, model.StatusKilled, workflow.State)
	assert.Equal(t, model.StatusSuccess, workflow.Children[0].State)
	assert.NotZero(t, workflow.Children[1].Finished)
}

// A workflow that ended meanwhile, its Done first, is left as it is, its
// steps too.
func TestKillWorkflowLost(t *testing.T) {
	workflow := &model.Workflow{ID: 1, State: model.StatusRunning, Children: []*model.Step{
		{ID: 3, State: model.StatusRunning, Started: 3},
	}}
	s := store_mocks.NewMockStore(t)
	s.EXPECT().WorkflowFinish(killedWorkflow(1)).Return(false, nil).Once()

	won, err := KillWorkflow(s, workflow)
	require.NoError(t, err)
	assert.False(t, won)
	assert.Equal(t, model.StatusRunning, workflow.State)
	assert.Equal(t, model.StatusRunning, workflow.Children[0].State)
}
