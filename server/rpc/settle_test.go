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

package rpc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server/logging"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	queue_mocks "go.woodpecker-ci.org/woodpecker/v3/server/queue/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/shared/constant"
)

// killedPipeline is the pipeline a cancel killed while its workflow runs
// its finalizers.
func killedPipeline() *model.Pipeline {
	pipeline := defaultPipeline(model.StatusKilled)
	pipeline.Finished = 150
	pipeline.CancelInfo = &model.CancelInfo{CanceledByUser: "octocat"}
	return pipeline
}

// A canceled workflow whose agent never reports it done (gone while its
// finalizers ran) is settled once its lease expired: its unstarted steps
// canceled, its started ones killed, it killed. Its pipeline, killed by the
// cancel, is not written again.
func TestCanceledWorkflowSettledAfterLease(t *testing.T) {
	saved := constant.TaskTimeout
	constant.TaskTimeout = 100 * time.Millisecond
	q := queue.NewMemoryQueue(t.Context())
	constant.TaskTimeout = saved

	stopped := &model.Step{ID: 41, State: model.StatusKilled, Started: 100, Finished: 110}
	finalizer := &model.Step{ID: 42, State: model.StatusRunning, Started: 120, Finalizer: true}
	unstarted := &model.Step{ID: 43, State: model.StatusPending, Finalizer: true}
	workflow := defaultWorkflow(model.StatusRunning)

	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
	mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{stopped, finalizer, unstarted}, nil)
	settled := make(chan *model.Workflow, 1)
	mockStore.On("WorkflowFinish", mock.Anything).Run(func(args mock.Arguments) {
		w, _ := args.Get(0).(*model.Workflow)
		settled <- w
	}).Return(true, nil).Once()
	mockStore.On("StepFinish", mock.Anything, mock.Anything).Return(true, nil)
	mockStore.EXPECT().GetPipeline(int64(20)).RunAndReturn(func(int64) (*model.Pipeline, error) { return killedPipeline(), nil })
	mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
	mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{{ID: 30, State: model.StatusKilled}}, nil)
	// a killed pipeline is not ended again
	pipelineChecked := make(chan struct{})
	mockStore.On("PipelineFinish", mock.Anything).Run(func(mock.Arguments) { close(pipelineChecked) }).Return(false, nil).Once()

	rpcInst := newTestRPC(t, mockStore, q)
	rpcInst.scheduler.SetCanceledExpiredHandler(rpcInst.settleCanceledWorkflow)

	require.NoError(t, q.PushAtOnce(t.Context(), []*model.Task{{ID: "30", PipelineID: 20}}))
	task, err := q.Poll(t.Context(), 1, func(*model.Task) (bool, int) { return true, 1 })
	require.NoError(t, err)
	canceling, err := rpcInst.scheduler.CancelWorkflows(t.Context(), []string{task.ID}, false)
	require.NoError(t, err)
	assert.Equal(t, []string{task.ID}, canceling)
	assert.Len(t, q.Info(t.Context()).Running, 1, "the agent still holds it while its finalizers run")

	select {
	case w := <-settled:
		assert.Equal(t, model.StatusKilled, w.State)
		assert.NotZero(t, w.Finished)
	case <-time.After(10 * time.Second):
		t.Fatal("the canceled workflow was not settled after its lease")
	}
	select {
	case <-pipelineChecked:
	case <-time.After(5 * time.Second):
		t.Fatal("the settle did not get to its pipeline")
	}
	assert.Equal(t, model.StatusKilled, stopped.State, "an ended step stays")
	assert.Equal(t, model.StatusKilled, finalizer.State)
	assert.NotZero(t, finalizer.Finished)
	assert.Equal(t, model.StatusCanceled, unstarted.State)
	mockStore.AssertNotCalled(t, "UpdatePipeline", mock.Anything)
	assert.Empty(t, q.Info(t.Context()).Running)
	assert.Empty(t, q.Info(t.Context()).Pending, "not run again")
}

// The server as built for production settles a canceled workflow whose
// lease expired: NewWoodpeckerServer wires the settle into the queue.
func TestServerSettlesCanceledWorkflow(t *testing.T) {
	saved := constant.TaskTimeout
	constant.TaskTimeout = 100 * time.Millisecond
	q := queue.NewMemoryQueue(t.Context())
	constant.TaskTimeout = saved

	workflow := defaultWorkflow(model.StatusRunning)
	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
	mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
	settled := make(chan *model.Workflow, 1)
	mockStore.On("WorkflowFinish", mock.Anything).Run(func(args mock.Arguments) {
		w, _ := args.Get(0).(*model.Workflow)
		settled <- w
	}).Return(true, nil).Once()
	mockStore.EXPECT().GetPipeline(int64(20)).RunAndReturn(func(int64) (*model.Pipeline, error) { return killedPipeline(), nil })
	mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
	mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{{ID: 30, State: model.StatusKilled}}, nil)
	pipelineChecked := make(chan struct{})
	mockStore.On("PipelineFinish", mock.Anything).Run(func(mock.Arguments) { close(pipelineChecked) }).Return(false, nil).Once()

	sched := scheduler.NewScheduler(t.Context(), mockStore, q, memory.New())
	logs := logging.New()
	NewWoodpeckerServer(sched, logs, mockStore, testWorkflowEnd(mockStore, sched, logs))

	require.NoError(t, q.PushAtOnce(t.Context(), []*model.Task{{ID: "30", PipelineID: 20}}))
	task, err := q.Poll(t.Context(), 1, func(*model.Task) (bool, int) { return true, 1 })
	require.NoError(t, err)
	_, err = sched.CancelWorkflows(t.Context(), []string{task.ID}, false)
	require.NoError(t, err)

	select {
	case w := <-settled:
		assert.Equal(t, model.StatusKilled, w.State)
	case <-time.After(10 * time.Second):
		t.Fatal("the production server did not settle the canceled workflow")
	}
	select {
	case <-pipelineChecked:
	case <-time.After(5 * time.Second):
		t.Fatal("the settle did not get to its pipeline")
	}
}

// Settling a workflow while another of its pipeline still runs leaves the
// pipeline alone.
func TestSettleWithAnotherWorkflowRunning(t *testing.T) {
	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
	mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
	mockStore.On("WorkflowFinish", mock.Anything).Return(true, nil).Once()
	mockStore.EXPECT().GetPipeline(int64(20)).RunAndReturn(func(int64) (*model.Pipeline, error) { return killedPipeline(), nil })
	mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
	mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{
		{ID: 30, State: model.StatusKilled},
		{ID: 31, State: model.StatusRunning},
	}, nil)

	rpcInst := newTestRPC(t, mockStore, nil)
	rpcInst.settleCanceledWorkflow(&model.Task{ID: "30", PipelineID: 20})
	mockStore.AssertNotCalled(t, "PipelineFinish", mock.Anything)
	mockStore.AssertNotCalled(t, "UpdatePipeline", mock.Anything)
}

// Of a workflow's Done and its settle at once, only one ends it: the loser
// writes nothing else.
func TestSettleAndDoneRace(t *testing.T) {
	t.Run("settle after Done", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
		mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{{ID: 41, State: model.StatusRunning, Started: 1}}, nil)
		mockStore.On("WorkflowFinish", mock.Anything).Return(false, nil).Once()

		rpcInst := newTestRPC(t, mockStore, nil)
		rpcInst.settleCanceledWorkflow(&model.Task{ID: "30", PipelineID: 20})
		mockStore.AssertNotCalled(t, "StepFinish", mock.Anything, mock.Anything)
		mockStore.AssertNotCalled(t, "GetPipeline", mock.Anything)
	})

	t.Run("Done after settle", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockQueue := queue_mocks.NewMockQueue(t)
		// running when Done reads it, the settle's end when read again
		settled := false
		mockStore.EXPECT().WorkflowLoad(int64(30)).RunAndReturn(func(int64) (*model.Workflow, error) {
			if settled {
				return defaultWorkflow(model.StatusKilled), nil
			}
			return defaultWorkflow(model.StatusRunning), nil
		})
		mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{{ID: 41, State: model.StatusRunning, Started: 1}}, nil)
		mockStore.EXPECT().GetPipeline(int64(20)).RunAndReturn(func(int64) (*model.Pipeline, error) { return killedPipeline(), nil })
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
		mockStore.On("WorkflowFinish", mock.Anything).Run(func(mock.Arguments) { settled = true }).Return(false, nil).Once()
		// the agent does not hold it any more
		mockQueue.On("Done", mock.Anything, "30", model.StatusKilled).Return(queue.ErrNotFound).Once()

		rpcInst := newTestRPC(t, mockStore, mockQueue)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))
		require.NoError(t, rpcInst.Done(ctx, "30", rpc.WorkflowState{Started: 100, Finished: 200, Canceled: true}))
		mockStore.AssertNotCalled(t, "StepFinish", mock.Anything, mock.Anything)
		mockStore.AssertNotCalled(t, "WorkflowGetTree", mock.Anything)
	})

	t.Run("Done of a workflow swept", func(t *testing.T) {
		mockStore := store_mocks.NewMockStore(t)
		mockQueue := queue_mocks.NewMockQueue(t)
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusKilled), nil)
		mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
		mockStore.EXPECT().GetPipeline(int64(20)).RunAndReturn(func(int64) (*model.Pipeline, error) { return killedPipeline(), nil })
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
		// its agent's task, still in the queue, is dropped
		mockQueue.On("Done", mock.Anything, "30", model.StatusKilled).Return(nil).Once()

		rpcInst := newTestRPC(t, mockStore, mockQueue)
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))
		err := rpcInst.Done(ctx, "30", rpc.WorkflowState{Started: 100, Finished: 200, Canceled: true})
		assert.ErrorIs(t, err, ErrAgentIllegalWorkflowReRunStateChange)
		mockStore.AssertNotCalled(t, "WorkflowFinish", mock.Anything)
	})
}

// A Done whose end cannot be written (a lock wait timeout, its retries
// exhausted) is refused: its agent tries again, which the compare-and-set
// makes safe. Nothing else is written, the queue keeps it.
func TestDoneEndNotWritten(t *testing.T) {
	mockStore := store_mocks.NewMockStore(t)
	mockQueue := queue_mocks.NewMockQueue(t)
	mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
	mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{{ID: 41, State: model.StatusRunning, Started: 1}}, nil)
	mockStore.On("GetPipeline", int64(20)).Return(defaultPipeline(model.StatusRunning), nil)
	mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
	mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
	lockWait := errors.New("Error 1205: Lock wait timeout exceeded")
	mockStore.On("WorkflowFinish", mock.Anything).Return(false, lockWait).Once()

	rpcInst := newTestRPC(t, mockStore, mockQueue)
	ctx := context.WithValue(t.Context(), agentIDKey, int64(1))
	err := rpcInst.Done(ctx, "30", rpc.WorkflowState{Started: 100, Finished: 200})
	assert.ErrorIs(t, err, lockWait)
	mockStore.AssertNotCalled(t, "StepFinish", mock.Anything, mock.Anything)
	mockStore.AssertNotCalled(t, "WorkflowGetTree", mock.Anything)
	mockQueue.AssertNotCalled(t, "Done", mock.Anything, mock.Anything, mock.Anything)
}

// A canceled workflow's Done after its finalizers succeeded is recorded,
// its pipeline stays killed and is not counted as ended now.
func TestDoneAfterCancelKeepsPipelineKilled(t *testing.T) {
	mockStore := store_mocks.NewMockStore(t)
	mockQueue := queue_mocks.NewMockQueue(t)
	workflow := defaultWorkflow(model.StatusRunning)
	mockStore.On("WorkflowLoad", int64(30)).Return(workflow, nil)
	mockStore.On("StepListFromWorkflowFind", mock.Anything).Return([]*model.Step{}, nil)
	mockStore.EXPECT().GetPipeline(int64(20)).RunAndReturn(func(int64) (*model.Pipeline, error) { return killedPipeline(), nil })
	mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
	mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
	mockStore.On("AgentUpdate", mock.Anything).Return(nil)
	mockStore.On("WorkflowFinish", mock.MatchedBy(func(w *model.Workflow) bool {
		return w.State == model.StatusKilled
	})).Return(true, nil).Once()
	mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{{ID: 30, State: model.StatusKilled}}, nil)
	mockStore.On("PipelineFinish", mock.Anything).Return(false, nil).Once()
	mockQueue.On("Done", mock.Anything, "30", model.StatusKilled).Return(nil).Once()

	rpcInst := newTestRPC(t, mockStore, mockQueue)
	ctx := context.WithValue(t.Context(), agentIDKey, int64(1))
	require.NoError(t, rpcInst.Done(ctx, "30", rpc.WorkflowState{Started: 100, Finished: 200, Canceled: true}))
	mockStore.AssertNotCalled(t, "UpdatePipeline", mock.Anything)
}

// After a server restart the queue has no task of a workflow running its
// finalizers: a user's second cancel kills it in the store only. Its
// agent's WaitKilled finds it so, and stops the finalizers.
func TestWaitKilledWorkflowEndedInStore(t *testing.T) {
	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
	mockStore.On("GetPipeline", int64(20)).Return(killedPipeline(), nil)
	mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
	var mu sync.Mutex
	state := model.StatusRunning
	mockStore.On("WorkflowLoad", int64(30)).Return(func(int64) (*model.Workflow, error) {
		mu.Lock()
		defer mu.Unlock()
		return defaultWorkflow(state), nil
	})
	q := queue.NewMemoryQueue(t.Context())
	rpcInst := newTestRPC(t, mockStore, q)
	rpcInst.killedCheckInterval = 20 * time.Millisecond
	ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

	killed := make(chan bool, 1)
	go func() {
		k, err := rpcInst.WaitKilled(ctx, "30")
		assert.NoError(t, err)
		killed <- k
	}()
	select {
	case <-killed:
		t.Fatal("WaitKilled returned while the workflow runs")
	case <-time.After(200 * time.Millisecond):
	}
	mu.Lock()
	state = model.StatusKilled
	mu.Unlock()
	select {
	case k := <-killed:
		assert.True(t, k, "its finalizers stop")
	case <-time.After(5 * time.Second):
		t.Fatal("WaitKilled did not see the workflow killed")
	}
}

// recordedStatuses records what was marked for the forge.
type recordedStatuses struct {
	mu        sync.Mutex
	pipelines []int64
	workflows []int64
}

func (r *recordedStatuses) Pipeline(pipelineID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pipelines = append(r.pipelines, pipelineID)
}

func (r *recordedStatuses) Workflow(_, workflowID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workflows = append(r.workflows, workflowID)
}

// Init marks the forge's status through the server's end of workflows, as
// Done does: a deployment's whole pipeline, another event's workflow.
func TestInitMarksStatusesThroughTheEnd(t *testing.T) {
	for _, event := range []model.WebhookEvent{model.EventPush, model.EventDeploy} {
		mockStore := store_mocks.NewMockStore(t)
		pipeline := defaultPipeline(model.StatusRunning)
		pipeline.Event = event
		mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusPending), nil)
		mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
		mockStore.On("GetPipeline", int64(20)).Return(pipeline, nil)
		mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
		mockStore.On("WorkflowStart", mock.Anything).Return(true, nil).Once()
		mockStore.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{defaultWorkflow(model.StatusRunning)}, nil)
		mockStore.On("AgentUpdate", mock.Anything).Return(nil)
		statuses := &recordedStatuses{}
		rpcInst := newTestRPC(t, mockStore, nil)
		rpcInst.end.Statuses = statuses
		ctx := context.WithValue(t.Context(), agentIDKey, int64(1))

		require.NoError(t, rpcInst.Init(ctx, "30", rpc.WorkflowState{Started: 100}))
		if event == model.EventDeploy {
			assert.Equal(t, []int64{20}, statuses.pipelines)
			assert.Empty(t, statuses.workflows)
		} else {
			assert.Equal(t, []int64{30}, statuses.workflows)
			assert.Empty(t, statuses.pipelines)
		}
	}
}
