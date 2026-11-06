// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// A task that lingers in the in-memory queue but is already gone from the
// backup store must be dropped on Poll instead of being handed to the agent,
// otherwise it loops forever (re-poll, illegal-instruction, resubmit).
func TestPersistentQueuePollDropsStaleTask(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskDelete("1").Return(types.ErrRecordNotExist).Once()

	pq := &persistentQueue{Queue: q, store: store}

	task := genDummyTask()
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))

	got, err := pq.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.Nil(t, got, "stale task must not be returned to the agent")

	info := q.Info(ctx)
	assert.Equal(t, 0, info.Stats.Pending, "stale task must be removed from pending")
	assert.Equal(t, 0, info.Stats.Running, "stale task must be removed from running")
}

func TestPersistentQueuePollDropsTaskWithMissingWorkflow(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskDelete("1").Return(nil).Once()
	store.EXPECT().WorkflowLoad(int64(1)).Return(nil, types.ErrRecordNotExist).Once()

	pq := &persistentQueue{Queue: q, store: store}

	task := genDummyTask()
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))

	got, err := pq.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.Nil(t, got, "task without a workflow must not be returned to the agent")

	info := q.Info(ctx)
	assert.Equal(t, 0, info.Stats.Pending)
	assert.Equal(t, 0, info.Stats.Running)
}

func TestPersistentQueuePollDropsTerminalWorkflowTask(t *testing.T) {
	terminalStates := []model.StatusValue{
		model.StatusSuccess,
		model.StatusFailure,
		model.StatusTimeout,
		model.StatusKilled,
		model.StatusCanceled,
		model.StatusSkipped,
		model.StatusError,
		model.StatusDeclined,
	}

	for _, state := range terminalStates {
		t.Run(string(state), func(t *testing.T) {
			ctx, cancel, q := setupTestQueue(t)
			defer cancel(nil)

			store := store_mocks.NewMockStore(t)
			store.EXPECT().TaskDelete("1").Return(nil).Once()
			store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, State: state}, nil).Once()

			pq := &persistentQueue{Queue: q, store: store}

			task := genDummyTask()
			assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))

			got, err := pq.Poll(ctx, 1, filterFnTrue)
			assert.NoError(t, err)
			assert.Nil(t, got, "terminal workflow task must not be returned to the agent")

			info := q.Info(ctx)
			assert.Equal(t, 0, info.Stats.Pending)
			assert.Equal(t, 0, info.Stats.Running)
		})
	}
}

// A task that is still present in the backup store and whose workflow is still
// active is polled normally.
func TestPersistentQueuePollReturnsLiveTask(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().GetPipeline(int64(9)).Return(&model.Pipeline{ID: 9, Status: model.StatusRunning}, nil).Once()
	store.EXPECT().TaskDelete("1").Return(nil).Once()
	store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, PipelineID: 9, State: model.StatusPending}, nil).Once()

	pq := &persistentQueue{Queue: q, store: store}

	task := genDummyTask()
	task.PipelineID = 9
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))

	got, err := pq.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, "1", got.ID)
}

// The server stopped after a cancel claimed the pipeline and before it
// evicted its tasks: the stored tasks are restored with the queue. Polled,
// a task of the canceled pipeline is dropped, not handed to an agent, and
// its dependents see it killed.
func TestPersistentQueueRestoredTaskOfCanceledPipeline(t *testing.T) {
	for _, status := range []model.StatusValue{model.StatusKilled, model.StatusCanceled} {
		t.Run(string(status), func(t *testing.T) {
			ctx, cancel, q := setupTestQueue(t)
			defer cancel(nil)

			store := store_mocks.NewMockStore(t)
			task := genDummyTask()
			task.PipelineID = 9
			dependent := &model.Task{ID: "2", Dependencies: []string{"1"}, DepStatus: make(map[string]model.StatusValue), RunOn: []string{"success", "failure"}}
			store.EXPECT().TaskList().Return([]*model.Task{task, dependent}, nil).Once()
			store.EXPECT().ServerConfigGet(mock.Anything).Return("", types.ErrRecordNotExist).Maybe()
			store.EXPECT().TaskDelete("1").Return(nil).Once()
			// the dependent's restore reads it
			store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, PipelineID: 9, State: model.StatusPending}, nil).Once()
			store.EXPECT().GetPipeline(int64(9)).Return(&model.Pipeline{
				ID: 9, Status: status, CancelInfo: &model.CancelInfo{CanceledByUser: "octocat"},
			}, nil).Once()

			pq := WithTaskStore(ctx, q, store)
			got, err := pq.Poll(ctx, 1, func(task *model.Task) (bool, int) { return task.ID == "1", 1 })
			assert.NoError(t, err)
			assert.Nil(t, got, "a task of a canceled pipeline must not run")

			info := q.Info(ctx)
			assert.Equal(t, 0, info.Stats.Running)
			found := false
			for _, waiting := range append(info.Pending, info.WaitingOnDeps...) {
				if waiting.ID == "2" {
					found = true
					assert.Equal(t, model.StatusKilled, waiting.DepStatus["1"], "its dependent sees it killed")
				}
			}
			assert.True(t, found, "its dependent waits in the queue")
		})
	}
}

// A cancel deletes the stored tasks also when the queue does not hold
// some of them: none is restored after a restart.
func TestPersistentQueueCancelDeletesStoredTasks(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskDelete("held").Return(nil).Once()
	store.EXPECT().TaskDelete("not-held").Return(nil).Once()
	pq := &persistentQueue{Queue: q, store: store}
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{{ID: "held"}}))

	_, err := pq.CancelAtOnce(ctx, []string{"held", "not-held"}, false)
	assert.ErrorIs(t, err, ErrNotFound, "the queue's error is told")
}

func TestPersistentQueueDoneRemovesPendingTaskFromBackup(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskDelete("1").Return(nil).Once()

	pq := &persistentQueue{Queue: q, store: store}

	task := genDummyTask()
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))
	assert.NoError(t, pq.Done(ctx, task.ID, model.StatusSuccess))

	info := q.Info(ctx)
	assert.Equal(t, 0, info.Stats.Pending)
	assert.Equal(t, 0, info.Stats.Running)
}

// A task whose pipeline cannot be read is not handed out: it keeps its
// stored copy and its lease, the agent polls on; once its lease expires it
// is put back and a later poll hands it out, its pipeline read then.
func TestPersistentQueuePipelineUnknownRetriedLater(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)
	q.extension = 300 * time.Millisecond

	store := store_mocks.NewMockStore(t)
	store.EXPECT().GetPipeline(int64(9)).Return(nil, errors.New("store unavailable")).Once()
	pq := &persistentQueue{Queue: q, store: store}
	task := genDummyTask()
	task.PipelineID = 9
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))

	got, err := pq.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.Nil(t, got, "not handed out while its pipeline is unknown")
	store.AssertNotCalled(t, "TaskDelete", mock.Anything)

	// the next poll does not get it again at once
	pollCtx, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	got, _ = pq.Poll(pollCtx, 1, filterFnTrue)
	stop()
	assert.Nil(t, got, "held by its lease: no busy loop")

	store.EXPECT().GetPipeline(int64(9)).Return(&model.Pipeline{ID: 9, Status: model.StatusRunning}, nil).Once()
	store.EXPECT().TaskDelete("1").Return(nil).Once()
	store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, PipelineID: 9, State: model.StatusPending}, nil).Once()
	waitCtx, stopWait := context.WithTimeout(ctx, 5*time.Second)
	defer stopWait()
	got, err = pq.Poll(waitCtx, 1, filterFnTrue)
	assert.NoError(t, err)
	if assert.NotNil(t, got, "its lease expired, it is handed out") {
		assert.Equal(t, "1", got.ID)
	}
}

func TestPersistentQueueDoneIgnoresAlreadyRemovedBackupTask(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().GetPipeline(int64(9)).Return(&model.Pipeline{ID: 9, Status: model.StatusRunning}, nil).Once()
	store.EXPECT().TaskDelete("1").Return(nil).Once()
	store.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, PipelineID: 9, State: model.StatusPending}, nil).Once()
	store.EXPECT().TaskDelete("1").Return(types.ErrRecordNotExist).Once()

	pq := &persistentQueue{Queue: q, store: store}

	task := genDummyTask()
	task.PipelineID = 9
	assert.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))

	got, err := pq.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.NoError(t, pq.Done(ctx, task.ID, model.StatusSuccess))

	info := q.Info(ctx)
	assert.Equal(t, 0, info.Stats.Pending)
	assert.Equal(t, 0, info.Stats.Running)
}

func TestPersistentQueuePausePersistsState(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().ServerConfigSet(serverConfigQueuePaused, "true").Return(nil).Once()

	pq := &persistentQueue{Queue: q, store: store}
	pq.Pause()

	assert.True(t, pq.Info(ctx).Paused)
}

func TestPersistentQueueResumePersistsState(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().ServerConfigSet(serverConfigQueuePaused, "true").Return(nil).Once()
	store.EXPECT().ServerConfigSet(serverConfigQueuePaused, "false").Return(nil).Once()

	pq := &persistentQueue{Queue: q, store: store}
	pq.Pause()
	pq.Resume()

	assert.False(t, pq.Info(ctx).Paused)
}

func TestWithTaskStoreRestoresPausedState(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskList().Return(nil, nil).Once()
	store.EXPECT().ServerConfigGet(serverConfigQueuePaused).Return("true", nil).Once()

	pq := WithTaskStore(ctx, q, store)

	assert.True(t, pq.Info(ctx).Paused)
}

func TestWithTaskStoreLeavesQueueRunningWhenNotPaused(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskList().Return(nil, nil).Once()
	store.EXPECT().ServerConfigGet(serverConfigQueuePaused).Return("false", nil).Once()

	pq := WithTaskStore(ctx, q, store)

	assert.False(t, pq.Info(ctx).Paused)
}

func TestWithTaskStoreLeavesQueueRunningWhenNoPausedConfig(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().TaskList().Return(nil, nil).Once()
	store.EXPECT().ServerConfigGet(serverConfigQueuePaused).Return("", types.ErrRecordNotExist).Once()

	pq := WithTaskStore(ctx, q, store)

	assert.False(t, pq.Info(ctx).Paused)
}

func TestWithTaskStoreRestoresPausedStateAfterRestart(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)

	store := store_mocks.NewMockStore(t)
	store.EXPECT().ServerConfigSet(serverConfigQueuePaused, "true").Return(nil).Once()

	pq := &persistentQueue{Queue: q, store: store}
	pq.Pause()
	assert.True(t, pq.Info(ctx).Paused)

	// Fresh in-memory queue after process restart; paused flag comes from store.
	ctx2, cancel2, q2 := setupTestQueue(t)
	defer cancel2(nil)

	store.EXPECT().TaskList().Return(nil, nil).Once()
	store.EXPECT().ServerConfigGet(serverConfigQueuePaused).Return("true", nil).Once()

	restored := WithTaskStore(ctx2, q2, store)
	assert.True(t, restored.Info(ctx2).Paused)
}
