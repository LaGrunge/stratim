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

package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// startServerQueue starts a queue on the store as a server does, its leases
// lasting extension, and returns it with what stops it: the server stopping.
func startServerQueue(t *testing.T, s store.Store, extension time.Duration) (context.Context, Queue, func()) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })
	memory, ok := NewMemoryQueue(ctx).(*fifo)
	require.True(t, ok)
	if extension != 0 {
		memory.extension = extension
	}
	return ctx, WithTaskStore(ctx, memory, s), func() { cancel(nil) }
}

// pollOne polls the queue for a task for at most timeout; nil if none came.
func pollOne(ctx context.Context, q Queue, agentID int64, timeout time.Duration) *model.Task {
	pollCtx, stop := context.WithTimeout(ctx, timeout)
	defer stop()
	task, _ := q.Poll(pollCtx, agentID, filterFnTrue)
	return task
}

func runningTasks(info InfoT) map[string]int64 {
	running := make(map[string]int64, len(info.Running))
	for _, task := range info.Running {
		running[task.ID] = task.AgentID
	}
	return running
}

func storedAgents(t *testing.T, s store.Store) map[string]int64 {
	t.Helper()
	tasks, err := s.TaskList()
	require.NoError(t, err)
	agents := make(map[string]int64, len(tasks))
	for _, task := range tasks {
		agents[task.ID] = task.AgentID
	}
	return agents
}

// A task handed out stays stored, with its agent, until its Done.
func TestLeaseTaskStoredUntilDone(t *testing.T) {
	s := newSQLiteStore(t)
	dep, gate := createDepAndGate(t, s, []string{string(model.StatusSuccess)})
	ctx, q, _ := startServerQueue(t, s, 0)
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{dep, gate}))

	polled := pollOne(ctx, q, 7, 5*time.Second)
	require.NotNil(t, polled)
	require.Equal(t, dep.ID, polled.ID)
	assert.Equal(t, map[string]int64{dep.ID: 7, gate.ID: 0}, storedAgents(t, s), "handed out to agent 7")

	setWorkflowState(t, s, dep.ID, model.StatusSuccess)
	require.NoError(t, q.Done(ctx, dep.ID, model.StatusSuccess))
	assert.Equal(t, map[string]int64{gate.ID: 0}, storedAgents(t, s), "removed on its Done")
}

// The server restarts while a dependency runs: the dependency is held
// running again, with its agent, and its dependent waits for it, not handed
// out until the dependency's Done reaches the new server.
func TestLeaseRestartKeepsDependentWaiting(t *testing.T) {
	s := newSQLiteStore(t)
	dep, gate := createDepAndGate(t, s, []string{string(model.StatusSuccess)})
	ctx1, q1, stop1 := startServerQueue(t, s, 0)
	require.NoError(t, q1.PushAtOnce(ctx1, []*model.Task{dep, gate}))
	require.NotNil(t, pollOne(ctx1, q1, 7, 5*time.Second))
	setWorkflowState(t, s, dep.ID, model.StatusRunning)
	stop1()

	ctx2, q2, _ := startServerQueue(t, s, 0)
	assert.Equal(t, map[string]int64{dep.ID: 7}, runningTasks(q2.Info(ctx2)), "held running, by its agent")
	assert.Nil(t, pollOne(ctx2, q2, 8, 500*time.Millisecond), "the gate waits for its running dependency")

	// its agent goes on: it extends its lease and reports it done
	require.NoError(t, q2.Extend(ctx2, 7, dep.ID))
	setWorkflowState(t, s, dep.ID, model.StatusSuccess)
	require.NoError(t, q2.Done(ctx2, dep.ID, model.StatusSuccess))

	got := pollOne(ctx2, q2, 8, 5*time.Second)
	require.NotNil(t, got, "the gate runs once its dependency is done")
	assert.Equal(t, gate.ID, got.ID)
	assert.Equal(t, model.StatusSuccess, got.DepStatus[dep.ID])
}

// After a restart a cancel reaches the agent of a task running: its Wait
// tells it canceled, and the task stays stored until its Done, held
// canceling again after another restart.
func TestLeaseCancelAfterRestart(t *testing.T) {
	s := newSQLiteStore(t)
	dep, _ := createDepAndGate(t, s, nil)
	dep.PipelineID = 1
	require.NoError(t, s.CreateRepo(&model.Repo{ForgeRemoteID: "1", Owner: "o", Name: "r", FullName: "o/r"}))
	pipeline := &model.Pipeline{RepoID: 1, Number: 1, Status: model.StatusRunning}
	require.NoError(t, s.CreatePipeline(pipeline))
	require.Equal(t, int64(1), pipeline.ID, "the workflows' pipeline")
	ctx1, q1, stop1 := startServerQueue(t, s, 0)
	require.NoError(t, q1.PushAtOnce(ctx1, []*model.Task{dep}))
	require.NotNil(t, pollOne(ctx1, q1, 7, 5*time.Second))
	setWorkflowState(t, s, dep.ID, model.StatusRunning)
	stop1()

	ctx2, q2, stop2 := startServerQueue(t, s, 0)
	waited := make(chan error, 1)
	go func() { waited <- q2.Wait(ctx2, dep.ID) }()
	canceling, err := q2.CancelAtOnce(ctx2, []string{dep.ID}, false)
	require.NoError(t, err)
	assert.Equal(t, []string{dep.ID}, canceling)
	select {
	case err := <-waited:
		assert.ErrorIs(t, err, ErrCancel, "its agent is told")
	case <-time.After(5 * time.Second):
		t.Fatal("the cancel did not reach the agent's wait")
	}
	assert.Contains(t, storedAgents(t, s), dep.ID, "stored while its finalizers run")
	stop2()

	// its pipeline ended killed: held canceling after another restart
	pipeline.Status = model.StatusKilled
	pipeline.Finished = time.Now().Unix()
	require.NoError(t, s.UpdatePipeline(pipeline))
	ctx3, q3, _ := startServerQueue(t, s, 0)
	assert.ErrorIs(t, q3.Wait(ctx3, dep.ID), ErrCancel, "still canceled")
	killed := make(chan error, 1)
	go func() { killed <- q3.WaitKilled(ctx3, dep.ID) }()
	_, err = q3.CancelAtOnce(ctx3, []string{dep.ID}, true)
	require.NoError(t, err)
	select {
	case err := <-killed:
		assert.ErrorIs(t, err, ErrCancel, "a second cancel stops its finalizers")
	case <-time.After(5 * time.Second):
		t.Fatal("the second cancel did not reach the agent")
	}
	setWorkflowState(t, s, dep.ID, model.StatusKilled)
	require.NoError(t, q3.Done(ctx3, dep.ID, model.StatusKilled))
	assert.NotContains(t, storedAgents(t, s), dep.ID, "removed on its Done")
}

// After a restart an agent that is gone does not extend the lease of its
// task: it expires, the task is queued again and handed to another agent,
// stored with it.
func TestLeaseRestoredExpiresWithoutItsAgent(t *testing.T) {
	s := newSQLiteStore(t)
	dep, _ := createDepAndGate(t, s, nil)
	dep.Data = []byte("config")
	ctx1, q1, stop1 := startServerQueue(t, s, 0)
	require.NoError(t, q1.PushAtOnce(ctx1, []*model.Task{dep}))
	require.NotNil(t, pollOne(ctx1, q1, 7, 5*time.Second))
	setWorkflowState(t, s, dep.ID, model.StatusRunning)
	stop1()

	ctx2, q2, _ := startServerQueue(t, s, 300*time.Millisecond)
	got := pollOne(ctx2, q2, 8, 5*time.Second)
	require.NotNil(t, got, "handed out again once its lease expired")
	assert.Equal(t, dep.ID, got.ID)
	assert.Equal(t, map[string]int64{dep.ID: 8}, storedAgents(t, s))
}

// A task whose workflow ended, its Done stored but not its task removed, is
// not held running again after a restart: it is removed, and its dependent
// gets the state it ended with.
func TestLeaseRestoreDropsEndedWorkflow(t *testing.T) {
	s := newSQLiteStore(t)
	dep, gate := createDepAndGate(t, s, []string{string(model.StatusSuccess)})
	dep.AgentID = 7
	require.NoError(t, s.TaskInsert(dep))
	require.NoError(t, s.TaskInsert(gate))
	setWorkflowState(t, s, dep.ID, model.StatusFailure)

	ctx, q, _ := startServerQueue(t, s, 0)
	assert.Empty(t, runningTasks(q.Info(ctx)))
	assert.Equal(t, map[string]int64{gate.ID: 0}, storedAgents(t, s))
	assert.Equal(t, model.StatusFailure, queuedTask(t, q.Info(ctx), gate.ID).DepStatus[dep.ID])
}

// A workflow running whose task is not stored, as one handed out by a
// server that did not store it, is adopted when the server starts: held
// running by its agent, its dependent waiting for it; its agent's Done
// releases the dependent.
func TestLeaseAdoptsOrphanedWorkflow(t *testing.T) {
	s := newSQLiteStore(t)
	dep, gate := createDepAndGate(t, s, []string{string(model.StatusSuccess)})
	require.NoError(t, s.TaskInsert(gate))
	setWorkflowRunningOn(t, s, dep.ID, 7)

	ctx, q, _ := startServerQueue(t, s, 0)
	assert.Equal(t, map[string]int64{dep.ID: 7}, runningTasks(q.Info(ctx)), "adopted, by its agent")
	assert.Equal(t, map[string]int64{dep.ID: 7, gate.ID: 0}, storedAgents(t, s), "stored")
	assert.Nil(t, pollOne(ctx, q, 8, 500*time.Millisecond), "the gate waits for it")

	require.NoError(t, q.Extend(ctx, 7, dep.ID), "its agent extends it")
	setWorkflowState(t, s, dep.ID, model.StatusSuccess)
	require.NoError(t, q.Done(ctx, dep.ID, model.StatusSuccess))
	got := pollOne(ctx, q, 8, 5*time.Second)
	require.NotNil(t, got)
	assert.Equal(t, gate.ID, got.ID)
}

// An adopted task whose agent is gone is not run again, it has no config:
// its lease expired, it is settled, and held until then, its dependent
// getting the state it was settled with.
func TestLeaseAdoptedExpiresSettled(t *testing.T) {
	s := newSQLiteStore(t)
	dep, gate := createDepAndGate(t, s, []string{string(model.StatusSuccess)})
	require.NoError(t, s.TaskInsert(gate))
	setWorkflowRunningOn(t, s, dep.ID, 7)

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	memory, ok := NewMemoryQueue(ctx).(*fifo)
	require.True(t, ok)
	memory.extension = 300 * time.Millisecond
	q := WithTaskStore(ctx, memory, s)
	var mu sync.Mutex
	var settled []string
	q.SetCanceledExpiredHandler(func(task *model.Task) {
		mu.Lock()
		defer mu.Unlock()
		settled = append(settled, task.ID)
		// as rpc.RPC.settleCanceledWorkflow does: killed
		setWorkflowState(t, s, task.ID, model.StatusKilled)
	})

	require.Eventually(t, func() bool {
		_, held := runningTasks(q.Info(ctx))[dep.ID]
		return !held
	}, 5*time.Second, 50*time.Millisecond, "released once settled")
	mu.Lock()
	assert.Equal(t, []string{dep.ID}, settled, "settled once")
	mu.Unlock()
	assert.Equal(t, map[string]int64{gate.ID: 0}, storedAgents(t, s), "removed from the store")
	assert.Equal(t, model.StatusKilled, queuedTask(t, q.Info(ctx), gate.ID).DepStatus[dep.ID])
}

// Orphans are looked for while the server runs too: a workflow running
// whose task the store lost is adopted.
func TestLeaseWatchAdoptsOrphans(t *testing.T) {
	s := newSQLiteStore(t)
	dep, _ := createDepAndGate(t, s, nil)
	_, q, _ := startServerQueue(t, s, 0)
	pq, ok := q.(*persistentQueue)
	require.True(t, ok)
	setWorkflowRunningOn(t, s, dep.ID, 7)

	pq.adoptOrphans(t.Context(), make(map[string]struct{}))
	assert.Equal(t, map[string]int64{dep.ID: 7}, runningTasks(q.Info(t.Context())))
	pq.adoptOrphans(t.Context(), map[string]struct{}{dep.ID: {}})
	assert.Len(t, q.Info(t.Context()).Running, 1, "held once")
}

func setWorkflowRunningOn(t *testing.T, s store.Store, id string, agentID int64) {
	t.Helper()
	var workflowID int64
	_, err := fmt.Sscan(id, &workflowID)
	require.NoError(t, err)
	workflow, err := s.WorkflowLoad(workflowID)
	require.NoError(t, err)
	workflow.State = model.StatusRunning
	workflow.AgentID = agentID
	require.NoError(t, s.WorkflowUpdate(workflow))
}

// A workflow running with no agent recorded is adopted for none: no agent
// extends it, it is settled; stored, it is held running again after a
// restart, not queued without its config.
func TestLeaseAdoptsOrphanWithoutAgent(t *testing.T) {
	s := newSQLiteStore(t)
	dep, _ := createDepAndGate(t, s, nil)
	setWorkflowRunningOn(t, s, dep.ID, 0)

	ctx1, q1, stop1 := startServerQueue(t, s, 0)
	assert.Equal(t, map[string]int64{dep.ID: model.IDNotSet}, runningTasks(q1.Info(ctx1)))
	assert.Equal(t, map[string]int64{dep.ID: model.IDNotSet}, storedAgents(t, s))
	assert.ErrorIs(t, q1.Extend(ctx1, 0, dep.ID), ErrAgentMissMatch, "no agent extends it")
	stop1()

	ctx2, q2, _ := startServerQueue(t, s, 0)
	assert.Equal(t, map[string]int64{dep.ID: model.IDNotSet}, runningTasks(q2.Info(ctx2)), "held again, not queued")
	assert.Nil(t, pollOne(ctx2, q2, 8, 500*time.Millisecond), "never handed out")
}

// The workflows running are read page by page: orphans past the first page
// are adopted too, the ones stored taking no room.
func TestLeaseAdoptsOrphansPastAPage(t *testing.T) {
	s := newSQLiteStore(t)
	before := orphansPage
	orphansPage = 2
	t.Cleanup(func() { orphansPage = before })
	workflows := []*model.Workflow{
		{PipelineID: 1, PID: 1, Name: "first", State: model.StatusPending},
		{PipelineID: 1, PID: 2, Name: "second", State: model.StatusPending},
		{PipelineID: 1, PID: 3, Name: "orphan", State: model.StatusPending},
	}
	require.NoError(t, s.WorkflowsCreate(workflows))
	first := &model.Task{ID: fmt.Sprint(workflows[0].ID)}
	second := &model.Task{ID: fmt.Sprint(workflows[1].ID)}
	orphan := &model.Task{ID: fmt.Sprint(workflows[2].ID)}
	for _, task := range []*model.Task{first, second} {
		task.AgentID = 7
		task.Data = []byte("config")
		require.NoError(t, s.TaskInsert(task))
		setWorkflowRunningOn(t, s, task.ID, 7)
	}
	setWorkflowRunningOn(t, s, orphan.ID, 9)

	ctx, q, _ := startServerQueue(t, s, 0)
	assert.Equal(t, map[string]int64{first.ID: 7, second.ID: 7, orphan.ID: 9}, runningTasks(q.Info(ctx)))
}

// A task whose agent cannot be stored is not handed out, and is put back at
// once, not after its lease: the next poll gets it.
func TestLeasePollPutsBackUnstoredTask(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)
	mockStore := store_mocks.NewMockStore(t)
	mockStore.EXPECT().WorkflowLoad(int64(1)).Return(&model.Workflow{ID: 1, State: model.StatusPending}, nil).Twice()
	mockStore.EXPECT().TaskSetAgent("1", int64(1)).Return(errors.New("database is down")).Once()
	mockStore.EXPECT().TaskSetAgent("1", int64(1)).Return(nil).Once()
	pq := &persistentQueue{Queue: q, store: mockStore}
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{genDummyTask()}))

	got, err := pq.Poll(ctx, 1, filterFnTrue)
	assert.NoError(t, err)
	assert.Nil(t, got, "not handed out with its agent not stored")

	got = pollOne(ctx, pq, 1, 2*time.Second) // the lease is a minute
	require.NotNil(t, got, "put back at once")
	assert.Equal(t, "1", got.ID)
}

// An orphan whose agent's Done came before it was held, finding no task,
// is released at once: removed from the store, not held running.
func TestLeaseAdoptReleasesOrphanDoneMeanwhile(t *testing.T) {
	ctx, cancel, q := setupTestQueue(t)
	defer cancel(nil)
	mockStore := store_mocks.NewMockStore(t)
	mockStore.EXPECT().WorkflowsRunning(int64(0), mock.Anything).Return([]*model.Workflow{{ID: 5, PipelineID: 9, AgentID: 7, State: model.StatusRunning}}, nil).Once()
	mockStore.EXPECT().GetPipeline(int64(9)).Return(&model.Pipeline{ID: 9, RepoID: 1, Status: model.StatusRunning}, nil)
	mockStore.EXPECT().TaskInsert(mock.Anything).Return(nil).Once()
	mockStore.EXPECT().WorkflowLoad(int64(5)).Return(&model.Workflow{ID: 5, State: model.StatusSuccess}, nil).Once()
	mockStore.EXPECT().TaskDelete("5").Return(nil).Once()
	pq := &persistentQueue{Queue: q, store: mockStore}

	pq.adoptOrphans(t.Context(), make(map[string]struct{}))
	assert.Empty(t, q.Info(ctx).Running)
}
