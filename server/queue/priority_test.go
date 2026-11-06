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
	"container/list"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

func infoIDs(tasks []*model.Task) []string {
	var ids []string
	for _, task := range tasks {
		ids = append(ids, task.ID)
	}
	return ids
}

func listIDs(tasks *list.List) []string {
	var ids []string
	for element := tasks.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		ids = append(ids, task.ID)
	}
	return ids
}

// pollOrder polls the queue with one worker that takes any task, n times,
// and returns the tasks in the order it got them, each reported done.
func pollOrder(ctx context.Context, t *testing.T, q Queue, n int) []string {
	t.Helper()
	var order []string
	for range n {
		task, err := q.Poll(ctx, 1, filterFnTrue)
		require.NoError(t, err)
		order = append(order, task.ID)
		require.NoError(t, q.Done(ctx, task.ID, model.StatusSuccess))
	}
	return order
}

// task is a pending task of a repository, queued at queued.
func task(id string, repo, queued int64, priority int) *model.Task {
	return &model.Task{ID: id, RepoID: repo, Queued: queued, Priority: priority}
}

// The queue's order: moved tasks first, the latest moved first; then in the
// order queued, the tasks of one repository reordered among their own places
// by priority.
func TestOrderPending(t *testing.T) {
	moved := func(tt *model.Task, boost int64) *model.Task {
		tt.Boost = boost
		return tt
	}
	tests := []struct {
		name  string
		tasks []*model.Task
		want  []string
	}{
		{
			name:  "no priorities: in the order queued",
			tasks: []*model.Task{task("c", 1, 3, 0), task("a", 2, 1, 0), task("b", 1, 2, 0)},
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "a higher priority first within a repository, ties in the order queued",
			tasks: []*model.Task{task("a", 1, 1, 0), task("b", 1, 2, 5), task("c", 1, 3, 0), task("d", 1, 4, 5), task("e", 1, 5, -3)},
			want:  []string{"b", "d", "a", "c", "e"},
		},
		{
			name: "a priority does not overtake another repository",
			tasks: []*model.Task{
				task("r1-a", 1, 1, 0), task("r2-x", 2, 2, 0), task("r1-b", 1, 3, 5), task("r2-y", 2, 4, 9), task("r3-z", 3, 5, 10),
			},
			// repository 1 keeps places 1 and 3, repository 2 places 2 and 4
			want: []string{"r1-b", "r2-y", "r1-a", "r2-x", "r3-z"},
		},
		{
			name:  "a lower priority lets the repository's others go first, not other repositories'",
			tasks: []*model.Task{task("r1-low", 1, 1, -1), task("r2-a", 2, 2, 0), task("r1-a", 1, 3, 0)},
			want:  []string{"r1-a", "r2-a", "r1-low"},
		},
		{
			name: "a move goes before all, the latest first, then priority within it",
			tasks: []*model.Task{
				task("high", 1, 1, 10),
				moved(task("first-a", 2, 2, 0), 100), moved(task("first-b", 2, 3, 2), 100),
				moved(task("second", 3, 4, -10), 200),
				task("a", 1, 5, 0),
			},
			want: []string{"second", "first-b", "first-a", "high", "a"},
		},
		{
			name: "tasks stored before they were stamped: by creation, pipeline, workflow id",
			tasks: []*model.Task{
				{ID: "10", Created: 2, PipelineID: 2},
				{ID: "9", Created: 2, PipelineID: 2},
				{ID: "3", Created: 1, PipelineID: 1},
				{ID: "late", Queued: 1},
			},
			want: []string{"3", "9", "10", "late"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 5 {
				tasks := slices.Clone(tt.tasks)
				rand.Shuffle(len(tasks), func(i, j int) { tasks[i], tasks[j] = tasks[j], tasks[i] })
				orderPending(tasks)
				assert.Equal(t, tt.want, infoIDs(tasks), "whatever order they come in")
				assert.False(t, orderPending(tasks), "ordering again changes nothing")
				assert.Equal(t, tt.want, infoIDs(tasks))
			}
		})
	}
}

// sortPending puts the pending list in the queue's order.
func TestFifoSortPending(t *testing.T) {
	q := &fifo{pending: list.New()}
	for _, tt := range []*model.Task{task("a", 1, 1, 0), task("x", 2, 2, 0), task("b", 1, 3, 5)} {
		q.pending.PushBack(tt)
	}
	q.sortPending()
	assert.Equal(t, []string{"a", "x", "b"}, listIDs(q.pending), "not dirty: left as it is")

	q.dirty = true
	q.sortPending()
	assert.Equal(t, []string{"b", "x", "a"}, listIDs(q.pending))
	assert.False(t, q.dirty)

	// a task leaving leaves the others in order
	q.pending.Remove(q.pending.Front())
	q.dirty = true
	q.sortPending()
	assert.Equal(t, []string{"x", "a"}, listIDs(q.pending))
}

// What makes the pending tasks dirty, and what does not.
func TestFifoDirty(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	q.Pause()
	clean := func() {
		q.Lock()
		q.sortPending()
		q.Unlock()
	}
	isDirty := func() bool {
		q.Lock()
		defer q.Unlock()
		return q.dirty
	}

	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{{ID: "a"}, {ID: "b", Dependencies: []string{"a"}, DepStatus: make(map[string]model.StatusValue)}}))
	assert.True(t, isDirty(), "pushed")
	clean()
	q.Lock()
	q.filterWaiting()
	q.Unlock()
	assert.False(t, isDirty(), "b still waits: nothing released")

	_, err := q.PrioritizeAtOnce(ctx, []string{"a"}, NextBoost())
	require.NoError(t, err)
	assert.True(t, isDirty(), "moved")
	clean()

	require.NoError(t, q.Done(ctx, "a", model.StatusSuccess))
	assert.True(t, isDirty(), "done")
	clean()
	q.Lock()
	q.filterWaiting()
	q.Unlock()
	assert.True(t, isDirty(), "b released")
}

// One worker gets the tasks of a repository by their priority; another
// repository's tasks keep their places.
func TestFifoPriorityPollOrder(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{
		{ID: "a", RepoID: 1},
		{ID: "other", RepoID: 2},
		{ID: "b", RepoID: 1, Priority: 5},
		{ID: "c", RepoID: 1},
		{ID: "d", RepoID: 1, Priority: 5},
		{ID: "e", RepoID: 1, Priority: -1},
	}))
	assert.Equal(t, []string{"b", "other", "d", "a", "c", "e"}, pollOrder(ctx, t, q, 6))
}

// The queue stamps the tasks it is pushed in the order pushed, and keeps a
// stamp a task has.
func TestFifoStampsQueued(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	tasks := []*model.Task{{ID: "a"}, {ID: "b"}, {ID: "kept", Queued: 7}}
	require.NoError(t, q.PushAtOnce(ctx, tasks))
	assert.NotZero(t, tasks[0].Queued)
	assert.Less(t, tasks[0].Queued, tasks[1].Queued)
	assert.Equal(t, int64(7), tasks[2].Queued)
	assert.Less(t, NextBoost(), NextBoost(), "stamps increase")
}

// A task of a higher priority that no idle agent may take does not hold up
// a lower one an agent may take: priority orders, labels still match.
func TestFifoPriorityKeepsLabelMatching(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{
		{ID: "arm", RepoID: 1, Priority: 10, Labels: map[string]string{"platform": "linux/arm64"}},
		{ID: "amd", RepoID: 1},
	}))
	amdOnly := func(task *model.Task) (bool, int) { return task.Labels["platform"] == "", 1 }
	got, err := q.Poll(ctx, 1, amdOnly)
	require.NoError(t, err)
	assert.Equal(t, "amd", got.ID)

	got, err = q.Poll(ctx, 2, filterFnTrue)
	require.NoError(t, err)
	assert.Equal(t, "arm", got.ID)
}

// A task waits for its dependencies whatever its priority; once they are
// done it goes before the lower ones of its repository.
func TestFifoPriorityWaitsOnDeps(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{
		{ID: "build", RepoID: 1},
		{ID: "deploy", RepoID: 1, Priority: 5, Dependencies: []string{"build"}, DepStatus: make(map[string]model.StatusValue)},
		{ID: "other", RepoID: 1},
	}))
	first, err := q.Poll(ctx, 1, filterFnTrue)
	require.NoError(t, err)
	assert.Equal(t, "build", first.ID, "deploy waits on build")

	waitForProcess()
	info := q.Info(ctx)
	assert.Equal(t, []string{"deploy"}, infoIDs(info.WaitingOnDeps))
	assert.Equal(t, []string{"other"}, infoIDs(info.Pending))

	require.NoError(t, q.Done(ctx, "build", model.StatusSuccess))
	assert.Equal(t, []string{"deploy", "other"}, pollOrder(ctx, t, q, 2))
}

// Moving tasks to the top: they go before all pending, whatever their
// repository and priority; the latest moved first. A task not pending is not
// moved, and a task keeps a higher boost it has.
func TestFifoPrioritizeAtOnce(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{
		{ID: "a", RepoID: 1},
		{ID: "b", RepoID: 2},
		{ID: "c1", RepoID: 3, PipelineID: 3},
		{ID: "c2", RepoID: 3, PipelineID: 3, Priority: -5},
		{ID: "d", RepoID: 1, Priority: 10},
	}))

	moved, err := q.PrioritizeAtOnce(ctx, []string{"c1", "c2", "unknown"}, 10)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"c1", "c2"}, moved)

	moved, err = q.PrioritizeAtOnce(ctx, []string{"b"}, 20)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, moved)

	moved, err = q.PrioritizeAtOnce(ctx, []string{"b"}, 15)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, moved, "pending, keeps its higher boost")
	assert.Equal(t, int64(20), queuedTask(t, q.Info(ctx), "b").Boost)

	moved, err = q.PrioritizeAtOnce(ctx, []string{"unknown"}, 30)
	require.NoError(t, err)
	assert.Empty(t, moved)

	assert.Equal(t, []string{"b", "c1", "c2", "d", "a"}, pollOrder(ctx, t, q, 5))
}

// A running task is not moved; one moved that waits on its dependencies
// keeps waiting, then goes first.
func TestFifoPrioritizeRunningAndWaiting(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{
		{ID: "build", RepoID: 1, PipelineID: 1},
		{ID: "other", RepoID: 2, PipelineID: 2},
		{ID: "deploy", RepoID: 1, PipelineID: 1, Dependencies: []string{"build"}, DepStatus: make(map[string]model.StatusValue)},
	}))
	running, err := q.Poll(ctx, 1, filterFnTrue)
	require.NoError(t, err)
	require.Equal(t, "build", running.ID)
	waitForProcess()

	moved, err := q.PrioritizeAtOnce(ctx, []string{"build", "deploy"}, NextBoost())
	require.NoError(t, err)
	assert.Equal(t, []string{"deploy"}, moved, "the running one is not moved")
	assert.NotZero(t, running.Boost, "it gets the boost, as in the store, for a resubmit")

	waitForProcess()
	assert.Equal(t, []string{"deploy"}, infoIDs(q.Info(ctx).WaitingOnDeps), "still waiting on build")

	require.NoError(t, q.Done(ctx, "build", model.StatusSuccess))
	assert.Equal(t, []string{"deploy", "other"}, pollOrder(ctx, t, q, 2))
}

// A running task of a moved pipeline whose lease expires comes back with the
// pipeline's boost: its pipeline's tasks keep one boost.
func TestFifoPrioritizeRunningExpires(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	q.extension = 50 * time.Millisecond
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{
		{ID: "build", RepoID: 1, PipelineID: 1},
		{ID: "test", RepoID: 1, PipelineID: 1},
		{ID: "other", RepoID: 2, PipelineID: 2},
	}))
	running, err := q.Poll(ctx, 1, filterFnTrue)
	require.NoError(t, err)
	require.Equal(t, "build", running.ID)

	boost := NextBoost()
	moved, err := q.PrioritizeAtOnce(ctx, []string{"build", "test"}, boost)
	require.NoError(t, err)
	assert.Equal(t, []string{"test"}, moved)

	// its agent does not extend the lease: it comes back
	time.Sleep(100 * time.Millisecond)
	waitForProcess()
	resubmitted := queuedTask(t, q.Info(ctx), "build")
	assert.Equal(t, boost, resubmitted.Boost)
	assert.Equal(t, boost, queuedTask(t, q.Info(ctx), "test").Boost)
}

// Within a concurrency group the slot goes to a moved pipeline first, then
// by creation; not by priority.
func TestFifoConcurrencyOrderMoved(t *testing.T) {
	member := func(id string, pipeline int64, priority int, boost int64) *model.Task {
		return &model.Task{
			ID: id, Name: "deploy", PipelineID: pipeline, Created: pipeline,
			ConcurrencyGroup: "deploy", ConcurrencyLimit: 1, Priority: priority, Boost: boost,
		}
	}
	assert.True(t, taskOrderLess(member("moved", 3, -5, 1), member("early", 1, 0, 0)))
	assert.True(t, taskOrderLess(member("early", 1, 0, 0), member("high", 2, 10, 0)), "not by priority")
	older := &model.Task{PipelineID: 1, Created: 5, Name: "z"}
	newer := &model.Task{PipelineID: 2, Created: 5, Name: "a"}
	assert.True(t, taskOrderLess(older, newer), "the pipeline before the name: one pipeline's tasks are all before or after another's")
}

// The deadlock a priority in the concurrency order made: an older pipeline's
// task A, a newer pipeline's B0 and its dependent B of a high priority, all
// in one group of one slot. Had B counted ahead of A on its priority while A
// counts ahead of B0 on its age, none could run. By age, A runs, then B0,
// then B.
func TestFifoConcurrencyPriorityNoDeadlock(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	member := func(id string, pipeline int64, priority int, deps ...string) *model.Task {
		return &model.Task{
			ID: id, Name: id, RepoID: 1, PipelineID: pipeline, Created: pipeline, Priority: priority,
			ConcurrencyGroup: "1//deploy", ConcurrencyLimit: 1,
			Dependencies: deps, DepStatus: make(map[string]model.StatusValue),
		}
	}
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{
		member("A", 1, 0),
		member("B0", 2, 0),
		member("B", 2, 10, "B0"),
	}))
	assert.Equal(t, []string{"A", "B0", "B"}, pollOrder(ctx, t, q, 3))
}

// Tasks restored from the store are queued again in the order they were
// queued, not by their pipeline's creation (a pipeline approved late comes
// late), and keep their priority and boost.
func TestWithTaskStoreRestoresOrder(t *testing.T) {
	s := newSQLiteStore(t)
	workflows := make([]*model.Workflow, 5)
	for i := range workflows {
		workflows[i] = &model.Workflow{PipelineID: 1, PID: i + 1, Name: fmt.Sprint("w", i), State: model.StatusPending}
	}
	require.NoError(t, s.WorkflowsCreate(workflows))
	id := func(i int) string { return fmt.Sprint(workflows[i].ID) }

	ctx1, cancel1 := context.WithCancelCause(t.Context())
	q1 := WithTaskStore(ctx1, NewMemoryQueue(ctx1), s)
	// queued in this order; the approved one was created first
	require.NoError(t, q1.PushAtOnce(ctx1, []*model.Task{{ID: id(0), RepoID: 1, Created: 2}}))
	require.NoError(t, q1.PushAtOnce(ctx1, []*model.Task{{ID: id(1), RepoID: 2, Created: 3}}))
	require.NoError(t, q1.PushAtOnce(ctx1, []*model.Task{{ID: id(2), RepoID: 1, Created: 1}}))
	require.NoError(t, q1.PushAtOnce(ctx1, []*model.Task{{ID: id(3), RepoID: 1, Created: 4, Priority: 3}}))
	require.NoError(t, q1.PushAtOnce(ctx1, []*model.Task{{ID: id(4), RepoID: 2, Created: 5}}))
	moved, err := q1.PrioritizeAtOnce(ctx1, []string{id(4)}, NextBoost())
	require.NoError(t, err)
	require.Equal(t, []string{id(4)}, moved)
	waitForProcess()
	before := infoIDs(q1.Info(ctx1).Pending)
	cancel1(nil)

	ctx2, cancel2 := context.WithCancelCause(t.Context())
	defer cancel2(nil)
	q2 := WithTaskStore(ctx2, NewMemoryQueue(ctx2), s)
	waitForProcess()
	want := []string{id(4), id(3), id(1), id(0), id(2)}
	assert.Equal(t, want, before)
	assert.Equal(t, want, infoIDs(q2.Info(ctx2).Pending), "the order after the restart")
	assert.NotZero(t, queuedTask(t, q2.Info(ctx2), id(4)).Boost)
	assert.Equal(t, 3, queuedTask(t, q2.Info(ctx2), id(3)).Priority)

	// a move after the restart goes above the restored one
	moved, err = q2.PrioritizeAtOnce(ctx2, []string{id(2)}, NextBoost())
	require.NoError(t, err)
	assert.Equal(t, []string{id(2)}, moved)
	waitForProcess()
	assert.Equal(t, []string{id(2), id(4), id(3), id(1), id(0)}, infoIDs(q2.Info(ctx2).Pending))
}

// The persistent queue stores the boost first: a move the store does not
// take is not made in memory either.
func TestPersistentPrioritizeAtOnce(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	s := store_mocks.NewMockStore(t)
	pq := &persistentQueue{Queue: q, store: s}
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{{ID: "a"}, {ID: "b"}}))

	moved, err := pq.PrioritizeAtOnce(ctx, nil, 1)
	require.NoError(t, err)
	assert.Empty(t, moved)

	s.EXPECT().TaskSetBoost([]string{"missing"}, int64(5)).Return(nil).Once()
	moved, err = pq.PrioritizeAtOnce(ctx, []string{"missing"}, 5)
	require.NoError(t, err)
	assert.Empty(t, moved)

	s.EXPECT().TaskSetBoost([]string{"b"}, int64(6)).Return(nil).Once()
	moved, err = pq.PrioritizeAtOnce(ctx, []string{"b"}, 6)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, moved)

	s.EXPECT().TaskSetBoost([]string{"a"}, int64(7)).Return(errors.New("db down")).Once()
	moved, err = pq.PrioritizeAtOnce(ctx, []string{"a"}, 7)
	assert.ErrorContains(t, err, "db down")
	assert.Empty(t, moved)
	assert.Zero(t, queuedTask(t, q.Info(ctx), "a").Boost, "not moved in memory")
	waitForProcess()
	assert.Equal(t, []string{"b", "a"}, infoIDs(q.Info(ctx).Pending))
}

// The persistent queue stores when a task was queued.
func TestPersistentPushStampsQueued(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	s := store_mocks.NewMockStore(t)
	pq := &persistentQueue{Queue: q, store: s}
	task := &model.Task{ID: "a"}
	s.EXPECT().TaskInsert(task).RunAndReturn(func(stored *model.Task) error {
		assert.NotZero(t, stored.Queued, "stamped before it is stored")
		return nil
	}).Once()
	require.NoError(t, pq.PushAtOnce(ctx, []*model.Task{task}))
}

// A task released by its dependencies is queued then, at the tail: after a
// task of another repository queued while it waited.
func TestFifoReleasedGoesToTail(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{
		{ID: "build", RepoID: 1},
		{ID: "deploy", RepoID: 1, Dependencies: []string{"build"}, DepStatus: make(map[string]model.StatusValue)},
	}))
	first, err := q.Poll(ctx, 1, filterFnTrue)
	require.NoError(t, err)
	require.Equal(t, "build", first.ID)
	waitForProcess()
	// queued while deploy still waits, before the tick that releases it
	require.NoError(t, q.Done(ctx, "build", model.StatusSuccess))
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{{ID: "later", RepoID: 2}}))
	assert.Equal(t, []string{"later", "deploy"}, pollOrder(ctx, t, q, 2))
}

// Stamps strictly increase, also faster than the clock, and stay above one
// seeded from the future, as stored by a server with a clock ahead.
func TestNextStamp(t *testing.T) {
	previous := nextStamp()
	for range 10000 {
		next := nextStamp()
		require.Greater(t, next, previous)
		previous = next
	}

	future := time.Now().Add(time.Hour).UnixNano()
	seedStamps(future)
	assert.Greater(t, nextStamp(), future)
	seedStamps(1)
	assert.Greater(t, NextBoost(), future, "a lower seed changes nothing")
}

// restoreWorkflows stores n pending workflows, for tasks the restored queue
// does not drop.
func restoreWorkflows(t *testing.T, s interface {
	WorkflowsCreate([]*model.Workflow) error
}, n int,
) []string {
	t.Helper()
	workflows := make([]*model.Workflow, n)
	for i := range workflows {
		workflows[i] = &model.Workflow{PipelineID: int64(i + 1), PID: 1, Name: fmt.Sprint("w", i), State: model.StatusPending}
	}
	require.NoError(t, s.WorkflowsCreate(workflows))
	ids := make([]string, n)
	for i, workflow := range workflows {
		ids[i] = fmt.Sprint(workflow.ID)
	}
	return ids
}

// A task stored before tasks were stamped is older than the ones stamped:
// restored, it stays before them, and keeps its stamp in the store, so that
// another restart restores the same order. Among themselves the legacy ones
// go by creation, then pipeline, then workflow id.
func TestWithTaskStoreRestoresLegacyOrder(t *testing.T) {
	s := newSQLiteStore(t)
	ids := restoreWorkflows(t, s, 4)
	stamped := NextBoost()
	// the legacy ones in another order than restored: same creation, the
	// pipeline breaks the tie
	require.NoError(t, s.TaskInsert(&model.Task{ID: ids[0], RepoID: 1, Created: 5, PipelineID: 2}))
	require.NoError(t, s.TaskInsert(&model.Task{ID: ids[1], RepoID: 1, Created: 5, PipelineID: 1}))
	require.NoError(t, s.TaskInsert(&model.Task{ID: ids[2], RepoID: 2, Created: 1, PipelineID: 3, Queued: stamped}))
	require.NoError(t, s.TaskInsert(&model.Task{ID: ids[3], RepoID: 2, Created: 9, PipelineID: 4}))
	want := []string{ids[1], ids[0], ids[3], ids[2]}

	for restart := range 2 {
		ctx, cancel := context.WithCancelCause(t.Context())
		q := WithTaskStore(ctx, NewMemoryQueue(ctx), s)
		waitForProcess()
		assert.Equal(t, want, infoIDs(q.Info(ctx).Pending), "restart %d", restart)
		cancel(nil)
	}

	stored, err := s.TaskList()
	require.NoError(t, err)
	slices.SortFunc(stored, compareQueued)
	var queued []int64
	for _, task := range stored {
		queued = append(queued, task.Queued)
	}
	assert.Equal(t, want, infoIDs(stored), "the store keeps the order")
	assert.Equal(t, []int64{stamped - 3, stamped - 2, stamped - 1, stamped}, queued)
}

// After a restart new stamps and boosts are above the stored ones, even
// those of a clock ahead: a task queued now goes after the restored ones,
// a move now above a restored move.
func TestWithTaskStoreSeedsStamps(t *testing.T) {
	s := newSQLiteStore(t)
	ids := restoreWorkflows(t, s, 2)
	future := time.Now().Add(time.Hour).UnixNano()
	require.NoError(t, s.TaskInsert(&model.Task{ID: ids[0], RepoID: 1, Queued: future}))
	require.NoError(t, s.TaskInsert(&model.Task{ID: ids[1], RepoID: 2, Queued: future - 10, Boost: future + 5}))

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	q := WithTaskStore(ctx, NewMemoryQueue(ctx), s)
	assert.Greater(t, NextBoost(), future+5)

	workflows := []*model.Workflow{{PipelineID: 9, PID: 1, Name: "new", State: model.StatusPending}}
	require.NoError(t, s.WorkflowsCreate(workflows))
	fresh := &model.Task{ID: fmt.Sprint(workflows[0].ID), RepoID: 1}
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{fresh}))
	assert.Greater(t, fresh.Queued, future)
	waitForProcess()
	assert.Equal(t, []string{ids[1], ids[0], fresh.ID}, infoIDs(q.Info(ctx).Pending))
}
