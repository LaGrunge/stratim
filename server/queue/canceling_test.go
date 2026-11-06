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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/shared/constant"
)

// A running task canceled is told so, and stays running for its agent, its
// lease extended, until its Done: its finalizers may still run.
func TestFifoCanceledRunningTaskStaysUntilDone(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	task := &model.Task{ID: "canceling-1"}
	dependent := &model.Task{
		ID:           "canceling-dep",
		Dependencies: []string{"canceling-1"},
		DepStatus:    make(map[string]model.StatusValue),
		RunOn:        []string{"success", "failure"},
	}
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))
	waitForProcess()
	got, err := q.Poll(ctx, 7, filterFnTrue)
	require.NoError(t, err)
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{dependent}))

	canceling, err := q.CancelAtOnce(ctx, []string{got.ID, "not-in-the-queue"}, false)
	assert.Error(t, err, "one it does not have")
	assert.Equal(t, []string{got.ID}, canceling)
	assert.ErrorIs(t, q.Wait(ctx, got.ID), ErrCancel, "its agent is told")
	assert.ErrorIs(t, q.Wait(ctx, got.ID), ErrCancel, "and told again: a Wait retried after a lost reply")

	info := q.Info(ctx)
	require.Len(t, info.Running, 1, "it runs on for its agent")
	assert.Equal(t, int64(7), info.Running[0].AgentID)
	assert.NoError(t, q.Extend(ctx, 7, got.ID), "its lease is extended")
	assert.Len(t, q.Info(ctx).Running, 1)

	waitForProcess()
	gotDep, err := q.Poll(ctx, 8, filterFnTrue)
	require.NoError(t, err)
	assert.Equal(t, model.StatusKilled, gotDep.DepStatus["canceling-1"], "its dependents see it killed at once")
	require.NoError(t, q.Done(ctx, gotDep.ID, model.StatusSuccess))

	require.NoError(t, q.Done(ctx, got.ID, model.StatusKilled))
	assert.Empty(t, q.Info(ctx).Running, "its Done ends it")
	assert.ErrorIs(t, q.Extend(ctx, 7, got.ID), ErrNotFound)
}

// canceledTask is a task polled by agent 1 and canceled.
func canceledTask(t *testing.T, ctx context.Context, q *fifo, task *model.Task) *model.Task {
	t.Helper()
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{task}))
	waitForProcess()
	got, err := q.Poll(ctx, 1, filterFnTrue)
	require.NoError(t, err)
	canceling, err := q.CancelAtOnce(ctx, []string{got.ID}, false)
	require.NoError(t, err)
	require.Equal(t, []string{got.ID}, canceling)
	return got
}

// waitKilled runs WaitKilled in the background.
func waitKilled(ctx context.Context, q *fifo, id string) <-chan error {
	killed := make(chan error, 1)
	go func() { killed <- q.WaitKilled(ctx, id) }()
	return killed
}

// assertWaiting asserts the WaitKilled did not return in 200ms.
func assertWaiting(t *testing.T, killed <-chan error) {
	t.Helper()
	select {
	case err := <-killed:
		t.Fatalf("WaitKilled returned: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}

// assertKilled asserts the WaitKilled returned ErrCancel.
func assertKilled(t *testing.T, killed <-chan error) {
	t.Helper()
	select {
	case err := <-killed:
		assert.ErrorIs(t, err, ErrCancel)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitKilled did not return")
	}
}

// A second cancel stops the finalizers too: WaitKilled returns. The task
// runs on until its Done; a third cancel changes nothing.
func TestFifoSecondCancel(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	got := canceledTask(t, ctx, q, &model.Task{ID: "twice"})

	killed := waitKilled(ctx, q, got.ID)
	assertWaiting(t, killed)
	canceling, err := q.CancelAtOnce(ctx, []string{got.ID}, false)
	require.NoError(t, err)
	assert.Equal(t, []string{got.ID}, canceling, "a cancel again, not escalated: still canceling")
	assertWaiting(t, killed)
	canceling, err = q.CancelAtOnce(ctx, []string{got.ID}, true)
	require.NoError(t, err)
	assert.Equal(t, []string{got.ID}, canceling)
	assertKilled(t, killed)
	assert.ErrorIs(t, q.WaitKilled(ctx, got.ID), ErrCancel, "whoever waits after it")
	assert.ErrorIs(t, q.Wait(ctx, got.ID), ErrCancel, "a plain Wait still tells the cancel")

	canceling, err = q.CancelAtOnce(ctx, []string{got.ID}, true)
	require.NoError(t, err)
	assert.Equal(t, []string{got.ID}, canceling)
	assert.Len(t, q.Info(ctx).Running, 1)
	require.NoError(t, q.Done(ctx, got.ID, model.StatusKilled))
	assertWaiting(t, waitKilled(ctx, q, got.ID))
}

// A task the queue does not have, as after a server restart, is no reason
// to stop its finalizers: its agent's WaitKilled, retried, waits until its
// context ends.
func TestFifoWaitKilledUnknownTask(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	waitCtx, cancel := context.WithCancelCause(ctx)
	killed := waitKilled(waitCtx, q, "unknown")
	assertWaiting(t, killed)
	cancel(nil)
	select {
	case err := <-killed:
		assert.NoError(t, err, "its context ended: not killed")
	case <-time.After(5 * time.Second):
		t.Fatal("WaitKilled did not return with its context")
	}
}

// The queue holds a canceled task at most MaxCanceledHold after its cancel:
// the longest finalizers' time, its agent reporting their end at the
// latest the queue takes, its lease extended right before that ends.
func TestMaxCanceledHold(t *testing.T) {
	q := &fifo{extension: constant.TaskTimeout, canceledGrace: canceledLeaseGrace}
	canceledAt := time.Now()
	const finalize = time.Hour
	state := &entry{
		item:             &model.Task{FinalizeTimeout: int64(finalize / time.Second)},
		canceling:        true,
		canceledAt:       canceledAt,
		finalizeDeadline: canceledAt.Add(10 * finalize),
	}
	end := q.canceledLeaseEnd(state)
	// the last Extend the cap allows, then the lease running out and the
	// queue noticing it
	lastLease := end.Add(q.extension + processTimeInterval)
	assert.False(t, lastLease.After(canceledAt.Add(MaxCanceledHold(finalize))))
	assert.True(t, end.After(canceledAt.Add(finalize)), "the finalizers' time is held")
}

// The queue giving up on a canceled task stops its finalizers: its Done,
// its lease expired, or capped.
func TestFifoWaitKilledWhenGivenUp(t *testing.T) {
	t.Run("done", func(t *testing.T) {
		ctx, _, q := setupTestQueue(t)
		got := canceledTask(t, ctx, q, &model.Task{ID: "done"})
		killed := waitKilled(ctx, q, got.ID)
		assertWaiting(t, killed)
		require.NoError(t, q.Done(ctx, got.ID, model.StatusKilled))
		assertKilled(t, killed)
	})
	t.Run("expired", func(t *testing.T) {
		ctx, _, q := setupTestQueue(t)
		q.extension = 300 * time.Millisecond
		got := canceledTask(t, ctx, q, &model.Task{ID: "expired"})
		killed := waitKilled(ctx, q, got.ID)
		assertKilled(t, killed)
	})
	t.Run("capped", func(t *testing.T) {
		ctx, _, q := setupTestQueue(t)
		q.canceledGrace = 50 * time.Millisecond
		got := canceledTask(t, ctx, q, &model.Task{ID: "capped"})
		killed := waitKilled(ctx, q, got.ID)
		time.Sleep(100 * time.Millisecond)
		assert.ErrorIs(t, q.Extend(ctx, 1, got.ID), ErrCanceledLeaseOver)
		assertKilled(t, killed)
	})
	t.Run("not canceled", func(t *testing.T) {
		ctx, _, q := setupTestQueue(t)
		require.NoError(t, q.PushAtOnce(ctx, []*model.Task{{ID: "running"}}))
		waitForProcess()
		got, err := q.Poll(ctx, 1, filterFnTrue)
		require.NoError(t, err)
		assert.NoError(t, q.WaitKilled(ctx, got.ID), "nothing to wait for")
	})
}

// A canceled task's lease is extended for its finalizers' time and a grace
// from the cancel, however long its agent asks: then it expires and is
// settled.
func TestFifoCanceledLeaseCapped(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	q.canceledGrace = 100 * time.Millisecond
	got := canceledTask(t, ctx, q, &model.Task{ID: "capped", FinalizeTimeout: 1})

	time.Sleep(300 * time.Millisecond)
	assert.NoError(t, q.Extend(ctx, 1, got.ID), "within its finalizers' second")
	time.Sleep(900 * time.Millisecond)
	assert.ErrorIs(t, q.Extend(ctx, 1, got.ID), ErrCanceledLeaseOver, "past it and the grace")

	running := &model.Task{ID: "not-canceled"}
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{running}))
	waitForProcess()
	got2, err := q.Poll(ctx, 1, filterFnTrue)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	assert.NoError(t, q.Extend(ctx, 1, got2.ID), "a running task is not capped")
}

// An agent telling when its finalizers' time ends is held until then and a
// grace, later than counted from the cancel, within the bound; earlier too.
func TestFifoCanceledLeaseFollowsReportedDeadline(t *testing.T) {
	t.Run("later", func(t *testing.T) {
		ctx, _, q := setupTestQueue(t)
		q.canceledGrace = 100 * time.Millisecond
		got := canceledTask(t, ctx, q, &model.Task{ID: "later", FinalizeTimeout: 0})
		// the regular steps took long to stop: the finalizers end later
		deadline := time.Now().Add(time.Second)
		time.Sleep(300 * time.Millisecond)
		assert.NoError(t, q.ExtendFinalizing(ctx, 1, got.ID, deadline), "past the old cap, within the reported end")
		assert.NoError(t, q.Extend(ctx, 1, got.ID), "the reported end is kept")
		time.Sleep(time.Until(deadline) + 200*time.Millisecond)
		assert.ErrorIs(t, q.Extend(ctx, 1, got.ID), ErrCanceledLeaseOver, "past the reported end and the grace")
	})
	t.Run("bounded", func(t *testing.T) {
		ctx, _, q := setupTestQueue(t)
		q.canceledGrace = 100 * time.Millisecond
		got := canceledTask(t, ctx, q, &model.Task{ID: "bounded", FinalizeTimeout: 0})
		q.Lock()
		q.running[got.ID].canceledAt = time.Now().Add(-maxFinalizeStartDelay - time.Second)
		q.Unlock()
		assert.ErrorIs(t, q.ExtendFinalizing(ctx, 1, got.ID, time.Now().Add(time.Hour)), ErrCanceledLeaseOver,
			"an end reported far off is not followed past the bound")
	})
	t.Run("old agent", func(t *testing.T) {
		ctx, _, q := setupTestQueue(t)
		q.canceledGrace = 100 * time.Millisecond
		got := canceledTask(t, ctx, q, &model.Task{ID: "old", FinalizeTimeout: 0})
		time.Sleep(200 * time.Millisecond)
		assert.ErrorIs(t, q.Extend(ctx, 1, got.ID), ErrCanceledLeaseOver, "no end reported: counted from the cancel")
	})
}

// A canceled task whose lease expires before its Done is handed to the
// handler to settle, not run again.
func TestFifoCanceledTaskExpiry(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	q.extension = 50 * time.Millisecond
	settled := make(chan *model.Task, 1)
	q.SetCanceledExpiredHandler(func(task *model.Task) { settled <- task })

	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{{ID: "canceling-exp"}}))
	waitForProcess()
	got, err := q.Poll(ctx, 1, filterFnTrue)
	require.NoError(t, err)
	require.NoError(t, q.ErrorAtOnce(ctx, []string{got.ID}, ErrCancel))

	select {
	case task := <-settled:
		assert.Equal(t, got.ID, task.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("the expired canceled task was not settled")
	}
	waitForProcess()
	info := q.Info(ctx)
	assert.Empty(t, info.Running)
	assert.Empty(t, info.Pending, "not run again")
}

// A running task that is not canceled is still resubmitted on expiry.
func TestFifoExpiredTaskResubmitted(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	q.extension = 50 * time.Millisecond
	q.SetCanceledExpiredHandler(func(*model.Task) { t.Error("not a canceled task") })

	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{{ID: "expiring"}}))
	waitForProcess()
	_, err := q.Poll(ctx, 1, filterFnTrue)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	waitForProcess()
	assert.Len(t, q.Info(ctx).Pending, 1)
}

// A task skipped while waiting for a skipped dependency waited for a skip
// that did not come: the tasks after it waiting for one do not see one.
func TestFifoSkippedWaiterIsNoSkip(t *testing.T) {
	ctx, _, q := setupTestQueue(t)
	waiter := &model.Task{ID: "waiter", RunOn: []string{"skipped"}}
	next := &model.Task{
		ID:           "next",
		Dependencies: []string{"waiter"},
		DepStatus:    make(map[string]model.StatusValue),
		RunOn:        []string{"skipped"},
	}
	plain := &model.Task{ID: "plain"}
	afterPlain := &model.Task{
		ID:           "after-plain",
		Dependencies: []string{"plain"},
		DepStatus:    make(map[string]model.StatusValue),
		RunOn:        []string{"skipped"},
	}
	require.NoError(t, q.PushAtOnce(ctx, []*model.Task{waiter, next, plain, afterPlain}))
	waitForProcess()
	for range 2 {
		got, err := q.Poll(ctx, 1, filterFnTrue)
		require.NoError(t, err)
		require.NoError(t, q.Done(ctx, got.ID, model.StatusSkipped))
	}
	waitForProcess()
	waitForProcess()

	for range 2 {
		got, err := q.Poll(ctx, 1, filterFnTrue)
		require.NoError(t, err)
		switch got.ID {
		case "next":
			assert.Equal(t, model.StatusCanceled, got.DepStatus["waiter"])
			assert.False(t, got.ShouldRun(), "no skip came")
		case "after-plain":
			assert.Equal(t, model.StatusSkipped, got.DepStatus["plain"])
			assert.True(t, got.ShouldRun(), "a plain skip is one")
		}
		require.NoError(t, q.Done(ctx, got.ID, model.StatusSuccess))
	}
}
