// Copyright 2022 Woodpecker Authors
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
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/shared/constant"
)

type entry struct {
	item     *model.Task
	done     chan bool
	error    error
	deadline time.Time
	// canceling is set once the task was canceled while running: its agent
	// was told (done is closed) and stops it, its finalizers may still run.
	// It holds its lease, and its agent, until Done or its lease expires.
	canceling bool
	// canceledAt is when it was canceled: its lease is extended for its
	// finalizers' time and a grace from then, not longer.
	canceledAt time.Time
	// finalizeDeadline is when its finalizers' time ends, as its agent
	// reported it (ExtendFinalizing); zero if it did not.
	finalizeDeadline time.Time
	// killed is closed by a second cancel, which stops its finalizers too,
	// or once the queue gives up on it: its lease expired or was capped, or
	// it left the queue. Set when it is canceled.
	killed chan bool
}

// kill closes the killed channel of a canceling entry, once.
func (e *entry) kill() {
	if e.killed == nil {
		return
	}
	select {
	case <-e.killed:
	default:
		close(e.killed)
	}
}

type worker struct {
	agentID int64
	filter  func(*model.Task) (bool, int)
	channel chan *model.Task
	stop    context.CancelCauseFunc
}

type fifo struct {
	sync.Mutex

	ctx           context.Context
	workers       map[*worker]struct{}
	running       map[string]*entry
	pending       *list.List
	waitingOnDeps *list.List
	extension     time.Duration
	// canceledGrace is how long past its finalizers' time a canceled task's
	// lease is extended.
	canceledGrace time.Duration
	paused        bool
	// onCanceledExpired settles a canceled task whose lease expired before
	// its Done: its agent is gone. Nil if not set.
	onCanceledExpired func(task *model.Task)
}

// processTimeInterval is the time till the queue rearranges things,
// as the agent pull in 10 milliseconds we should also give them work asap.
const processTimeInterval = 100 * time.Millisecond

// canceledLeaseGrace is how long past its finalizers' time the lease of a
// canceled task is extended: the regular steps stopping, and the agent's
// own grace (a minute), fit in it.
const canceledLeaseGrace = 2 * time.Minute

// maxFinalizeStartDelay bounds how much later than its finalizers' time
// counted from the cancel the end its agent reports may be: the time its
// regular steps took to stop.
const maxFinalizeStartDelay = 15 * time.Minute

// MaxCanceledHold is how long after its cancel the queue holds a canceled
// task whose finalizers' time is finalizeTimeout at most: its lease extended
// until the latest end its agent may report and the grace, then expiring.
func MaxCanceledHold(finalizeTimeout time.Duration) time.Duration {
	return finalizeTimeout + canceledLeaseGrace + maxFinalizeStartDelay + constant.TaskTimeout + processTimeInterval
}

// NewMemoryQueue returns a new fifo queue.
func NewMemoryQueue(ctx context.Context) Queue {
	q := &fifo{
		ctx:           ctx,
		workers:       make(map[*worker]struct{}),
		running:       make(map[string]*entry),
		pending:       list.New(),
		waitingOnDeps: list.New(),
		extension:     constant.TaskTimeout,
		canceledGrace: canceledLeaseGrace,
		paused:        false,
	}
	go q.process()
	return q
}

// PushAtOnce pushes multiple tasks to the tail of this queue.
func (q *fifo) PushAtOnce(_ context.Context, tasks []*model.Task) error {
	q.Lock()
	for _, task := range tasks {
		q.pending.PushBack(task)
	}
	q.Unlock()
	return nil
}

// Poll retrieves and removes a task head of this queue.
func (q *fifo) Poll(c context.Context, agentID int64, filter func(*model.Task) (bool, int)) (*model.Task, error) {
	q.Lock()
	ctx, stop := context.WithCancelCause(c)

	w := &worker{
		agentID: agentID,
		channel: make(chan *model.Task, 1),
		filter:  filter,
		stop:    stop,
	}
	q.workers[w] = struct{}{}
	q.Unlock()

	for {
		select {
		case <-ctx.Done():
			q.Lock()
			delete(q.workers, w)
			q.Unlock()
			return nil, ctx.Err()
		case t := <-w.channel:
			return t, nil
		}
	}
}

// Done signals the task is complete.
func (q *fifo) Done(_ context.Context, id string, exitStatus model.StatusValue) error {
	_, err := q.finished([]string{id}, exitStatus, nil, false)
	return err
}

// Error signals the task is done with an error.
func (q *fifo) Error(_ context.Context, id string, err error) error {
	_, err = q.finished([]string{id}, model.StatusFailure, err, false)
	return err
}

// ErrorAtOnce signals multiple tasks are done and complete with an error.
// If still pending they will just get removed from the queue.
func (q *fifo) ErrorAtOnce(_ context.Context, ids []string, err error) error {
	if errors.Is(err, ErrCancel) {
		_, err := q.finished(ids, model.StatusKilled, err, false)
		return err
	}
	_, err = q.finished(ids, model.StatusFailure, err, false)
	return err
}

// CancelAtOnce cancels the tasks as ErrorAtOnce with ErrCancel does, and
// returns the ones canceling: running, their agents told and stopping them,
// the ones canceled before told to stop their finalizers too.
func (q *fifo) CancelAtOnce(_ context.Context, ids []string, escalate bool) ([]string, error) {
	return q.finished(ids, model.StatusKilled, ErrCancel, escalate)
}

// locks the queue itself!
func (q *fifo) finished(ids []string, exitStatus model.StatusValue, err error, escalate bool) (canceling []string, _ error) {
	q.Lock()
	defer q.Unlock()

	// it's an external error so we wrap it
	err = NewErrExternal(err)

	canceled := errors.Is(err, ErrCancel)
	var errs []error
	updated := make(map[string]model.StatusValue, len(ids))
	// we first process the tasks itself
	for _, id := range ids {
		taskEntry, ok := q.running[id]
		status := exitStatus
		if ok {
			status = depStatus(taskEntry.item, exitStatus)
		}
		switch {
		case !ok:
			errs = append(errs, q.removeFromPendingAndWaiting(id))
		case taskEntry.canceling && canceled:
			// canceled again: only an explicit second cancel (escalate) stops its
			// finalizers too
			if escalate {
				taskEntry.kill()
			}
			canceling = append(canceling, id)
			continue
		case taskEntry.canceling:
			// its agent reports it done after the cancel
			taskEntry.kill()
			delete(q.running, id)
		case canceled:
			// its agent stops it, and runs its finalizers: it stays running,
			// holding its lease and its agent, until Done or its lease expires
			taskEntry.error = err
			close(taskEntry.done)
			taskEntry.canceling = true
			taskEntry.canceledAt = time.Now()
			taskEntry.killed = make(chan bool)
			canceling = append(canceling, id)
		default:
			taskEntry.error = err
			close(taskEntry.done)
			delete(q.running, id)
		}
		updated[id] = status
	}

	// next we aim for there dependencies
	// we do this because in our ids list there could be tasks and its dependencies
	// so not to mess things up
	for _, id := range ids {
		if status, ok := updated[id]; ok {
			q.updateDepStatusInQueue(id, status)
		}
	}

	return canceling, errors.Join(errs...)
}

// Wait waits until the item is done executing.
// Also signals via error ErrCancel if workflow got canceled, as often as
// asked once canceled.
func (q *fifo) Wait(ctx context.Context, taskID string) error {
	q.Lock()
	state := q.running[taskID]
	q.Unlock()
	if state != nil {
		select {
		case <-ctx.Done():
		case <-state.done:
			// check if we have a wrapped cancel error and unwrap it
			if errors.Is(state.error, ErrCancel) {
				return ErrCancel
			}
			// or return queue errors and no workflow errors
			if !errors.Is(state.error, new(ErrExternal)) {
				return state.error
			}
		}
	}
	return nil
}

// WaitKilled waits, for a task its agent was told canceled, until its
// finalizers are to be stopped too: a second cancel, or the queue giving up
// on it. It returns ErrCancel then; nil for a task not canceled, or when ctx
// ends first. For a task the queue does not have, as after a server restart
// (its agent's wait retried), it waits until ctx ends: the restart is no
// reason to stop the finalizers, their time or the sweep ends them.
func (q *fifo) WaitKilled(ctx context.Context, taskID string) error {
	q.Lock()
	state, ok := q.running[taskID]
	var killed chan bool
	if ok {
		killed = state.killed
	}
	q.Unlock()
	switch {
	case !ok:
		<-ctx.Done()
		return nil
	case killed == nil:
		return nil
	}
	select {
	case <-ctx.Done():
		return nil
	case <-killed:
		return ErrCancel
	}
}

// Extend extends the task execution deadline.
func (q *fifo) Extend(_ context.Context, agentID int64, taskID string) error {
	return q.extend(agentID, taskID, time.Time{})
}

// ExtendFinalizing extends the task execution deadline, its agent telling
// when its finalizers' time ends: a canceled task is held until then (and a
// grace), not until its finalizers' time counted from the cancel.
func (q *fifo) ExtendFinalizing(_ context.Context, agentID int64, taskID string, finalizeDeadline time.Time) error {
	return q.extend(agentID, taskID, finalizeDeadline)
}

func (q *fifo) extend(agentID int64, taskID string, finalizeDeadline time.Time) error {
	q.Lock()
	defer q.Unlock()

	state, ok := q.running[taskID]
	if !ok {
		return ErrNotFound
	}
	if state.item.AgentID != agentID {
		return ErrAgentMissMatch
	}
	if !finalizeDeadline.IsZero() {
		state.finalizeDeadline = finalizeDeadline
	}
	// a canceled task is held for its finalizers' time, not as long as its
	// agent extends it: its lease then expires and it is settled
	if state.canceling && time.Now().After(q.canceledLeaseEnd(state)) {
		// the server gives up on it: a live agent stops its finalizers
		state.kill()
		return ErrCanceledLeaseOver
	}

	state.deadline = time.Now().Add(q.extension)
	return nil
}

// canceledLeaseEnd is when the lease of a canceled task ends: the end of its
// finalizers' time its agent reported, or, from an agent that reports none,
// that time counted from the cancel; a grace after either. The finalizers
// start once the regular steps stopped, so a reported end may come later,
// but never more than maxFinalizeStartDelay later: an agent cannot hold it
// for ever.
func (q *fifo) canceledLeaseEnd(state *entry) time.Time {
	finalize := time.Duration(state.item.FinalizeTimeout) * time.Second
	fromCancel := state.canceledAt.Add(finalize + q.canceledGrace)
	if state.finalizeDeadline.IsZero() {
		return fromCancel
	}
	return minTime(state.finalizeDeadline.Add(q.canceledGrace), fromCancel.Add(maxFinalizeStartDelay))
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// Info returns internal queue information.
func (q *fifo) Info(_ context.Context) InfoT {
	q.Lock()
	stats := InfoT{}
	stats.Stats.Workers = len(q.workers)
	stats.Stats.Pending = q.pending.Len()
	stats.Stats.WaitingOnDeps = q.waitingOnDeps.Len()
	stats.Stats.Running = len(q.running)

	for element := q.pending.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		stats.Pending = append(stats.Pending, task)
	}
	for element := q.waitingOnDeps.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		stats.WaitingOnDeps = append(stats.WaitingOnDeps, task)
	}
	for _, entry := range q.running {
		stats.Running = append(stats.Running, entry.item)
	}
	stats.Paused = q.paused

	q.Unlock()
	return stats
}

// Pause stops the queue from handing out new work items in Poll.
func (q *fifo) Pause() {
	q.Lock()
	q.paused = true
	q.Unlock()
}

// Resume starts the queue again.
func (q *fifo) Resume() {
	q.Lock()
	q.paused = false
	q.Unlock()
}

// KickAgentWorkers kicks all workers for a given agent.
func (q *fifo) KickAgentWorkers(agentID int64) {
	q.Lock()
	defer q.Unlock()

	for worker := range q.workers {
		if worker.agentID == agentID {
			worker.stop(ErrWorkerKicked)
			delete(q.workers, worker)
		}
	}
}

// helper function that loops through the queue and attempts to
// match the item to a single subscriber until context got cancel.
func (q *fifo) process() {
	for {
		select {
		case <-time.After(processTimeInterval):
		case <-q.ctx.Done():
			return
		}

		q.Lock()
		if q.paused {
			q.Unlock()
			continue
		}

		canceledExpired := q.resubmitExpiredPipelines()
		if handler := q.onCanceledExpired; handler != nil {
			for _, task := range canceledExpired {
				go handler(task)
			}
		}
		q.filterWaiting()
		for pending, worker := q.assignToWorker(); pending != nil && worker != nil; pending, worker = q.assignToWorker() {
			task, _ := pending.Value.(*model.Task)
			task.AgentID = worker.agentID
			delete(q.workers, worker)
			q.pending.Remove(pending)
			q.running[task.ID] = &entry{
				item:     task,
				done:     make(chan bool),
				deadline: time.Now().Add(q.extension),
			}
			worker.channel <- task
		}
		q.Unlock()
	}
}

func (q *fifo) filterWaiting() {
	// resubmits all waiting tasks to pending, deps may have cleared
	for element := q.waitingOnDeps.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		q.pending.PushBack(task)
	}

	// rebuild waitingDeps
	q.waitingOnDeps = list.New()
	var filtered []*list.Element
	var pendingIDs map[string]struct{}
	for element := q.pending.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		if len(task.Dependencies) == 0 {
			continue
		}
		if pendingIDs == nil {
			// Snapshot all queued IDs before removing any blocked tasks. Waiting
			// tasks have been requeued above and can themselves be dependencies.
			// Build the index only when dependencies need checking.
			pendingIDs = make(map[string]struct{}, q.pending.Len())
			for pending := q.pending.Front(); pending != nil; pending = pending.Next() {
				queued, _ := pending.Value.(*model.Task)
				pendingIDs[queued.ID] = struct{}{}
			}
		}
		if q.depsInQueue(task, pendingIDs) {
			q.waitingOnDeps.PushBack(task)
			filtered = append(filtered, element)
		}
	}

	// filter waiting tasks
	for _, f := range filtered {
		q.pending.Remove(f)
	}
}

func (q *fifo) assignToWorker() (*list.Element, *worker) {
	var bestWorker *worker
	var bestScore int

	for element := q.pending.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)

		// skip tasks that would exceed their workflow concurrency limit, they
		// stay pending and are retried on the next process tick.
		if !q.canRunConcurrent(task) {
			continue
		}

		for worker := range q.workers {
			matched, score := worker.filter(task)
			if matched && score > bestScore {
				bestWorker = worker
				bestScore = score
			}
		}
		if bestWorker != nil {
			log.Debug().
				Str("task_id", task.ID).
				Strs("dependencies", task.Dependencies).
				Int("score", bestScore).
				Msg("queue: assigned task to worker")
			return element, bestWorker
		}
	}

	return nil, nil
}

// canRunConcurrent reports whether the given task may currently start without
// violating its workflow concurrency limit. Tasks without a limit always pass,
// keeping the default scheduling behavior unchanged.
//
// Slots within a concurrency group are granted in creation order (earliest
// pipeline first, by the task's Created timestamp, with the workflow name as a
// deterministic tiebreaker) rather than in the order tasks become ready. This
// guarantees that a later pipeline whose dependencies happen to finish faster
// cannot overtake an earlier one that is still waiting.
//
// The ordering reservation only applies across pipelines. Within a single
// pipeline, execution order is already defined by depends_on, and reserving a
// slot for an earlier workflow could deadlock when it depends on a later
// workflow that shares the same group (e.g. deploy.yaml depending on
// test.yaml). Because dependencies never cross pipelines, restricting the
// reservation to other pipelines keeps the ordering guarantee while making
// such deadlocks impossible.
//
// Expects the queue to be locked by the caller.
func (q *fifo) canRunConcurrent(task *model.Task) bool {
	if task.ConcurrencyLimit <= 0 || task.ConcurrencyGroup == "" {
		return true
	}

	group := task.ConcurrencyGroup

	// count tasks of the same group that already occupy a running slot.
	running := 0
	for _, e := range q.running {
		if e.item.ConcurrencyGroup == group {
			running++
		}
	}
	if running >= task.ConcurrencyLimit {
		return false
	}

	// count not-yet-running members of the group from earlier pipelines. They
	// have priority for the remaining slots, even if they are still waiting on
	// their dependencies, so cross-pipeline ordering is preserved.
	ahead := 0
	countAhead := func(other *model.Task) {
		if other.ConcurrencyGroup != group || other.ID == task.ID {
			return
		}
		// only reserve order across pipelines; see the function doc above.
		if other.PipelineID == task.PipelineID {
			return
		}
		if taskOrderLess(other, task) {
			ahead++
		}
	}
	for element := q.pending.Front(); element != nil; element = element.Next() {
		other, _ := element.Value.(*model.Task)
		countAhead(other)
	}
	for element := q.waitingOnDeps.Front(); element != nil; element = element.Next() {
		other, _ := element.Value.(*model.Task)
		countAhead(other)
	}

	return running+ahead < task.ConcurrencyLimit
}

// taskOrderLess reports whether task a was instantiated before task b. Ordering
// is by the Created timestamp (the pipeline creation time), with the workflow
// name as a deterministic tiebreaker for tasks created within the same second.
// The task ID is intentionally not used for ordering.
func taskOrderLess(a, b *model.Task) bool {
	if a.Created != b.Created {
		return a.Created < b.Created
	}
	return a.Name < b.Name
}

// depStatus is the status task ended with for the tasks depending on it. A
// task skipped while it waits for a skipped dependency (status: skipped)
// waited for one that did not come: it counts as canceled for them, not as
// a skip the ones waiting for a skip would run on.
func depStatus(task *model.Task, status model.StatusValue) model.StatusValue {
	if status == model.StatusSkipped && slices.Contains(task.RunOn, string(model.StatusSkipped)) {
		return model.StatusCanceled
	}
	return status
}

// SetCanceledExpiredHandler sets what settles a canceled task whose lease
// expired before its Done.
func (q *fifo) SetCanceledExpiredHandler(handler func(task *model.Task)) {
	q.Lock()
	defer q.Unlock()
	q.onCanceledExpired = handler
}

// resubmitExpiredPipelines puts the running tasks whose lease expired back
// to pending, for another agent. A canceled one is not run again: it is
// returned, to be settled by the caller once the queue is unlocked.
// Expects the queue to be locked by the caller.
func (q *fifo) resubmitExpiredPipelines() (canceledExpired []*model.Task) {
	for taskID, taskState := range q.running {
		if time.Now().After(taskState.deadline) && taskState.canceling {
			log.Info().Str("task_id", taskID).Msg("queue: canceled task expired before its agent reported it done")
			delete(q.running, taskID)
			taskState.kill()
			canceledExpired = append(canceledExpired, taskState.item)
			continue
		}
		if time.Now().After(taskState.deadline) {
			log.Info().Str("task_id", taskID).Msg("queue: resubmitting expired task")
			taskState.error = ErrTaskExpired
			q.pending.PushFront(taskState.item)
			delete(q.running, taskID)
			close(taskState.done)
		}
	}
	return canceledExpired
}

func (q *fifo) depsInQueue(task *model.Task, pendingIDs map[string]struct{}) bool {
	for _, dep := range task.Dependencies {
		if _, ok := pendingIDs[dep]; ok {
			return true
		}
		// a canceled one ended for its dependents: they got its status
		if running, ok := q.running[dep]; ok && !running.canceling {
			return true
		}
	}
	return false
}

// expects the q to be currently owned e.g. locked by caller!
func (q *fifo) updateDepStatusInQueue(taskID string, status model.StatusValue) {
	for element := q.pending.Front(); element != nil; element = element.Next() {
		pending, _ := element.Value.(*model.Task)
		for _, dep := range pending.Dependencies {
			if taskID == dep {
				pending.DepStatus[dep] = status
			}
		}
	}

	for _, running := range q.running {
		for _, dep := range running.item.Dependencies {
			if taskID == dep {
				running.item.DepStatus[dep] = status
			}
		}
	}

	for element := q.waitingOnDeps.Front(); element != nil; element = element.Next() {
		waiting, _ := element.Value.(*model.Task)
		for _, dep := range waiting.Dependencies {
			if taskID == dep {
				waiting.DepStatus[dep] = status
			}
		}
	}
}

// expects the q to be currently owned e.g. locked by caller!
func (q *fifo) removeFromPendingAndWaiting(taskID string) error {
	// we assume pending first
	for element := q.pending.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		if task.ID == taskID {
			log.Debug().Str("task_id", taskID).Msg("queue: task removed from pending")
			_ = q.pending.Remove(element)
			return nil
		}
	}

	// well looks like it's waiting
	for element := q.waitingOnDeps.Front(); element != nil; element = element.Next() {
		task, _ := element.Value.(*model.Task)
		if task.ID == taskID {
			log.Debug().Str("task_id", taskID).Msg("queue: task removed from waitingOnDeps")
			_ = q.waitingOnDeps.Remove(element)
			return nil
		}
	}

	// well it could not be found
	return ErrNotFound
}
