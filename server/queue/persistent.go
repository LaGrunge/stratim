// Copyright 2021 Woodpecker Authors
// Copyright 2018 Drone.IO Inc.
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
	"slices"
	"strconv"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// serverConfigQueuePaused stores whether the global queue is paused so the
// state survives server restarts.
const serverConfigQueuePaused = "queue-paused"

// WithTaskStore returns a queue that is backed by the TaskStore. A task is
// stored from its push until its Done or Error (or its cancel, if it is not
// running), the agent it is handed out to with it: when the system starts,
// the tasks pending are queued again and the ones handed out are held running
// again, each with a new lease, so that the ones depending on them wait for
// them, their agents extend and wait on them, and a cancel reaches their
// agents. The paused flag is restored too.
//
// A workflow running that the queue does not hold, as one whose task was
// handed out by a server that did not store it, is adopted (see
// adoptOrphans): when the system starts, and every orphansCheckInterval.
func WithTaskStore(ctx context.Context, q Queue, s store.Store) Queue {
	tasks, err := s.TaskList()
	if err != nil {
		log.Error().Err(err).Msg("cannot load the stored tasks")
	}
	// in the order they were queued in, the queue's: the store keeps none
	slices.SortStableFunc(tasks, compareQueued)
	restoreStamps(s, tasks)
	pq := &persistentQueue{q, s}
	stored := make(map[string]struct{}, len(tasks))
	for _, task := range tasks {
		stored[task.ID] = struct{}{}
	}
	// held running before the pending ones are queued: the ones depending on
	// them wait for them
	pending := pq.restoreRunning(tasks)
	if err == nil {
		pq.adoptOrphans(ctx, stored)
	}
	for _, task := range pending {
		restoreDepStatus(s, task)
	}
	if err := q.PushAtOnce(ctx, pending); err != nil {
		log.Error().Err(err).Msg("PushAtOnce failed")
	}
	pq.restorePausedState()
	go pq.watchOrphans(ctx)
	return pq
}

// deleteTask removes the stored task of id, logging what fails.
func (q *persistentQueue) deleteTask(id string) {
	if err := q.store.TaskDelete(id); err != nil && !errors.Is(err, types.ErrRecordNotExist) {
		log.Error().Err(err).Str("task_id", id).Msg("cannot remove a stored task")
	}
}

// restoreStamps keeps the stamps of the restored tasks in order. The stamps
// handed out from now on are higher than any stored, as the server that
// stored them may have had a clock ahead of this one's. A task stored before
// it was stamped (0) is older than the ones stamped: in their order (see
// compareQueued), they get the stamps just below the lowest stored, and
// keep them in the store, so that they do not go to the tail, now or after
// another restart.
func restoreStamps(s store.Store, tasks []*model.Task) {
	var highest, lowest int64
	var legacy []*model.Task
	for _, task := range tasks {
		highest = max(highest, task.Queued, task.Boost)
		switch {
		case task.Queued == 0:
			legacy = append(legacy, task)
		case lowest == 0 || task.Queued < lowest:
			lowest = task.Queued
		}
	}
	seedStamps(highest)
	if len(legacy) == 0 {
		return
	}
	base := lowest - int64(len(legacy))
	for i, task := range legacy {
		if lowest == 0 || base <= 0 {
			// none stamped, or no room below them: in their order from now on
			task.Queued = nextStamp()
		} else {
			task.Queued = base + int64(i)
		}
		if err := s.TaskSetQueued(task.ID, task.Queued); err != nil {
			log.Error().Err(err).Msgf("restore queue item: %s: cannot store when it was queued", task.ID)
		}
	}
}

// restoreDepStatus sets the status of every finished dependency of a reloaded
// task from its workflow. The queue keeps that status only in memory, so the
// stored task still has the empty DepStatus it was inserted with and
// Task.ShouldRun() would vacuously let it run.
func restoreDepStatus(s store.Store, task *model.Task) {
	for _, dep := range task.Dependencies {
		workflowID, err := strconv.ParseInt(dep, 10, 64)
		if err != nil {
			log.Error().Err(err).Msgf("restore queue item: %s: invalid dependency id %s", task.ID, dep)
			continue
		}
		workflow, err := s.WorkflowLoad(workflowID)
		if err != nil {
			log.Error().Err(err).Msgf("restore queue item: %s: cannot load dependency %s", task.ID, dep)
			continue
		}
		if !isTerminalWorkflowState(workflow.State) {
			continue
		}
		if task.DepStatus == nil {
			task.DepStatus = make(map[string]model.StatusValue)
		}
		task.DepStatus[dep] = workflow.State
	}
}

type persistentQueue struct {
	Queue
	store store.Store
}

// Pause stops handing out new work and persists that state so a server
// restart keeps the queue paused.
func (q *persistentQueue) Pause() {
	q.Queue.Pause()
	if err := q.store.ServerConfigSet(serverConfigQueuePaused, "true"); err != nil {
		log.Error().Err(err).Msg("failed to persist queue paused state")
	}
}

// Resume starts handing out work again and clears the persisted paused flag.
func (q *persistentQueue) Resume() {
	q.Queue.Resume()
	if err := q.store.ServerConfigSet(serverConfigQueuePaused, "false"); err != nil {
		log.Error().Err(err).Msg("failed to persist queue resumed state")
	}
}

func (q *persistentQueue) restorePausedState() {
	value, err := q.store.ServerConfigGet(serverConfigQueuePaused)
	if err != nil {
		if !errors.Is(err, types.ErrRecordNotExist) {
			log.Error().Err(err).Msg("failed to load persisted queue paused state")
		}
		return
	}
	if value == "true" {
		log.Warn().Msg("restoring paused queue state from server config")
		q.Queue.Pause()
	}
}

func isTerminalWorkflowState(state model.StatusValue) bool {
	return state.IsDone()
}

// PushAtOnce pushes multiple tasks to the tail of this queue.
func (q *persistentQueue) PushAtOnce(c context.Context, tasks []*model.Task) error {
	// TODO: invent store.NewSession who return context including a session and make TaskInsert & TaskDelete use it
	// stored with when they were queued, so that they are queued again in
	// that order after a restart
	stampQueued(tasks)
	for _, task := range tasks {
		if err := q.store.TaskInsert(task); err != nil {
			return err
		}
	}
	err := q.Queue.PushAtOnce(c, tasks)
	if err != nil {
		for _, task := range tasks {
			if err := q.store.TaskDelete(task.ID); err != nil {
				return err
			}
		}
	}
	return err
}

// Poll retrieves a task head of this queue, and stores the agent it is
// handed out to with it: the task stays stored until its Done.
func (q *persistentQueue) Poll(c context.Context, agentID int64, f func(*model.Task) (bool, int)) (*model.Task, error) {
	task, err := q.Queue.Poll(c, agentID, f)
	if task != nil && task.PipelineID != 0 {
		// Its pipeline canceled (or ended) after the task was stored and
		// before the cancel evicted it, as when the server stopped in
		// between: the task was restored with the queue. It must not run.
		ended, lookupErr := q.pipelineEnded(task.PipelineID)
		switch {
		case lookupErr != nil:
			// Cannot tell: not handed out this time. It keeps its stored copy
			// and its place in the queue's running tasks, whose lease expires
			// and puts it back for a later poll; the agent polls on, for
			// other tasks.
			log.Error().Err(lookupErr).Msgf("pull queue item: %s: cannot load pipeline %d, retried later", task.ID, task.PipelineID)
			return nil, nil
		case ended:
			// its dependents see it killed, and the sweep ends the workflow
			log.Warn().Msgf("pull queue item: %s: pipeline ended, dropping canceled task", task.ID)
			q.deleteTask(task.ID)
			if dropErr := q.Queue.Done(c, task.ID, model.StatusKilled); dropErr != nil {
				log.Error().Err(dropErr).Msgf("pull queue item: %s: failed to drop canceled task", task.ID)
			}
			return nil, nil
		}
	}
	if task != nil {
		if stop := q.dropStale(c, task); stop {
			return nil, nil
		}
		if setErr := q.store.TaskSetAgent(task.ID, agentID); setErr != nil {
			if errors.Is(setErr, types.ErrRecordNotExist) {
				// The task is no longer in the store, which means it was
				// already finished/removed (e.g. its workflow was canceled) and
				// only lingered in the in-memory queue. Handing it to the agent
				// would loop forever, so drop it from the queue instead.
				log.Error().Err(setErr).Msgf("pull queue item: %s: not found in backup, dropping stale task", task.ID)
				if dropErr := q.Queue.Error(c, task.ID, setErr); dropErr != nil {
					log.Error().Err(dropErr).Msgf("pull queue item: %s: failed to drop stale task", task.ID)
				}
				return nil, nil
			}
			// Not handed out unstored: after a restart it would be queued
			// again while its agent runs it. It is put back at once, for a
			// later poll; if the queue cannot, its lease expires and does.
			log.Error().Err(setErr).Msgf("pull queue item: %s: cannot store its agent, retried later", task.ID)
			if holder, ok := q.Queue.(*fifo); ok {
				holder.putBack(task.ID)
			}
			return nil, nil
		}
	}
	return task, err
}

// dropStale drops a polled task whose workflow is missing or ended
// already, from the queue and the store: it reports whether it did. A task
// whose workflow cannot be loaded is handed out: its agent's Init tells.
func (q *persistentQueue) dropStale(c context.Context, task *model.Task) bool {
	workflowID, parseErr := strconv.ParseInt(task.ID, 10, 64)
	if parseErr != nil {
		log.Error().Err(parseErr).Msgf("pull queue item: %s: invalid workflow id", task.ID)
		return false
	}

	workflow, loadErr := q.store.WorkflowLoad(workflowID)
	if loadErr != nil {
		if errors.Is(loadErr, types.ErrRecordNotExist) {
			// A queued task without its workflow can never be initialized.
			// Drop it now instead of letting the agent reject and later
			// re-poll the same task after its lease expires.
			log.Error().Err(loadErr).Msgf("pull queue item: %s: workflow missing, dropping stale task", task.ID)
			q.deleteTask(task.ID)
			if dropErr := q.Queue.Error(c, task.ID, loadErr); dropErr != nil {
				log.Error().Err(dropErr).Msgf("pull queue item: %s: failed to drop stale task", task.ID)
			}
			return true
		}
		log.Error().Err(loadErr).Msgf("pull queue item: %s: failed to load workflow", task.ID)
		return false
	}

	if isTerminalWorkflowState(workflow.State) {
		// The task can still exist in the persistent queue while its
		// workflow was completed through another path. Returning it to an
		// agent makes RPC.Init reject it as already finished, while the
		// in-memory task remains running and is resubmitted after timeout.
		log.Warn().Str("state", string(workflow.State)).Msgf("pull queue item: %s: workflow already terminal, dropping stale task", task.ID)
		q.deleteTask(task.ID)
		if dropErr := q.Queue.Done(c, task.ID, workflow.State); dropErr != nil {
			log.Error().Err(dropErr).Msgf("pull queue item: %s: failed to drop terminal task", task.ID)
		}
		return true
	}
	return false
}

// pipelineEnded reports whether the pipeline of id ended: canceled, or
// ended otherwise.
func (q *persistentQueue) pipelineEnded(id int64) (bool, error) {
	pipeline, err := q.store.GetPipeline(id)
	if err != nil {
		return false, err
	}
	return pipeline.Status.IsDone(), nil
}

// Done signals the task is complete, and removes it from the store, also
// when the queue does not hold it (ErrNotFound): it ended.
func (q *persistentQueue) Done(c context.Context, id string, exitStatus model.StatusValue) error {
	err := q.Queue.Done(c, id, exitStatus)
	return errors.Join(err, q.deleteTasks([]string{id}))
}

// Error signals the task is done with an error, and removes it from the
// store, also when the queue does not hold it (ErrNotFound): it ended.
func (q *persistentQueue) Error(c context.Context, id string, err error) error {
	queueErr := q.Queue.Error(c, id, err)
	return errors.Join(queueErr, q.deleteTasks([]string{id}))
}

// CancelAtOnce cancels the tasks, see Queue, and removes them from the
// store, also when the queue does not hold some of them (ErrNotFound): the
// stored ones would be restored after a restart and run. The ones canceling
// stay stored until their Done, or their lease expires: after a restart they
// are held canceling again, and their agents' finalizers keep their time.
func (q *persistentQueue) CancelAtOnce(c context.Context, ids []string, escalate bool) ([]string, error) {
	canceling, err := q.Queue.CancelAtOnce(c, ids, escalate)
	ended := make([]string, 0, len(ids))
	for _, id := range ids {
		if !slices.Contains(canceling, id) {
			ended = append(ended, id)
		}
	}
	return canceling, errors.Join(err, q.deleteTasks(ended))
}

// PrioritizeAtOnce stores the boost of the tasks first, then moves them to
// the top of the queue, see Queue: a move that is not stored is not made, a
// move made is kept after a restart. A task handed out gets it too, as in the
// queue; one canceled meanwhile is not stored, and stays as it is.
func (q *persistentQueue) PrioritizeAtOnce(c context.Context, ids []string, boost int64) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if err := q.store.TaskSetBoost(ids, boost); err != nil {
		return nil, fmt.Errorf("failed to store the boost of tasks %v: %w", ids, err)
	}
	return q.Queue.PrioritizeAtOnce(c, ids, boost)
}

// ErrorAtOnce signals multiple tasks are done and complete with an error.
// If still pending they will just get removed from the queue.
func (q *persistentQueue) ErrorAtOnce(c context.Context, ids []string, err error) error {
	if err := q.Queue.ErrorAtOnce(c, ids, err); err != nil {
		return err
	}
	return q.deleteTasks(ids)
}

// deleteTasks removes tasks from the store, those not in it aside.
func (q *persistentQueue) deleteTasks(ids []string) error {
	var errs []error
	for _, id := range ids {
		if deleteErr := q.store.TaskDelete(id); deleteErr != nil && !errors.Is(deleteErr, types.ErrRecordNotExist) {
			errs = append(errs, fmt.Errorf("task id [%s]: %w", id, deleteErr))
		}
	}

	if len(errs) != 0 {
		return fmt.Errorf("failed to delete tasks from persistent store: %w", errors.Join(errs...))
	}
	return nil
}
