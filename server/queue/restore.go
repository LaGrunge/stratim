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
	"strconv"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// The leases of the workflows running, kept over a restart: the tasks
// handed out are held running again when the server starts, the workflows
// running the queue does not hold are adopted, and a task not run again
// whose lease expired is settled. See WithTaskStore.

// orphansCheckInterval is how often the queue looks for workflows running
// that it does not hold, see adoptOrphans. The agents' wait (rpc.RPC.Wait)
// checks more often whether the queue holds a workflow it waits on.
const orphansCheckInterval = time.Minute

// orphansPage is how many workflows running one read of the store lists
// when the queue looks for orphans; it reads them all, page by page. Only
// tests change it.
var orphansPage = 500

// leaseHolder is the queue the store backs, the one that holds leases
// (fifo.restoreRunning, fifo.putBack); not ok, logged, if it is another: the
// tasks handed out are then queued again, and no workflow is adopted.
func (q *persistentQueue) leaseHolder() (*fifo, bool) {
	holder, ok := q.Queue.(*fifo)
	if !ok {
		log.Error().Msgf("the queue %T cannot hold the leases of tasks handed out: they are queued again after a restart", q.Queue)
	}
	return holder, ok
}

// restoreRunning holds the stored tasks handed out to an agent running
// again, and returns the others, pending. A task whose workflow ended, or is
// missing, is removed from the store instead: its Done reached the store, not
// the queue. One of a pipeline that ended is held canceling, as from its
// pipeline's end. A queue that cannot hold them running gets them pending.
func (q *persistentQueue) restoreRunning(tasks []*model.Task) (pending []*model.Task) {
	holder, ok := q.leaseHolder()
	if !ok {
		return tasks
	}
	var running []runningTask
	for _, task := range tasks {
		if task.AgentID == 0 {
			pending = append(pending, task)
			continue
		}
		restored, keep := q.runningTask(task)
		if !keep {
			continue
		}
		running = append(running, restored)
		log.Info().Str("task_id", task.ID).Int64("agent_id", task.AgentID).Msg("restore queue item: held running again")
	}
	holder.restoreRunning(running)
	return pending
}

// runningTask is the stored task handed out to an agent to hold running
// again; not if its workflow ended or is missing, then removed from the
// store. Its config missing, it is adopted.
func (q *persistentQueue) runningTask(task *model.Task) (runningTask, bool) {
	restored := runningTask{task: task, adopted: len(task.Data) == 0}
	workflowID, err := strconv.ParseInt(task.ID, 10, 64)
	if err != nil {
		log.Error().Err(err).Msgf("restore queue item: %s: invalid workflow id", task.ID)
		return restored, true
	}
	workflow, err := q.store.WorkflowLoad(workflowID)
	switch {
	case errors.Is(err, types.ErrRecordNotExist):
		log.Warn().Msgf("restore queue item: %s: workflow missing, removed", task.ID)
		q.deleteTask(task.ID)
		return restored, false
	case err != nil:
		// cannot tell: held, its agent's Done or its lease ends it
		log.Error().Err(err).Msgf("restore queue item: %s: cannot load its workflow", task.ID)
		return restored, true
	case workflow.State.IsDone():
		log.Debug().Msgf("restore queue item: %s: workflow ended, removed", task.ID)
		q.deleteTask(task.ID)
		return restored, false
	}
	restored.canceledAt = q.pipelineEndedAt(task.PipelineID)
	return restored, true
}

// pipelineEndedAt is when the pipeline of id ended, as canceled: now if it
// did not tell; zero if it did not end, or cannot be loaded.
func (q *persistentQueue) pipelineEndedAt(id int64) time.Time {
	if id == 0 {
		return time.Time{}
	}
	pipeline, err := q.store.GetPipeline(id)
	if err != nil {
		log.Error().Err(err).Int64("pipeline_id", id).Msg("restore queue item: cannot load its pipeline")
		return time.Time{}
	}
	if !pipeline.Status.IsDone() {
		return time.Time{}
	}
	if pipeline.Finished == 0 {
		return time.Now()
	}
	return time.Unix(pipeline.Finished, 0)
}

// adoptOrphans holds running, as adopted, the workflows running not stored
// as tasks (stored: the IDs of those that are, listed before): their tasks
// were handed out by a server that did not store them, or were lost. It
// reads all the workflows running, page by page.
func (q *persistentQueue) adoptOrphans(ctx context.Context, stored map[string]struct{}) {
	holder, ok := q.leaseHolder()
	if !ok {
		return
	}
	var after int64
	for {
		workflows, err := q.store.WorkflowsRunning(after, orphansPage)
		if err != nil {
			log.Error().Err(err).Msg("cannot look for orphaned workflows")
			return
		}
		for _, workflow := range workflows {
			if _, ok := stored[strconv.FormatInt(workflow.ID, 10)]; !ok {
				q.adopt(ctx, holder, workflow)
			}
		}
		if len(workflows) < orphansPage {
			return
		}
		after = workflows[len(workflows)-1].ID
	}
}

// adopt stores an orphaned workflow's task, without its config, with the
// agent the workflow runs on, and holds it running: its agent extends it,
// waits on it and reports it done as any, and if it does not, the task is
// not run again but settled, see SetCanceledExpiredHandler. One with no
// agent recorded is held for none (model.IDNotSet), never extended: it
// expires and is settled; stored with agent 0 it would be queued again
// after a restart.
func (q *persistentQueue) adopt(ctx context.Context, holder *fifo, workflow *model.Workflow) {
	id := strconv.FormatInt(workflow.ID, 10)
	agentID := workflow.AgentID
	if agentID == 0 {
		agentID = model.IDNotSet
	}
	task := &model.Task{
		ID:         id,
		PID:        workflow.PID,
		Name:       workflow.Name,
		PipelineID: workflow.PipelineID,
		AgentID:    agentID,
	}
	if pipeline, err := q.store.GetPipeline(workflow.PipelineID); err == nil {
		task.RepoID = pipeline.RepoID
	}
	if err := q.store.TaskInsert(task); err != nil {
		log.Error().Err(err).Str("task_id", id).Msg("cannot store an orphaned workflow's task")
		return
	}
	holder.restoreRunning([]runningTask{{task: task, canceledAt: q.pipelineEndedAt(task.PipelineID), adopted: true}})

	// Its agent's Done may have come before it was held, finding no task to
	// release: released now, with the state it ended with. One coming after
	// finds it held.
	current, err := q.store.WorkflowLoad(workflow.ID)
	if err == nil && current.State.IsDone() {
		q.deleteTask(id)
		if err := q.Queue.Done(ctx, id, current.State); err != nil && !errors.Is(err, ErrNotFound) {
			log.Error().Err(err).Str("task_id", id).Msg("cannot release an adopted workflow that ended")
		}
		return
	}
	log.Warn().Str("task_id", id).Int64("agent_id", agentID).Msg("adopted an orphaned workflow")
}

// watchOrphans adopts the orphaned workflows every orphansCheckInterval,
// until ctx ends. The stored tasks are listed before the workflows running:
// a workflow ends before its task is removed, so one running then has its
// task listed, unless it is an orphan.
func (q *persistentQueue) watchOrphans(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(orphansCheckInterval):
		}
		tasks, err := q.store.TaskList()
		if err != nil {
			log.Error().Err(err).Msg("cannot list the stored tasks to look for orphaned workflows")
			continue
		}
		stored := make(map[string]struct{}, len(tasks))
		for _, task := range tasks {
			stored[task.ID] = struct{}{}
		}
		q.adoptOrphans(ctx, stored)
	}
}

// SetCanceledExpiredHandler sets what settles a task not run again whose
// lease expired, see Queue. Once settled, its task is removed from the
// store, and, if its workflow ended, from the queue, the ones depending on it
// getting the state it ended with.
func (q *persistentQueue) SetCanceledExpiredHandler(handler func(task *model.Task)) {
	q.Queue.SetCanceledExpiredHandler(func(task *model.Task) {
		handler(task)
		q.settled(task)
	})
}

// settled removes a settled task from the store, and, if its workflow ended,
// from the queue, with the state it ended with: an adopted one is held
// running until then.
func (q *persistentQueue) settled(task *model.Task) {
	workflowID, err := strconv.ParseInt(task.ID, 10, 64)
	if err != nil {
		return
	}
	workflow, err := q.store.WorkflowLoad(workflowID)
	if err != nil {
		log.Error().Err(err).Str("task_id", task.ID).Msg("cannot load a settled workflow")
		return
	}
	if !workflow.State.IsDone() {
		return
	}
	q.deleteTask(task.ID)
	if err := q.Queue.Done(context.Background(), task.ID, workflow.State); err != nil && !errors.Is(err, ErrNotFound) {
		log.Error().Err(err).Str("task_id", task.ID).Msg("cannot release a settled workflow from the queue")
	}
}
