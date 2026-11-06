// Copyright 2022 Woodpecker Authors
// Copyright 2021 Informatyka Boguslawski sp. z o.o. sp.k., http://www.ib.pl/
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

package rpc

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	grpc_metadata "google.golang.org/grpc/metadata"

	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/logging"
	"go.woodpecker-ci.org/woodpecker/v3/server/metric"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

type ctxKey struct{}

// agentIDKey is a non imitable context key.
var agentIDKey = &ctxKey{}

// updateAgentLastWorkDelay the delay before the LastWork info should be updated.
const updateAgentLastWorkDelay = time.Minute

type RPC struct {
	scheduler scheduler.Scheduler
	logger    logging.Log
	store     store.Store
	// end follows a workflow's end, its Done's or its settle's.
	end pipeline.WorkflowEnd
	// killedCheckInterval replaces the const of the name when set; only
	// tests set it.
	killedCheckInterval time.Duration
}

// Next blocks until it provides the next workflow to execute.
func (s *RPC) Next(c context.Context, agentFilter rpc.Filter) (*rpc.Workflow, error) {
	if hostname, err := s.getHostnameFromContext(c); err == nil {
		log.Debug().Msgf("agent connected: %s: polling", hostname)
	}

	agent, err := s.getAgentFromContext(c)
	if err != nil {
		return nil, err
	}

	if agent.NoSchedule {
		time.Sleep(1 * time.Second)
		return nil, nil
	}

	agentServerLabels, err := agent.GetServerLabels()
	if err != nil {
		return nil, err
	}

	// enforce labels from server by overwriting agent labels
	if agentFilter.Labels == nil {
		agentFilter.Labels = make(map[string]string)
	}
	maps.Copy(agentFilter.Labels, agentServerLabels)

	log.Trace().Msgf("Agent %s[%d] tries to pull task with labels: %v", agent.Name, agent.ID, agentFilter.Labels)

	rpcWorkflow, err := s.scheduler.Poll(c, agent.ID, agentFilter, func(taskID string) error {
		// a skipped workflow is finalized through the regular Done flow; it
		// was never initialized by an agent, so lock it to this agent first
		// to satisfy the workflow ownership check.
		if err := s.lockAgentToWorkflow(c, agent, taskID); err != nil {
			return err
		}
		return s.Done(c, taskID, rpc.WorkflowState{})
	})
	if err != nil || rpcWorkflow == nil {
		return nil, err
	}

	if err := s.lockAgentToWorkflow(c, agent, rpcWorkflow.ID); err != nil {
		return nil, err
	}

	return rpcWorkflow, nil
}

// Wait blocks until the workflow with the given ID is completed or got canceled.
// Used to let agents wait for cancel signals from server side.
func (s *RPC) Wait(c context.Context, workflowID string) (canceled bool, err error) {
	agent, err := s.getAgentFromContext(c)
	if err != nil {
		return false, err
	}

	if err := s.checkAgentPermissionByWorkflow(c, agent, workflowID, nil, nil); err != nil {
		return false, err
	}

	if err := s.scheduler.Wait(c, workflowID); err != nil {
		if errors.Is(err, queue.ErrCancel) {
			// we explicit send a cancel signal
			log.Debug().Str("workflowID", workflowID).Msg("while waiting the queue reported the workflow as canceled")
			return true, nil
		}
		// unknown error happened
		log.Error().Err(err).Str("workflowID", workflowID).Msg("while waiting the queue returned an unexpected error")
		return false, err
	}

	// workflow finished and on issues appeared
	log.Debug().Str("workflowID", workflowID).Msg("queue reported the workflow as finished")
	return false, nil
}

// killedCheckInterval is how often WaitKilled checks the workflow's
// stored state.
const killedCheckInterval = 15 * time.Second

// WaitKilled blocks, for a workflow its agent was told canceled, until its
// finalizers are to be stopped too: the queue tells, or the workflow is
// stored ended. The queue does not have a task after a server restart: a
// second cancel then kills the workflow in the store only, which this
// checks every killedCheckInterval.
func (s *RPC) WaitKilled(c context.Context, workflowID string) (killed bool, err error) {
	agent, err := s.getAgentFromContext(c)
	if err != nil {
		return false, err
	}

	if err := s.checkAgentPermissionByWorkflow(c, agent, workflowID, nil, nil); err != nil {
		return false, err
	}

	ctx, cancel := context.WithCancelCause(c)
	defer cancel(nil)
	waited := make(chan error, 1)
	go func() { waited <- s.scheduler.WaitKilled(ctx, workflowID) }()

	interval := s.killedCheckInterval
	if interval == 0 {
		interval = killedCheckInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case err := <-waited:
			if errors.Is(err, queue.ErrCancel) {
				log.Debug().Str("workflowID", workflowID).Msg("the queue stops the canceled workflow's finalizers")
				return true, nil
			}
			return false, err
		case <-ticker.C:
			if s.workflowEnded(workflowID) {
				log.Debug().Str("workflowID", workflowID).Msg("the workflow ended: its finalizers stop")
				return true, nil
			}
		}
	}
}

// workflowEnded reports whether the workflow of id is stored ended; not
// when it cannot tell.
func (s *RPC) workflowEnded(id string) bool {
	workflowID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return false
	}
	workflow, err := s.store.WorkflowLoad(workflowID)
	if err != nil {
		log.Error().Err(err).Str("workflowID", id).Msg("cannot load the workflow waiting to be killed")
		return false
	}
	return workflow.State.IsDone()
}

// Extend extends the lease for the workflow with the given ID.
func (s *RPC) Extend(c context.Context, workflowID string) error {
	agent, err := s.getAgentFromContext(c)
	if err != nil {
		return err
	}

	err = s.updateAgentLastWork(agent)
	if err != nil {
		return err
	}

	if err := s.checkAgentPermissionByWorkflow(c, agent, workflowID, nil, nil); err != nil {
		return err
	}

	return s.scheduler.Extend(c, agent.ID, workflowID)
}

// ExtendFinalizing extends the lease for the workflow with the given ID, its
// agent telling when its finalizers' time ends.
func (s *RPC) ExtendFinalizing(c context.Context, workflowID string, finalizeDeadline time.Time) error {
	agent, err := s.getAgentFromContext(c)
	if err != nil {
		return err
	}

	err = s.updateAgentLastWork(agent)
	if err != nil {
		return err
	}

	if err := s.checkAgentPermissionByWorkflow(c, agent, workflowID, nil, nil); err != nil {
		return err
	}

	return s.scheduler.ExtendFinalizing(c, agent.ID, workflowID, finalizeDeadline)
}

// Update let agent updates the step state at the server.
func (s *RPC) Update(c context.Context, strWorkflowID string, state rpc.StepState) error {
	workflowID, err := strconv.ParseInt(strWorkflowID, 10, 64)
	if err != nil {
		return err
	}

	workflow, err := s.store.WorkflowLoad(workflowID)
	if err != nil {
		log.Error().Err(err).Msgf("rpc.update: cannot find workflow with id %d", workflowID)
		return err
	}

	currentPipeline, err := s.store.GetPipeline(workflow.PipelineID)
	if err != nil {
		log.Error().Err(err).Msgf("cannot find pipeline with id %d", workflow.PipelineID)
		return err
	}

	agent, err := s.getAgentFromContext(c)
	if err != nil {
		return err
	}

	step, err := s.store.StepByUUID(state.StepUUID)
	if err != nil {
		log.Error().Err(err).Msgf("cannot find step with uuid %s", state.StepUUID)
		return err
	}

	if step.PipelineID != currentPipeline.ID {
		msg := fmt.Sprintf("agent returned status with step uuid '%s' which does not belong to current pipeline", state.StepUUID)
		log.Error().
			Int64("stepPipelineID", step.PipelineID).
			Int64("currentPipelineID", currentPipeline.ID).
			Msg(msg)
		return errors.New(msg)
	}

	repo, err := s.store.GetRepo(currentPipeline.RepoID)
	if err != nil {
		log.Error().Err(err).Msgf("cannot find repo with id %d", currentPipeline.RepoID)
		return err
	}

	// check before agent can alter some state
	if err := s.checkAgentPermissionByWorkflow(c, agent, strWorkflowID, currentPipeline, repo); err != nil {
		return err
	}

	// sanitize agent input: reject states no compatible agent can produce
	if err := checkAgentReportedStepState(agent.ID, state); err != nil {
		return err
	}

	// sanitize agent input: only allow step updates that the workflow state permits
	if err := checkWorkflowAllowsStepUpdate(workflow.State, step, state); err != nil {
		return err
	}

	if err := pipeline.UpdateStepStatus(c, s.store, s.end, step, state); err != nil {
		log.Error().Err(err).Msg("rpc.update: cannot update step")
	}

	if metric.FailurePipelineStepInfoCount != nil &&
		state.Exited &&
		step.State.IsFailure() {
		metric.FailurePipelineStepInfoCount.WithLabelValues(workflow.Name, repo.FullName, step.Name, string(step.Type)).Inc()
	}

	if metric.StepDurationRecord != nil && state.Exited && step.Started > 0 && step.Finished >= step.Started {
		duration := step.Finished - step.Started
		metric.StepDurationRecord.WithLabelValues(
			workflow.Name,
			repo.FullName,
			step.Name,
			string(step.Type),
		).Observe(float64(duration))
	}
	if state.Exited {
		if err := server.Config.Services.LogStore.StepFinished(c, step); err != nil {
			log.Error().Err(err).Int64("step", step.ID).Msg("log store could not finalize the step log")
		}
	}

	if currentPipeline.Workflows, err = s.store.WorkflowGetTree(currentPipeline); err != nil {
		log.Error().Err(err).Msg("cannot build tree from step list")
		return err
	}

	return s.scheduler.PublishPipelineEvent(c, repo, currentPipeline)
}

// Init signals the workflow is initialized.
func (s *RPC) Init(c context.Context, strWorkflowID string, state rpc.WorkflowState) error {
	workflowID, err := strconv.ParseInt(strWorkflowID, 10, 64)
	if err != nil {
		return err
	}

	workflow, err := s.store.WorkflowLoad(workflowID)
	if err != nil {
		log.Error().Err(err).Msgf("cannot find workflow with id %d", workflowID)
		return err
	}

	agent, err := s.getAgentFromContext(c)
	if err != nil {
		return err
	}

	currentPipeline, err := s.store.GetPipeline(workflow.PipelineID)
	if err != nil {
		log.Error().Err(err).Msgf("cannot find pipeline with id %d", workflow.PipelineID)
		return err
	}

	repo, err := s.store.GetRepo(currentPipeline.RepoID)
	if err != nil {
		log.Error().Err(err).Msgf("cannot find repo with id %d", currentPipeline.RepoID)
		return err
	}

	// check before agent can alter some state
	if err := s.checkAgentPermissionByWorkflow(c, agent, strWorkflowID, currentPipeline, repo); err != nil {
		return err
	}

	// check workflow's own state to prevent re-initializing a finished or blocked workflow
	if err := checkWorkflowState(workflow.State); err != nil {
		return err
	}

	// sanitize agent input: reject states no compatible agent can produce
	if err := checkAgentReportedInitState(agent.ID, state); err != nil {
		return err
	}

	if currentPipeline.Status == model.StatusPending {
		// only a pending one: a cancel claimed meanwhile stays
		started, won, err := pipeline.UpdateToStatusRunning(s.store, *currentPipeline, state.Started)
		switch {
		case err != nil:
			log.Error().Err(err).Msgf("init: cannot update pipeline %d state", currentPipeline.ID)
		case won:
			currentPipeline = started
		default:
			if latest, err := s.store.GetPipeline(currentPipeline.ID); err == nil {
				currentPipeline = latest
			}
		}
	}

	event := currentPipeline.Event
	defer func() {
		s.end.MarkStatuses(event, workflow.PipelineID, workflow.ID)
		currentPipeline.Workflows, _ = s.store.WorkflowGetTree(currentPipeline)

		if err := s.scheduler.PublishPipelineEvent(c, repo, currentPipeline); err != nil {
			log.Error().Err(err).Msg("could not publish pipeline state change to pubsub")
		}
	}()

	// only a pending one: an Init retried, or a cancel that skipped it
	// meanwhile, leaves it as it is
	if _, won, err := pipeline.UpdateWorkflowStatusToRunning(s.store, *workflow, state); err != nil {
		return err
	} else if !won {
		log.Debug().Msgf("init: workflow %d is not pending any more", workflow.ID)
	}

	return s.updateAgentLastWork(agent)
}

// Done marks the workflow with the given ID as stopped.
func (s *RPC) Done(c context.Context, strWorkflowID string, state rpc.WorkflowState) error {
	workflowID, err := strconv.ParseInt(strWorkflowID, 10, 64)
	if err != nil {
		return err
	}

	workflow, err := s.store.WorkflowLoad(workflowID)
	if err != nil {
		log.Error().Err(err).Msgf("cannot find workflow with id %d", workflowID)
		return err
	}

	workflow.Children, err = s.store.StepListFromWorkflowFind(workflow)
	if err != nil {
		return err
	}

	currentPipeline, err := s.store.GetPipeline(workflow.PipelineID)
	if err != nil {
		log.Error().Err(err).Msgf("cannot find pipeline with id %d", workflow.PipelineID)
		return err
	}

	repo, err := s.store.GetRepo(currentPipeline.RepoID)
	if err != nil {
		log.Error().Err(err).Msgf("cannot find repo with id %d", currentPipeline.RepoID)
		return err
	}

	agent, err := s.getAgentFromContext(c)
	if err != nil {
		return err
	}

	// check before agent can alter some state
	if err := s.checkAgentPermissionByWorkflow(c, agent, strWorkflowID, currentPipeline, repo); err != nil {
		return err
	}

	// check workflow's own state to prevent finishing an already-finished or blocked workflow
	if err := checkWorkflowState(workflow.State); err != nil {
		s.releaseEnded(c, strWorkflowID, workflow.State)
		return err
	}

	// sanitize agent input: reject states no compatible agent can produce
	if err := checkAgentReportedDoneState(agent.ID, state); err != nil {
		return err
	}

	logger := log.With().
		Str("repo_id", fmt.Sprint(repo.ID)).
		Str("pipeline_id", fmt.Sprint(currentPipeline.ID)).
		Str("workflow_id", strWorkflowID).Logger()

	logger.Debug().Msgf("workflow state in store: %#v", workflow)
	logger.Debug().Msgf("gRPC Done with state: %#v", state)

	// End any still-running children (e.g. service containers) before
	// computing the workflow status, so their final state is reflected;
	// they are written once the workflow's end is.
	ended := s.completeChildrenIfParentCompleted(workflow, state)

	if workflow, err = pipeline.UpdateWorkflowStatusToDone(s.store, *workflow, state); err != nil {
		if errors.Is(err, pipeline.ErrWorkflowEnded) {
			// settled or swept meanwhile: that end stands
			logger.Warn().Msg("done: the workflow ended already")
			if stored, err := s.store.WorkflowLoad(workflowID); err == nil {
				s.releaseEnded(c, strWorkflowID, stored.State)
			}
			return nil
		}
		// not written: the agent tries again, the compare-and-set making that
		// safe; nothing else is written, the queue keeps it
		logger.Error().Err(err).Msgf("pipeline.UpdateWorkflowStatusToDone: cannot update workflow state: %s", err)
		return err
	}
	s.storeSteps(ended)

	// The queue gets the status the workflow was stored with: the workflows
	// depending on it run or not by it.
	var queueErr error
	if workflow.State == model.StatusFailure && state.Error != "" {
		queueErr = s.scheduler.Error(c, strWorkflowID, fmt.Errorf("workflow finished with error %s", state.Error))
	} else {
		queueErr = s.scheduler.Done(c, strWorkflowID, workflow.State)
	}
	if queueErr != nil {
		logger.Error().Err(queueErr).Msg("queue.Done: cannot ack workflow")
	}

	if err := s.end.Ended(c, logger, repo, currentPipeline, workflow); err != nil {
		return err
	}

	return s.updateAgentLastWork(agent)
}

// Log writes a log entry to the database and publishes it to the pubsub.
// An explicit stepUUID makes it obvious that all entries must come from the same step.
func (s *RPC) Log(c context.Context, stepUUID string, rpcLogEntries []*rpc.LogEntry) error {
	step, err := s.store.StepByUUID(stepUUID)
	if err != nil {
		return fmt.Errorf("could not find step with uuid %s in store: %w", stepUUID, err)
	}

	agent, err := s.getAgentFromContext(c)
	if err != nil {
		return err
	}

	currentPipeline, err := s.store.GetPipeline(step.PipelineID)
	if err != nil {
		log.Error().Err(err).Msgf("cannot find pipeline with id %d", step.PipelineID)
		return err
	}

	workflow, err := s.store.WorkflowByStep(step)
	if err != nil {
		log.Error().Err(err).Msgf("cannot find workflow for step uuid %s", stepUUID)
		return err
	}

	// check before agent can alter some state
	if err := s.checkAgentPermissionByWorkflow(c, agent, strconv.FormatInt(workflow.ID, 10), currentPipeline, nil); err != nil {
		return err
	}

	// sanitize agent input
	if err := allowAppendingLogs(currentPipeline, step); err != nil {
		return fmt.Errorf("can not alter logs: %w", err)
	}

	err = s.updateAgentLastWork(agent)
	if err != nil {
		return err
	}

	var logEntries []*model.LogEntry

	for _, rpcLogEntry := range rpcLogEntries {
		if rpcLogEntry.StepUUID != stepUUID {
			return fmt.Errorf("expected step UUID %s, got %s", stepUUID, rpcLogEntry.StepUUID)
		}
		logEntries = append(logEntries, &model.LogEntry{
			StepID: step.ID,
			Time:   rpcLogEntry.Time,
			Line:   rpcLogEntry.Line,
			Data:   rpcLogEntry.Data,
			Type:   model.LogEntryType(rpcLogEntry.Type),
		})
	}

	// make sure writes to pubsub are non blocking (https://github.com/woodpecker-ci/woodpecker/blob/c919f32e0b6432a95e1a6d3d0ad662f591adf73f/server/logging/log.go#L9)
	go func() {
		// write line to listening web clients
		if err := s.logger.Write(c, step.ID, logEntries); err != nil {
			log.Error().Err(err).Msgf("rpc server could not write to logger")
		}
	}()

	if err = server.Config.Services.LogStore.LogAppend(c, step, logEntries); err != nil {
		log.Error().Err(err).Msg("could not store log entries")
	}

	return nil
}

func (s *RPC) RegisterAgent(ctx context.Context, info rpc.AgentInfo) (int64, error) {
	agent, err := s.getAgentFromContext(ctx)
	if err != nil {
		return -1, err
	}

	if agent.Name == "" {
		if hostname, err := s.getHostnameFromContext(ctx); err == nil {
			agent.Name = hostname
		}
	}

	agent.Backend = info.Backend
	agent.Platform = info.Platform
	agent.Capacity = int32(info.Capacity)
	agent.Version = info.Version
	agent.CustomLabels = info.CustomLabels

	err = s.store.AgentUpdate(agent)
	if err != nil {
		return -1, err
	}

	return agent.ID, nil
}

// UnregisterAgent removes the agent from the database.
func (s *RPC) UnregisterAgent(ctx context.Context) error {
	agent, err := s.getAgentFromContext(ctx)
	if !agent.IsSystemAgent() {
		// registered with individual agent token -> do not unregister
		return nil
	}
	log.Debug().Msgf("un-registering agent with ID %d", agent.ID)
	if err != nil {
		return err
	}

	err = s.store.AgentDelete(agent)

	return err
}

func (s *RPC) ReportHealth(ctx context.Context, status string) error {
	agent, err := s.getAgentFromContext(ctx)
	if err != nil {
		return err
	}

	if status != "I am alive!" {
		//nolint:staticcheck
		return errors.New("Are you alive?")
	}

	agent.LastContact = time.Now().Unix()

	return s.store.AgentUpdate(agent)
}

// completeChildrenIfParentCompleted ends, in memory, the steps of a done
// workflow still pending or running, and returns them to be written once
// the workflow's end is. A step the workflow's end stopped was killed (a
// service: success); one that never started when the workflow was canceled
// or timed out is canceled: it was not killed by anyone.
func (s *RPC) completeChildrenIfParentCompleted(completedWorkflow *model.Workflow, state rpc.WorkflowState) []*model.Step {
	var ended []*model.Step
	for _, c := range completedWorkflow.Children {
		if !c.Running() {
			continue
		}
		end := pipeline.StepEndWorkflowDone
		if state.Canceled || state.TimedOut {
			end = pipeline.StepEndWorkflowStopped
		}
		c.State = pipeline.StepEndState(c, end)
		if c.Started != 0 {
			c.Finished = state.Finished
		}
		ended = append(ended, c)
	}
	return ended
}

// releaseEnded drops from the queue a workflow its agent reports done that
// ended already (settled, swept), with the state it ended with, so that its
// agent does not hold it any more; not one blocked, which never ran.
func (s *RPC) releaseEnded(c context.Context, workflowID string, state model.StatusValue) {
	if state == model.StatusBlocked {
		return
	}
	if err := s.scheduler.Done(c, workflowID, state); err != nil && !errors.Is(err, queue.ErrNotFound) {
		log.Error().Err(err).Str("workflow_id", workflowID).Msg("cannot release an ended workflow from the queue")
	}
}

// storeSteps writes the ends of steps not ended meanwhile, logging what it
// cannot.
func (s *RPC) storeSteps(steps []*model.Step) {
	for _, step := range steps {
		if _, err := s.store.StepFinish(step, model.StatusPending, model.StatusRunning); err != nil {
			log.Error().Err(err).Msgf("done: cannot update step_id %d child state", step.ID)
		}
	}
}

func (s *RPC) getAgentFromContext(ctx context.Context) (*model.Agent, error) {
	rawAgentID := ctx.Value(agentIDKey)
	if rawAgentID == nil {
		return nil, errors.New("agent_id is not provided")
	}
	agentID, ok := rawAgentID.(int64)
	if !ok {
		return nil, errors.New("agent_id is not a valid integer")
	}

	return s.store.AgentFind(agentID)
}

func (s *RPC) getHostnameFromContext(ctx context.Context) (string, error) {
	metadata, ok := grpc_metadata.FromIncomingContext(ctx)
	if ok {
		hostname, ok := metadata["hostname"]
		if ok && len(hostname) != 0 {
			return hostname[0], nil
		}
	}
	return "", errors.New("no hostname in metadata")
}

func (s *RPC) updateAgentLastWork(agent *model.Agent) error {
	// only update agent.LastWork if not recently updated
	if time.Unix(agent.LastWork, 0).Add(updateAgentLastWorkDelay).After(time.Now()) {
		return nil
	}

	agent.LastWork = time.Now().Unix()
	if err := s.store.AgentUpdate(agent); err != nil {
		return err
	}

	return nil
}
