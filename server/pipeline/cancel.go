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

package pipeline

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// Cancel the pipeline and returns the status.
//
// The cancel is claimed first: the pipeline is written killed (canceled if
// nothing of it ran, killed after all if the queue reports a workflow its
// agent polled) only if it still is pending, running or blocked. Of
// two cancels, or a cancel and the pipeline's end, one wins; the other does
// nothing more. A workflow its agent runs is stopped by it, its finalizers
// may still run: it stays running until the agent reports it done (or its
// lease is over), then its own end is written, the pipeline's stays. A
// running workflow the queue has no task of has no agent to stop it: it is
// killed at once.
//
// Canceling a killed pipeline whose workflows still run their finalizers
// stops those too. Only a user's cancel does that: the cancel of previous
// pipelines and a step's failure: cancel never do.
//
// The end, the server's, follows the end of the workflows it kills.
func Cancel(ctx context.Context, _forge forge.Forge, store store.Store, end WorkflowEnd, repo *model.Repo, user *model.User, pipeline *model.Pipeline, cancelInfo *model.CancelInfo) error {
	if end.Store == nil {
		return errNoWorkflowEnd
	}
	workflows, err := store.WorkflowGetTree(pipeline)
	if err != nil {
		return &ErrNotFound{Msg: err.Error()}
	}

	if !pipelineCancelable(pipeline) {
		if byUser(cancelInfo) && pipeline.Status == model.StatusKilled && pipeline.CancelInfo != nil && model.IsThereRunningStage(workflows) {
			return stopFinalizers(ctx, store, end, repo, pipeline, workflows)
		}
		return &ErrBadRequest{Msg: "Cannot cancel a non-running or non-pending or non-blocked pipeline"}
	}

	hasPendingOnly := true
	for _, workflow := range workflows {
		if workflow.State != model.StatusPending {
			hasPendingOnly = false
		}
	}
	killed := *pipeline
	killed.Status = model.StatusKilled
	if hasPendingOnly {
		killed.Status = model.StatusCanceled
	}
	killed.Finished = time.Now().Unix()
	killed.CancelInfo = cancelInfo
	won, err := store.PipelineCancel(&killed)
	if err != nil {
		log.Error().Err(err).Msgf("cannot cancel pipeline %d", pipeline.ID)
		return err
	}
	if !won {
		// canceled or ended meanwhile: that stands
		return nil
	}

	// Then cancel/evict the running and pending workflows from the queue:
	// the ones it reports canceling run on for their finalizers, whatever
	// the tree read above says (a workflow its agent polled may not be
	// initialized yet)
	var workflowsToCancel []string
	for _, w := range workflows {
		if w.State == model.StatusRunning || w.State == model.StatusPending {
			workflowsToCancel = append(workflowsToCancel, fmt.Sprint(w.ID))
		}
	}
	canceling, err := server.Config.Services.Scheduler.CancelWorkflows(ctx, workflowsToCancel, false)
	if err != nil {
		log.Error().Err(err).Msgf("cancel workflows: %v", workflowsToCancel)
	}
	if killed.Status == model.StatusCanceled && len(canceling) > 0 {
		// a workflow its agent polled runs its finalizers: the pipeline ran
		killed.Status = model.StatusKilled
		if _, err := store.PipelineCanceledKilled(&killed); err != nil {
			log.Error().Err(err).Msgf("cannot write pipeline %d killed", pipeline.ID)
		}
	}

	// The tree was read before the agents were told: the steps and
	// workflows below are written only if still as read (an agent may have
	// started or ended them meanwhile), and only their state and end.
	for _, workflow := range workflows {
		agentStops := slices.Contains(canceling, fmt.Sprint(workflow.ID))
		// the steps that did not start of a pending workflow, and of one its
		// agent stops but its finalizers, which it runs (a running one no agent
		// stops is killed with its steps below)
		for _, step := range workflow.Children {
			if step.State == model.StatusPending && (agentStops && !step.Finalizer || !agentStops && workflow.State == model.StatusPending) {
				if _, _, err = UpdateStepToStatusSkipped(store, *step, 0, model.StatusCanceled); err != nil {
					log.Error().Err(err).Msgf("cannot update workflow with id %d state", workflow.ID)
				}
			}
		}
		switch {
		case agentStops:
		case workflow.State == model.StatusPending:
			if _, _, err = UpdateWorkflowToStatusSkipped(store, *workflow); err != nil {
				log.Error().Err(err).Msgf("cannot update workflow with id %d state", workflow.ID)
			}
		case workflow.State == model.StatusRunning:
			// no agent holds it any more
			if err := end.KillAndEnd(ctx, log.Logger, workflow); err != nil {
				log.Error().Err(err).Msgf("cannot kill workflow with id %d", workflow.ID)
			}
		}
	}

	updatePipelineStatus(&killed)

	return publishCanceled(ctx, store, repo, &killed)
}

// byUser reports whether a cancel is a user's.
func byUser(cancelInfo *model.CancelInfo) bool {
	return cancelInfo != nil && cancelInfo.CanceledByUser != ""
}

// stopFinalizers is the second cancel of a killed pipeline: the workflows
// still running their finalizers are told to stop them too, the ones no
// agent holds any more are killed.
func stopFinalizers(ctx context.Context, store store.Store, end WorkflowEnd, repo *model.Repo, pipeline *model.Pipeline, workflows []*model.Workflow) error {
	var running []string
	for _, workflow := range workflows {
		if workflow.Running() {
			running = append(running, fmt.Sprint(workflow.ID))
		}
	}
	canceling, err := server.Config.Services.Scheduler.CancelWorkflows(ctx, running, true)
	if err != nil {
		log.Error().Err(err).Msgf("stop the finalizers of workflows: %v", running)
	}
	for _, workflow := range workflows {
		if workflow.Running() && !slices.Contains(canceling, fmt.Sprint(workflow.ID)) {
			if err := end.KillAndEnd(ctx, log.Logger, workflow); err != nil {
				log.Error().Err(err).Msgf("cannot kill workflow with id %d", workflow.ID)
			}
		}
	}
	return publishCanceled(ctx, store, repo, pipeline)
}

// publishCanceled tells the subscribers of a pipeline canceled.
func publishCanceled(ctx context.Context, store store.Store, repo *model.Repo, pipeline *model.Pipeline) error {
	var err error
	if pipeline.Workflows, err = store.WorkflowGetTree(pipeline); err != nil {
		return err
	}
	if err := server.Config.Services.Scheduler.PublishPipelineEvent(ctx, repo, pipeline); err != nil {
		log.Error().Err(err).Msg("could not push pipeline status change to pubsub provider")
	}
	return nil
}

func cancelPreviousPipelines(
	ctx context.Context,
	_forge forge.Forge,
	_store store.Store,
	pipeline *model.Pipeline,
	repo *model.Repo,
	user *model.User,
) error {
	// check this event should cancel previous pipelines
	eventIncluded := slices.Contains(repo.CancelPreviousPipelineEvents, pipeline.Event)
	if !eventIncluded {
		return nil
	}

	// get all active activeBuilds
	activeBuilds, err := _store.GetActivePipelineList(repo)
	if err != nil {
		return err
	}

	pipelineNeedsCancel := func(active *model.Pipeline) bool {
		// always filter on same event
		if active.Event != pipeline.Event {
			return false
		}

		// find events for the same context
		switch pipeline.Event {
		case model.EventPush:
			return pipeline.Branch == active.Branch
		default:
			return pipeline.Refspec == active.Refspec
		}
	}

	for _, active := range activeBuilds {
		if active.ID == pipeline.ID {
			// same pipeline. e.g. self
			continue
		}
		// canceled already: canceling it again would stop its finalizers
		if active.CancelInfo != nil {
			continue
		}

		cancel := pipelineNeedsCancel(active)

		if !cancel {
			continue
		}

		// the server's end, as the request, webhook or cron starting the
		// pipeline carries it
		end, ok := WorkflowEndFromContext(ctx)
		if !ok {
			log.Error().Err(errNoWorkflowEnd).Int64("id", active.ID).Msg("cannot cancel a previous pipeline")
			continue
		}
		if err = Cancel(ctx, _forge, _store, end, repo, user, active, &model.CancelInfo{
			SupersededBy: pipeline.Number,
		}); err != nil {
			log.Error().
				Err(err).
				Str("ref", active.Ref).
				Int64("id", active.ID).
				Msg("failed to cancel pipeline")
		}
	}

	return nil
}

// pipelineCancelable reports whether a pipeline can still be canceled: it is
// running, pending or blocked.
func pipelineCancelable(pipeline *model.Pipeline) bool {
	return pipeline.Status == model.StatusRunning || pipeline.Status == model.StatusPending || pipeline.Status == model.StatusBlocked
}
