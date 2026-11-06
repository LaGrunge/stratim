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

package pipeline

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog"

	"go.woodpecker-ci.org/woodpecker/v3/server/logging"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// WorkflowEndMetrics counts what a workflow's end ends.
type WorkflowEndMetrics interface {
	// PipelineEnded counts a pipeline ended now, by its last workflow.
	PipelineEnded(repo *model.Repo, pipeline *model.Pipeline)
	// WorkflowEnded counts a workflow ended.
	WorkflowEnded(repo *model.Repo, pipeline *model.Pipeline, workflow *model.Workflow)
}

// WorkflowEnd is what follows the end of a workflow, once written, whoever
// ended it (its agent's Done, its settle, a cancel, the sweep): the end of
// its pipeline when it was the last one running, unless that ended already
// (a cancel kills a pipeline at once), the forge's status, its steps' log
// streams closed, the pipeline's subscribers told and the metrics.
type WorkflowEnd struct {
	Store     store.Store
	Publisher interface {
		PublishPipelineEvent(c context.Context, repo *model.Repo, pipeline *model.Pipeline) error
	}
	// Logs, Statuses and Metrics are left out when nil.
	Logs     logging.Log
	Statuses WorkflowStatuses
	Metrics  WorkflowEndMetrics
}

// WorkflowStatuses reports statuses to the forge.
type WorkflowStatuses interface {
	// Pipeline marks every workflow of the pipeline.
	Pipeline(pipelineID int64)
	// Workflow marks one workflow of the pipeline.
	Workflow(pipelineID, workflowID int64)
}

// MarkStatuses has the status of the workflow sent to its forge; of every
// workflow of the pipeline for a deployment, whose status they show.
// Nothing without Statuses.
func (e WorkflowEnd) MarkStatuses(event model.WebhookEvent, pipelineID, workflowID int64) {
	switch {
	case e.Statuses == nil:
	case event == model.EventDeploy:
		e.Statuses.Pipeline(pipelineID)
	default:
		e.Statuses.Workflow(pipelineID, workflowID)
	}
}

// WorkflowEndKey is the key of the server's WorkflowEnd in a context, as
// the store's: the API's requests get it by a middleware.
const WorkflowEndKey = "workflow-end"

// InjectWorkflowEnd returns ctx carrying end.
func InjectWorkflowEnd(ctx context.Context, end WorkflowEnd) context.Context {
	return context.WithValue(ctx, WorkflowEndKey, end) //nolint:staticcheck
}

// WorkflowEndFromContext returns the WorkflowEnd ctx carries, if any.
func WorkflowEndFromContext(ctx context.Context) (WorkflowEnd, bool) {
	end, ok := ctx.Value(WorkflowEndKey).(WorkflowEnd)
	return end, ok
}

// errNoWorkflowEnd is a cancel given no WorkflowEnd to end workflows with.
var errNoWorkflowEnd = errors.New("pipeline: no workflow end to cancel with")

// Ended follows the end of workflow, of currentPipeline.
func (e WorkflowEnd) Ended(c context.Context, logger zerolog.Logger, repo *model.Repo, currentPipeline *model.Pipeline, workflow *model.Workflow) error {
	event := currentPipeline.Event
	var err error
	currentPipeline.Workflows, err = e.Store.WorkflowGetTree(currentPipeline)
	if err != nil {
		return err
	}

	pipelineEnded := false
	if !model.IsThereRunningStage(currentPipeline.Workflows) {
		// use the latest finish time of all workflows
		finished := workflow.Finished
		for _, w := range currentPipeline.Workflows {
			finished = max(finished, w.Finished)
		}
		if finished == 0 {
			finished = time.Now().Unix()
		}

		ended := *currentPipeline
		ended.Status = PipelineStatus(currentPipeline.Workflows)
		ended.Finished = finished
		// only status and finish, and only if not ended already: a pipeline
		// a cancel killed stays killed
		if pipelineEnded, err = e.Store.PipelineFinish(&ended); err != nil {
			logger.Error().Err(err).Msg("cannot update the pipeline's final state")
		}
		if pipelineEnded {
			currentPipeline = &ended
		} else if latest, err := e.Store.GetPipeline(currentPipeline.ID); err == nil {
			latest.Workflows = currentPipeline.Workflows
			currentPipeline = latest
		}
	}

	e.MarkStatuses(event, workflow.PipelineID, workflow.ID)

	if e.Logs != nil {
		// make sure writes to pubsub are non blocking (https://github.com/woodpecker-ci/woodpecker/blob/c919f32e0b6432a95e1a6d3d0ad662f591adf73f/server/logging/log.go#L9)
		go func() {
			for _, step := range workflow.Children {
				if step.State == model.StatusSkipped {
					continue
				}
				if err := e.Logs.Close(c, step.ID); err != nil {
					// A step killed before it ran never opened a stream.
					if errors.Is(err, logging.ErrNotFound) {
						logger.Debug().Err(err).Msgf("no log stream to close for step %d", step.ID)
					} else {
						logger.Error().Err(err).Msgf("cannot close log stream for step %d", step.ID)
					}
				}
			}
		}()
	}

	if err := e.Publisher.PublishPipelineEvent(c, repo, currentPipeline); err != nil {
		return err
	}

	if e.Metrics != nil {
		if pipelineEnded {
			e.Metrics.PipelineEnded(repo, currentPipeline)
		}
		e.Metrics.WorkflowEnded(repo, currentPipeline, workflow)
	}
	return nil
}

// KillAndEnd kills a running workflow no agent stops any more (see
// KillWorkflow) and, if it did, follows its end.
func (e WorkflowEnd) KillAndEnd(c context.Context, logger zerolog.Logger, workflow *model.Workflow) error {
	won, err := KillWorkflow(e.Store, workflow)
	if err != nil || !won {
		return err
	}
	pipeline, err := e.Store.GetPipeline(workflow.PipelineID)
	if err != nil {
		return err
	}
	repo, err := e.Store.GetRepo(pipeline.RepoID)
	if err != nil {
		return err
	}
	return e.Ended(c, logger, repo, pipeline, workflow)
}
