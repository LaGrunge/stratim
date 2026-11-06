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

package rpc

import (
	"context"
	"strconv"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// settleCanceledWorkflow ends a canceled workflow whose lease expired before
// its agent reported it done, as the agent is gone: it ends killed (see
// pipeline.KillWorkflow), unless its Done came meanwhile, and what follows
// a workflow's end, as after Done, its pipeline staying killed.
func (s *RPC) settleCanceledWorkflow(task *model.Task) {
	ctx := context.Background()
	logger := log.With().Str("workflow_id", task.ID).Int64("pipeline_id", task.PipelineID).Logger()

	workflowID, err := strconv.ParseInt(task.ID, 10, 64)
	if err != nil {
		logger.Error().Err(err).Msg("settle canceled workflow: invalid workflow id")
		return
	}
	workflow, err := s.store.WorkflowLoad(workflowID)
	if err != nil {
		logger.Error().Err(err).Msg("settle canceled workflow: cannot find it")
		return
	}
	if workflow.Children, err = s.store.StepListFromWorkflowFind(workflow); err != nil {
		logger.Error().Err(err).Msg("settle canceled workflow: cannot load its steps")
		return
	}
	if err := s.end.KillAndEnd(ctx, logger, workflow); err != nil {
		logger.Error().Err(err).Msg("settle canceled workflow: cannot kill it")
	}
}
