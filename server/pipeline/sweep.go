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
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/compiler"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
)

// CanceledWorkflowsBound is how long after its cancel a workflow left
// running for its finalizers may run: longer than the queue holds one with
// the longest finalizers' time, and a margin for the time between the
// pipeline's cancel being written and the queue told. After a server
// restart, which loses the queue's running tasks, the agent may still
// report it done until then.
var CanceledWorkflowsBound = queue.MaxCanceledHold(compiler.MaxFinalizeTimeout) + 5*time.Minute

// SweepInterval is how often the sweep of canceled workflows runs.
const SweepInterval = time.Minute

// SweepBatch is how many workflows a sweep ends at most: a backlog is
// ended over several, their forge statuses and events not all at once.
const SweepBatch = 100

// SweepCanceledWorkflows ends, killed, the workflows still pending or
// running in pipelines a cancel killed or canceled longer than
// CanceledWorkflowsBound before now, at most SweepBatch of them: no agent's
// report or lease ends them any more, as after a server restart. Their
// ends follow as any workflow's, their pipelines staying as they are.
func SweepCanceledWorkflows(ctx context.Context, end WorkflowEnd, now time.Time) error {
	workflows, err := end.Store.WorkflowsLeftInCanceledPipelines(now.Add(-CanceledWorkflowsBound).Unix(), SweepBatch)
	if err != nil {
		return err
	}
	for _, workflow := range workflows {
		logger := log.With().Int64("workflow_id", workflow.ID).Logger()
		if err := end.KillAndEnd(ctx, logger, workflow); err != nil {
			logger.Error().Err(err).Msg("cannot kill a workflow of a canceled pipeline")
			continue
		}
		logger.Info().Msg("killed a workflow of a canceled pipeline no agent ended")
	}
	return nil
}

// RunCanceledWorkflowsSweep sweeps the canceled workflows now and then every
// interval, until ctx ends.
func RunCanceledWorkflowsSweep(ctx context.Context, end WorkflowEnd, interval time.Duration) {
	for {
		if err := SweepCanceledWorkflows(ctx, end, time.Now()); err != nil {
			log.Error().Err(err).Msg("cannot sweep the workflows of canceled pipelines")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
