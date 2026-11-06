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
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// StepEnd is how the workflow of a step left pending or running ended.
type StepEnd int

const (
	// StepEndWorkflowDone is its agent reporting it done by itself: a step
	// still running was a service the end stopped, one never started was
	// killed.
	StepEndWorkflowDone StepEnd = iota
	// StepEndWorkflowStopped is its agent reporting it canceled or timed
	// out: a step still running was a service, one never started canceled.
	StepEndWorkflowStopped
	// StepEndKilled is the server killing it, no agent stopping it any
	// more: a step running is killed, one never started canceled.
	StepEndKilled
)

// StepEndState is the state a step left pending or running ends with, its
// workflow ending so.
func StepEndState(step *model.Step, end StepEnd) model.StatusValue {
	started := step.Started != 0
	switch {
	case end == StepEndKilled && started:
		return model.StatusKilled
	case started:
		return model.StatusSuccess // for daemons that are killed
	case end == StepEndWorkflowDone:
		return model.StatusKilled
	default:
		return model.StatusCanceled
	}
}

// KillWorkflow ends a running workflow no agent stops any more, killed: its
// steps that never started end canceled, the ones running killed. The end
// is written only if the workflow did not end meanwhile (its agent's Done
// may come at the same time): it reports whether it was, and writes its
// steps only then.
func KillWorkflow(store store.Store, workflow *model.Workflow) (bool, error) {
	now := time.Now().Unix()
	ended := *workflow
	ended.State = model.StatusKilled
	ended.Finished = now
	won, err := store.WorkflowFinish(&ended)
	if err != nil || !won {
		return false, err
	}
	*workflow = ended

	for _, step := range workflow.Children {
		if !step.Running() {
			continue
		}
		step.State = StepEndState(step, StepEndKilled)
		if step.Started != 0 {
			step.Finished = now
		}
		// only one not ended: a late report of its agent that started it
		// meanwhile is overwritten, one that ended it stays
		if _, err := store.StepFinish(step, model.StatusPending, model.StatusRunning); err != nil {
			log.Error().Err(err).Int64("step_id", step.ID).Msg("cannot end a step of a killed workflow")
		}
	}
	return true, nil
}
