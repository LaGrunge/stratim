// Copyright 2022 Woodpecker Authors
// Copyright 2019 mhmxs
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
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

func CalcStepStatus(step model.Step, state rpc.StepState) (_ *model.Step, cancelPipelineFromStep bool, _ error) {
	log.Debug().Str("StepUUID", step.UUID).Msgf("Update step %#v state %#v", step, state)

	switch step.State {
	case model.StatusPending:
		// Handle skip before anything else — skipped steps never started,
		// so we must not set Started or transition through Running.
		if state.Skipped {
			step.State = model.StatusSkipped
			if state.Finished != 0 {
				step.Finished = state.Finished
			}
			return &step, false, nil
		}

		// Transition from pending to running when started
		if state.Finished == 0 {
			step.State = model.StatusRunning
		}
		step.Started = state.Started
		if step.Started == 0 {
			step.Started = time.Now().Unix()
		}

		// Handle direct transition to finished if step setup error happened
		if state.Exited || state.Error != "" {
			step.Finished = state.Finished
			if step.Finished == 0 {
				step.Finished = time.Now().Unix()
			}
			step.ExitCode = state.ExitCode
			step.Error = state.Error

			step.State = finishedStepStatus(state)
			if step.State != model.StatusSuccess && step.Failure == model.FailureCancel {
				cancelPipelineFromStep = true
			}
		}

	case model.StatusRunning:
		// Already running, check if it finished
		if state.Exited || state.Error != "" {
			step.Finished = state.Finished
			if step.Finished == 0 {
				step.Finished = time.Now().Unix()
			}
			step.ExitCode = state.ExitCode
			step.Error = state.Error

			step.State = finishedStepStatus(state)
			if step.State != model.StatusSuccess && step.Failure == model.FailureCancel {
				cancelPipelineFromStep = true
			}
		}

	default:
		return nil, false, fmt.Errorf("step has state %s and does not expect rpc state updates", step.State)
	}

	// Handle cancellation across both cases: a step the workflow's timeout
	// stopped timed out, any other stopped step was killed
	if state.Canceled {
		stopped := model.StatusKilled
		if state.TimedOut {
			stopped = model.StatusTimeout
		}
		if step.State != stopped {
			step.State = stopped
			if step.Finished == 0 {
				step.Finished = time.Now().Unix()
			}
		}
	}

	return &step, cancelPipelineFromStep, nil
}

// finishedStepStatus is the status of a step that ended. The agent tells a
// timeout explicitly: its exit code is the one timeout(1) gives, which a
// command may exit with by itself.
func finishedStepStatus(state rpc.StepState) model.StatusValue {
	switch {
	case state.TimedOut:
		return model.StatusTimeout
	case state.ExitCode == 0 && state.Error == "":
		return model.StatusSuccess
	default:
		return model.StatusFailure
	}
}

// UpdateStepStatus updates step status based on agent reports via RPC.
func UpdateStepStatus(ctx context.Context, store store.Store, end WorkflowEnd, step *model.Step, state rpc.StepState) error {
	log.Debug().Str("StepUUID", step.UUID).Msgf("Update step %#v state %#v", *step, state)

	from := step.State
	updatedStep, shouldCancelPipelineFromStep, err := CalcStepStatus(*step, state)
	if err != nil {
		return err
	}

	// the step is stored first: canceling the pipeline must not lose it.
	// Only if still as read: what ended it meanwhile (its workflow killed,
	// canceled) stays.
	won, err := store.StepReport(updatedStep, from)
	if err != nil {
		return err
	}
	if !won {
		return ErrStepEnded
	}
	*step = *updatedStep // update step for external callers
	if shouldCancelPipelineFromStep {
		return cancelPipelineFromStep(ctx, store, end, step)
	}
	return nil
}

// ErrStepEnded is UpdateStepStatus finding the step changed since it was
// read: ended by its workflow's end, which stands.
var ErrStepEnded = errors.New("the step changed meanwhile")

func cancelPipelineFromStep(ctx context.Context, store store.Store, end WorkflowEnd, step *model.Step) error {
	pipeline, err := store.GetPipeline(step.PipelineID)
	if err != nil {
		return err
	}
	// another step canceled it already, or it ended meanwhile
	if !pipelineCancelable(pipeline) {
		return nil
	}

	repo, err := store.GetRepo(pipeline.RepoID)
	if err != nil {
		return err
	}

	repoUser, err := store.GetUser(repo.UserID)
	if err != nil {
		return err
	}

	_forge, err := server.Config.Services.Manager.ForgeFromRepo(repo)
	if err != nil {
		return err
	}
	return Cancel(ctx, _forge, store, end, repo, repoUser, pipeline, &model.CancelInfo{
		CanceledByStep: step.Name,
	})
}

// UpdateStepToStatusSkipped ends a step that did not run with status; one
// that started ends as its workflow's end stops it (see StepEndState). Only
// state and finished are written, and only if the step still is as read:
// its agent may have started or ended it meanwhile. It reports whether it
// was written.
func UpdateStepToStatusSkipped(store store.Store, step model.Step, finished int64, status model.StatusValue) (*model.Step, bool, error) {
	from := step.State
	step.State = status
	if step.Started != 0 {
		step.State = StepEndState(&step, StepEndWorkflowDone)
		step.Finished = finished
	}
	won, err := store.StepFinish(&step, from)
	return &step, won, err
}
