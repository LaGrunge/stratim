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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

func mockStoreStep(t *testing.T) store.Store {
	s := mocks.NewMockStore(t)
	s.On("StepReport", mock.Anything, mock.Anything).Return(true, nil).Maybe()
	return s
}

func TestUpdateStepStatus(t *testing.T) {
	t.Parallel()

	t.Run("Pending", func(t *testing.T) {
		t.Parallel()

		t.Run("TransitionToRunning", func(t *testing.T) {
			t.Parallel()

			t.Run("WithStartTime", func(t *testing.T) {
				t.Parallel()
				step := &model.Step{State: model.StatusPending}
				state := rpc.StepState{Started: 42, Finished: 0}

				err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

				assert.NoError(t, err)
				assert.Equal(t, model.StatusRunning, step.State)
				assert.Equal(t, int64(42), step.Started)
				assert.Equal(t, int64(0), step.Finished)
			})

			t.Run("WithoutStartTime", func(t *testing.T) {
				t.Parallel()
				step := &model.Step{State: model.StatusPending}
				state := rpc.StepState{Started: 0, Finished: 0}

				err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

				assert.NoError(t, err)
				assert.Equal(t, model.StatusRunning, step.State)
				assert.Greater(t, step.Started, int64(0))
			})
		})

		t.Run("DirectToSuccess", func(t *testing.T) {
			t.Parallel()

			t.Run("WithFinishTime", func(t *testing.T) {
				t.Parallel()
				step := &model.Step{State: model.StatusPending}
				state := rpc.StepState{Started: 42, Exited: true, Finished: 100, ExitCode: 0, Error: ""}

				err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

				assert.NoError(t, err)
				assert.Equal(t, model.StatusSuccess, step.State)
				assert.Equal(t, int64(42), step.Started)
				assert.Equal(t, int64(100), step.Finished)
			})

			t.Run("WithoutFinishTime", func(t *testing.T) {
				t.Parallel()
				step := &model.Step{State: model.StatusPending}
				state := rpc.StepState{Started: 42, Exited: true, Finished: 0, ExitCode: 0, Error: ""}

				err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

				assert.NoError(t, err)
				assert.Equal(t, model.StatusSuccess, step.State)
				assert.Greater(t, step.Finished, int64(0))
			})
		})

		t.Run("DirectToFailure", func(t *testing.T) {
			t.Parallel()

			t.Run("WithExitCode", func(t *testing.T) {
				t.Parallel()
				step := &model.Step{State: model.StatusPending}
				state := rpc.StepState{Started: 42, Exited: true, Finished: 34, ExitCode: 1, Error: "an error"}

				err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

				assert.NoError(t, err)
				assert.Equal(t, model.StatusFailure, step.State)
				assert.Equal(t, 1, step.ExitCode)
				assert.Equal(t, "an error", step.Error)
			})
		})
	})

	t.Run("Running", func(t *testing.T) {
		t.Parallel()

		t.Run("ToSuccess", func(t *testing.T) {
			t.Parallel()

			t.Run("WithFinishTime", func(t *testing.T) {
				t.Parallel()
				step := &model.Step{State: model.StatusRunning, Started: 42}
				state := rpc.StepState{Exited: true, Finished: 100, ExitCode: 0, Error: ""}

				err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

				assert.NoError(t, err)
				assert.Equal(t, model.StatusSuccess, step.State)
				assert.Equal(t, int64(100), step.Finished)
			})

			t.Run("WithoutFinishTime", func(t *testing.T) {
				t.Parallel()
				step := &model.Step{State: model.StatusRunning, Started: 42}
				state := rpc.StepState{Exited: true, Finished: 0, ExitCode: 0, Error: ""}

				err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

				assert.NoError(t, err)
				assert.Equal(t, model.StatusSuccess, step.State)
				assert.Greater(t, step.Finished, int64(0))
			})
		})

		t.Run("ToFailure", func(t *testing.T) {
			t.Parallel()

			t.Run("WithExitCode137", func(t *testing.T) {
				t.Parallel()
				step := &model.Step{State: model.StatusRunning, Started: 42}
				state := rpc.StepState{Exited: true, Finished: 34, ExitCode: pipeline.ExitCodeKilled, Error: "an error"}

				err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

				assert.NoError(t, err)
				assert.Equal(t, model.StatusFailure, step.State)
				assert.Equal(t, int64(34), step.Finished)
				assert.Equal(t, pipeline.ExitCodeKilled, step.ExitCode)
			})

			t.Run("WithError", func(t *testing.T) {
				t.Parallel()
				step := &model.Step{State: model.StatusRunning, Started: 42}
				state := rpc.StepState{Exited: true, Finished: 34, ExitCode: 0, Error: "an error"}

				err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

				assert.NoError(t, err)
				assert.Equal(t, model.StatusFailure, step.State)
				assert.Equal(t, "an error", step.Error)
			})
		})

		t.Run("StillRunning", func(t *testing.T) {
			t.Parallel()
			step := &model.Step{State: model.StatusRunning, Started: 42}
			state := rpc.StepState{Exited: false, Finished: 0}

			err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

			assert.NoError(t, err)
			assert.Equal(t, model.StatusRunning, step.State)
			assert.Equal(t, int64(0), step.Finished)
		})
	})

	t.Run("Canceled", func(t *testing.T) {
		t.Parallel()

		t.Run("WithoutFinishTime", func(t *testing.T) {
			t.Parallel()
			step := &model.Step{State: model.StatusRunning, Started: 42}
			state := rpc.StepState{Canceled: true}

			err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

			assert.NoError(t, err)
			assert.Equal(t, model.StatusKilled, step.State)
			assert.Greater(t, step.Finished, int64(0))
		})

		t.Run("WithExitedAndFinishTime", func(t *testing.T) {
			t.Parallel()
			step := &model.Step{State: model.StatusRunning, Started: 42}
			state := rpc.StepState{Canceled: true, Exited: true, Finished: 100, ExitCode: 1, Error: "canceled"}

			err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

			assert.NoError(t, err)
			assert.Equal(t, model.StatusKilled, step.State)
			assert.Equal(t, int64(100), step.Finished)
			assert.Equal(t, 1, step.ExitCode)
			assert.Equal(t, "canceled", step.Error)
		})
	})

	t.Run("TimedOut", func(t *testing.T) {
		t.Parallel()

		t.Run("ByItsOwnTimeout", func(t *testing.T) {
			t.Parallel()
			step := &model.Step{State: model.StatusRunning, Started: 42}
			state := rpc.StepState{Exited: true, Finished: 100, ExitCode: 124, Error: "step timed out", TimedOut: true}

			err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

			assert.NoError(t, err)
			assert.Equal(t, model.StatusTimeout, step.State)
			assert.Equal(t, int64(100), step.Finished)
			assert.Equal(t, 124, step.ExitCode)
		})

		t.Run("FromPending", func(t *testing.T) {
			t.Parallel()
			step := &model.Step{State: model.StatusPending}
			state := rpc.StepState{Started: 42, Exited: true, Finished: 100, ExitCode: 124, Error: "step timed out", TimedOut: true}

			err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

			assert.NoError(t, err)
			assert.Equal(t, model.StatusTimeout, step.State)
		})

		t.Run("ByTheWorkflowTimeout", func(t *testing.T) {
			t.Parallel()
			step := &model.Step{State: model.StatusRunning, Started: 42}
			state := rpc.StepState{Canceled: true, TimedOut: true, Exited: true, Finished: 100, ExitCode: 137, Error: "Canceled"}

			err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

			assert.NoError(t, err)
			assert.Equal(t, model.StatusTimeout, step.State)
			assert.Equal(t, int64(100), step.Finished)
		})

		t.Run("ByTheWorkflowTimeoutNotExited", func(t *testing.T) {
			t.Parallel()
			step := &model.Step{State: model.StatusRunning, Started: 42}
			state := rpc.StepState{Canceled: true, TimedOut: true}

			err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

			assert.NoError(t, err)
			assert.Equal(t, model.StatusTimeout, step.State)
			assert.Greater(t, step.Finished, int64(0))
		})

		// timeout(1) exits 124 too: a command may exit with it by itself
		t.Run("ExitCode124ByItselfIsFailure", func(t *testing.T) {
			t.Parallel()
			step := &model.Step{State: model.StatusRunning, Started: 42}
			state := rpc.StepState{Exited: true, Finished: 100, ExitCode: 124}

			err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

			assert.NoError(t, err)
			assert.Equal(t, model.StatusFailure, step.State)
		})

		// a timeout is a failure of the step: failure: cancel cancels the pipeline
		t.Run("FailureCancel", func(t *testing.T) {
			t.Parallel()
			step := model.Step{State: model.StatusRunning, Started: 42, Failure: model.FailureCancel}
			state := rpc.StepState{Exited: true, Finished: 100, ExitCode: 124, Error: "step timed out", TimedOut: true}

			newStep, cancelPipeline, err := CalcStepStatus(step, state)

			assert.NoError(t, err)
			assert.Equal(t, model.StatusTimeout, newStep.State)
			assert.True(t, cancelPipeline)
		})
	})

	// Two steps with failure: cancel time out together: the second finds the
	// pipeline canceled already, which must not lose its own update.
	t.Run("FailureCancelOnCanceledPipeline", func(t *testing.T) {
		t.Parallel()
		s := mocks.NewMockStore(t)
		s.On("StepReport", mock.MatchedBy(func(step *model.Step) bool {
			return step.State == model.StatusTimeout
		}), model.StatusRunning).Return(true, nil).Once()
		s.On("GetPipeline", int64(7)).Return(&model.Pipeline{ID: 7, Status: model.StatusKilled}, nil)
		step := &model.Step{State: model.StatusRunning, Started: 42, PipelineID: 7, Failure: model.FailureCancel}
		state := rpc.StepState{Exited: true, Finished: 100, ExitCode: 124, Error: "step timed out", TimedOut: true}

		err := UpdateStepStatus(t.Context(), s, WorkflowEnd{}, step, state)

		assert.NoError(t, err)
		assert.Equal(t, model.StatusTimeout, step.State)
	})

	// The step is stored before the pipeline is canceled: a failing cancel
	// does not lose it.
	t.Run("FailureCancelStoresStepFirst", func(t *testing.T) {
		t.Parallel()
		s := mocks.NewMockStore(t)
		s.On("StepReport", mock.MatchedBy(func(step *model.Step) bool {
			return step.State == model.StatusTimeout
		}), model.StatusRunning).Return(true, nil).Once()
		s.On("GetPipeline", int64(7)).Return(&model.Pipeline{ID: 7, RepoID: 3, Status: model.StatusRunning}, nil)
		s.On("GetRepo", int64(3)).Return(nil, errors.New("store unavailable"))
		step := &model.Step{State: model.StatusRunning, Started: 42, PipelineID: 7, Failure: model.FailureCancel}
		state := rpc.StepState{Exited: true, Finished: 100, ExitCode: 124, Error: "step timed out", TimedOut: true}

		err := UpdateStepStatus(t.Context(), s, WorkflowEnd{}, step, state)

		assert.Error(t, err, "the cancel failed")
		assert.Equal(t, model.StatusTimeout, step.State)
		s.AssertCalled(t, "StepReport", mock.Anything, model.StatusRunning)
	})

	t.Run("Skipped", func(t *testing.T) {
		t.Parallel()

		// This mirrors exactly what the agent sends when executor.go detects
		// OnSuccess=false or OnFailure=false — only Skipped is set, everything
		// else is zero/false (no Started, no Finished, not Exited).
		t.Run("PendingToSkipped_AgentPayload", func(t *testing.T) {
			t.Parallel()
			step := &model.Step{State: model.StatusPending}
			// Exact payload from: traceStep(&backend.State{Skipped: true}, nil, step)
			// Started=0, Finished=0, Exited=false, Skipped=true
			state := rpc.StepState{
				Skipped:  true,
				Exited:   false,
				Finished: 0,
				Started:  0,
			}

			err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

			assert.NoError(t, err)
			// Must be Skipped, NOT Running (the bug: Finished==0 triggers StatusRunning first)
			assert.Equal(t, model.StatusSkipped, step.State)
			// Started must NOT be set — skipped steps never ran
			assert.Equal(t, int64(0), step.Started)
			// Finished must NOT be set — skipped steps never ran
			assert.Equal(t, int64(0), step.Finished)
		})

		t.Run("PendingToSkipped", func(t *testing.T) {
			t.Parallel()
			step := &model.Step{State: model.StatusPending}
			state := rpc.StepState{Skipped: true}

			err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

			assert.NoError(t, err)
			assert.Equal(t, model.StatusSkipped, step.State)
		})

		t.Run("PendingToSkippedWithFinishTime", func(t *testing.T) {
			t.Parallel()
			step := &model.Step{State: model.StatusPending}
			state := rpc.StepState{Skipped: true, Exited: true, Finished: 50}

			err := UpdateStepStatus(t.Context(), mockStoreStep(t), WorkflowEnd{}, step, state)

			assert.NoError(t, err)
			assert.Equal(t, model.StatusSkipped, step.State)
			assert.Equal(t, int64(50), step.Finished)
		})
	})

	// A late report of its agent finds the step ended meanwhile (its
	// workflow killed): that end stays, the step as the caller has it too,
	// and no cancel follows.
	t.Run("EndedMeanwhile", func(t *testing.T) {
		t.Parallel()
		s := mocks.NewMockStore(t)
		s.On("StepReport", mock.MatchedBy(func(step *model.Step) bool {
			return step.State == model.StatusFailure
		}), model.StatusRunning).Return(false, nil).Once()
		step := &model.Step{State: model.StatusRunning, Started: 42, PipelineID: 7, Failure: model.FailureCancel}
		state := rpc.StepState{Exited: true, Finished: 100, ExitCode: 1}

		err := UpdateStepStatus(t.Context(), s, WorkflowEnd{}, step, state)

		assert.ErrorIs(t, err, ErrStepEnded)
		assert.Equal(t, model.StatusRunning, step.State)
		s.AssertNotCalled(t, "GetPipeline", mock.Anything)
	})

	t.Run("TerminalState", func(t *testing.T) {
		t.Parallel()
		step := &model.Step{State: model.StatusKilled, Started: 42, Finished: 64}
		state := rpc.StepState{Exited: false}

		err := UpdateStepStatus(t.Context(), mocks.NewMockStore(t), WorkflowEnd{}, step, state)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not expect rpc state updates")
		assert.Equal(t, model.StatusKilled, step.State)
	})
}

func TestUpdateStepToStatusSkipped(t *testing.T) {
	t.Parallel()

	t.Run("NotStarted", func(t *testing.T) {
		t.Parallel()

		s := mocks.NewMockStore(t)
		s.On("StepFinish", mock.Anything, []model.StatusValue{model.StatusPending}).Return(true, nil).Once()
		step, won, err := UpdateStepToStatusSkipped(s, model.Step{State: model.StatusPending}, int64(1), model.StatusSkipped)

		assert.NoError(t, err)
		assert.True(t, won)
		assert.Equal(t, model.StatusSkipped, step.State)
		assert.Equal(t, int64(0), step.Finished)
	})

	t.Run("AlreadyStarted", func(t *testing.T) {
		t.Parallel()

		s := mocks.NewMockStore(t)
		s.On("StepFinish", mock.Anything, []model.StatusValue{model.StatusRunning}).Return(false, nil).Once()
		step, won, err := UpdateStepToStatusSkipped(s, model.Step{State: model.StatusRunning, Started: 42}, int64(100), model.StatusSkipped)

		assert.NoError(t, err)
		assert.False(t, won, "its agent ended it meanwhile")
		assert.Equal(t, model.StatusSuccess, step.State)
		assert.Equal(t, int64(100), step.Finished)
	})
}
