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

package agent

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/state"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/tracing"
	"go.woodpecker-ci.org/woodpecker/v3/rpc"
)

// createTracer returns the tracer reporting step states. The workflowCtx is
// the context the workflow runs in: it tells a step stopped by the
// workflow's timeout from a canceled one.
func (r *Runner) createTracer(ctxMeta, workflowCtx context.Context, logger zerolog.Logger, workflow *rpc.Workflow) tracing.TraceFunc {
	return func(state *state.State) error {
		stepLogger := logger.With().
			Str("image", state.CurrStep.Image).
			Str("workflow_id", workflow.ID).
			Err(state.CurrStepState.Error).
			Int("exit_code", state.CurrStepState.ExitCode).
			Bool("exited", state.CurrStepState.Exited).
			Logger()

		stepState := rpc.StepState{
			StepUUID: state.CurrStep.UUID,
			Exited:   state.CurrStepState.Exited,
			ExitCode: state.CurrStepState.ExitCode,
			Started:  state.CurrStepState.Started,
			Canceled: errors.Is(state.CurrStepState.Error, pipeline_errors.ErrCancel),
			Skipped:  state.CurrStepState.Skipped,
		}
		stepState.TimedOut = stepTimedOut(state.CurrStepState.Error) ||
			(stepState.Canceled && workflowTimedOut(workflowCtx))
		if state.CurrStepState.Error != nil {
			stepState.Error = state.CurrStepState.Error.Error()
		}
		if state.CurrStepState.Exited {
			stepState.Finished = time.Now().Unix()
		}

		stepLogger.Debug().Msg("update step status")
		defer stepLogger.Debug().Msg("update step status complete")

		return r.client.Update(ctxMeta, workflow.ID, stepState)
	}
}

// stepTimedOut reports whether err ended a step by its own timeout.
func stepTimedOut(err error) bool {
	var timeoutErr *pipeline_errors.TimeoutError
	return errors.As(err, &timeoutErr)
}

// workflowTimedOut reports whether workflowCtx ended by the workflow's
// timeout, not by a cancel, a sigterm or the workflow's end.
func workflowTimedOut(workflowCtx context.Context) bool {
	return errors.Is(context.Cause(workflowCtx), context.DeadlineExceeded)
}
