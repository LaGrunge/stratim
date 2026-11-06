// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline"
	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/state"
)

// executeStep is the single entry point called per step from runStage.
// It checks whether the step should be skipped, emits a "started" trace,
// sets up drone-compat env vars, then hands off to blocking or detached execution.
func (r *Runtime) executeStep(runnerCtx context.Context, step *backend_types.Step) error {
	logger := r.makeLogger()
	logger.Debug().Str("step", step.Name).Msg("prepare")

	if r.shouldSkipStep(step) {
		// a step waiting for a skip that did not come is no skip itself:
		// steps after it waiting for one would run on it
		if !slices.Contains(runOn(step), pipeline.StatusSkipped) {
			r.skippedInStage.Store(true)
		}
		// Trace the skip so the server marks the step as skipped immediately,
		// rather than leaving it in "pending" until workflow Done.
		return r.traceStep(&backend_types.State{Skipped: true}, nil, step)
	}

	// Emit a "step started" trace before doing any real work.
	if err := r.traceStep(nil, nil, step); err != nil {
		return err
	}

	// Set runtime specific step environment
	if err := r.setStepEnv(step); err != nil {
		return err
	}

	logger.Debug().Str("step", step.Name).Msg("executing")

	if step.Detached && !r.isFinalizer(step) {
		return r.runDetachedStep(runnerCtx, step)
	}
	return r.runBlockingStep(runnerCtx, step)
}

// runOn lists the outcomes so far the step runs after: its RunOn, or from
// a compiler before RunOn, its OnSuccess and OnFailure.
func runOn(step *backend_types.Step) []string {
	if len(step.RunOn) > 0 {
		return step.RunOn
	}
	var statuses []string
	if step.OnSuccess {
		statuses = append(statuses, pipeline.StatusSuccess)
	}
	if step.OnFailure {
		statuses = append(statuses, pipeline.StatusFailure)
	}
	return statuses
}

// runsAfter reports whether a step running on statuses runs after the
// outcome status: failure covers timeout and killed, skipped is whether a
// step of an earlier stage was skipped.
func runsAfter(statuses []string, status string, skipped bool) bool {
	if slices.Contains(statuses, status) {
		return true
	}
	if (status == statusTimeout || status == statusKilled) && slices.Contains(statuses, statusFailure) {
		return true
	}
	return skipped && slices.Contains(statuses, pipeline.StatusSkipped)
}

// shouldSkipStep returns true when the step should not run based on the
// workflow's outcome so far and the outcomes the step runs on.
// It logs the reason for skipping before returning.
func (r *Runtime) shouldSkipStep(step *backend_types.Step) bool {
	status := r.status()
	statuses := runOn(step)
	if runsAfter(statuses, status, r.skipped.Load()) {
		return false
	}

	logger := r.makeLogger()
	logger.Debug().
		Str("step", step.Name).
		Str("status", status).
		Strs("run_on", statuses).
		Msg("skipped: it does not run on the outcome so far")
	return true
}

// setStepEnv sets runtime specific step environment variables.
// It also adds the drone plugin compatibility layer.
func (r *Runtime) setStepEnv(step *backend_types.Step) error {
	if step.Environment == nil {
		return fmt.Errorf("step %q (%q) has no environment variables initialized", step.Name, step.UUID)
	}

	// Add compatibility environment variables for drone-ci plugins.
	if step.Type == backend_types.StepTypePlugin {
		metadata.SetDroneEnviron(step.Environment)
	}

	// the outcome so far; CI_PIPELINE_STATUS keeps Drone's two values
	status := r.status()
	step.Environment["STRATIM_PIPELINE_STATUS"] = status
	if status == statusSuccess {
		step.Environment["CI_PIPELINE_STATUS"] = statusSuccess
	} else {
		step.Environment["CI_PIPELINE_STATUS"] = statusFailure
	}
	step.Environment["CI_PIPELINE_STARTED"] = strconv.FormatInt(r.started, 10)
	step.Environment["CI_STEP_STARTED"] = strconv.FormatInt(time.Now().Unix(), 10)
	step.Environment["CI_STEP_TYPE"] = string(step.Type)
	step.Environment["CI_STEP_NAME"] = step.Name

	return nil
}

// startStep starts the step container and spawns a goroutine to stream its logs.
// It returns:
//   - waitForLogs: must be called before WaitStep — it blocks until the log stream
//     is fully drained. Some backends (e.g. local) close the log stream when
//     WaitStep is called, so draining first is required.
//   - startTime: unix timestamp recorded right after the container started, used
//     later to fill waitState.Started.
//
// If StartStep or TailStep fail, startStep returns a non-nil error and the caller
// must not call waitForLogs.
//
// It also returns the step's context, which its timeout ends, counted from
// now: the time it took to start (pulling the image, scheduling a pod) is
// not the step's. Its deadline closes the log stream, which a backend that
// ignores the context of TailStep would keep open; release frees it.
func (r *Runtime) startStep(step *backend_types.Step) (stepCtx context.Context, release, waitForLogs func(), startTime int64, err error) {
	ctx := r.backendCtx(step)
	if err := r.engine.StartStep(ctx, step, r.taskUUID); err != nil {
		return nil, nil, nil, 0, err
	}
	startTime = time.Now().Unix()

	rc, err := r.engine.TailStep(ctx, step, r.taskUUID)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	stepCtx, cancel := r.stepContext(step)
	stop := context.AfterFunc(stepCtx, func() {
		if r.timedOut(step, stepCtx) {
			_ = rc.Close()
		}
	})
	release = func() {
		stop()
		cancel()
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		logger := r.makeLogger()
		if err := r.logger(step, rc); err != nil {
			logger.Error().Err(err).Str("step", step.Name).Msg("step log streaming failed")
		}
		_ = rc.Close()
	})

	return stepCtx, release, wg.Wait, startTime, nil
}

// cancelFallout reports whether err is a consequence of the workflow being
// canceled rather than of the step itself. Once the workflow context is done,
// every backend call is aborted mid-flight and the error it returns describes
// that abort. Its shape is backend specific: context.Canceled, ErrCancel, or a
// transport error wrapping whichever cancel cause the caller set, so the
// context is the only reliable signal.
func (r *Runtime) cancelFallout(step *backend_types.Step, err error) bool {
	if err == nil {
		return false
	}
	// a timeout stands, also when the context ended after it
	if _, ok := errors.AsType[*pipeline_errors.TimeoutError](err); ok {
		return false
	}
	return r.stepStopped(step) || errors.Is(err, context.Canceled) || errors.Is(err, pipeline_errors.ErrCancel)
}

// cancelErr logs the original error and returns ErrCancel in its place, so a
// canceled step reports the cancellation instead of the backend's internal
// error text.
func (r *Runtime) cancelErr(err error, step *backend_types.Step) error {
	logger := r.makeLogger()
	logger.Debug().Err(err).Str("step", step.Name).Msg("step error caused by workflow cancellation")
	return pipeline_errors.ErrCancel
}

// completeStep drains the log stream, waits for the process to exit, destroys
// the container, and maps exit conditions (OOM kill, non-zero exit code, context
// cancellation) to typed errors.
//
// The runnerCtx is intentionally used for DestroyStep so that container cleanup can
// still reach the backend even after the workflow context (r.ctx) is canceled.
func (r *Runtime) completeStep(runnerCtx, stepCtx context.Context, step *backend_types.Step, waitForLogs func(), startTime int64) (*backend_types.State, error) {
	// Drain the log stream before waiting on the process exit.
	waitForLogs()

	waitState, err := r.engine.WaitStep(stepCtx, step, r.taskUUID)
	// the step ran past its timeout, unless the wait found it ended by
	// itself: it is stopped below, as a canceled step is, and fails
	timedOut := !exited(waitState, err) && r.timedOut(step, stepCtx) ||
		// a finalizer that outlived the finalizers' time and its grace too
		r.finalizeTimeUp(step) //nolint:contextcheck // the finalizers' own context, which it reads
	if timedOut {
		err = nil
	}
	if err != nil {
		if !r.cancelFallout(step, err) {
			return nil, err
		}
		if waitState == nil {
			waitState = &backend_types.State{}
		}
		waitState.Error = r.cancelErr(err, step)
	}

	// Use runnerCtx here: the workflow context may already be canceled but we
	// still need to reach the backend to stop/remove the container.
	if err := r.engine.DestroyStep(runnerCtx, step, r.taskUUID); err != nil {
		return nil, err
	}

	if timedOut {
		timeoutErr := &pipeline_errors.TimeoutError{UUID: step.UUID, Timeout: r.timeoutOf(step, stepCtx)}
		return &backend_types.State{
			Started:  startTime,
			Exited:   true,
			ExitCode: pipeline_errors.TimeoutExitCode,
			Error:    timeoutErr,
		}, timeoutErr
	}
	waitState.Started = startTime

	// Re-check context cancellation: the wait may have raced with cancellation.
	if r.stepStopped(step) {
		waitState.Error = pipeline_errors.ErrCancel
	}

	if waitState.OOMKilled {
		err := &pipeline_errors.OomError{
			UUID: step.UUID,
			Code: waitState.ExitCode,
		}

		waitState.Error = errors.Join(waitState.Error, err)

		return waitState, err
	}
	if waitState.ExitCode != 0 {
		return waitState, &pipeline_errors.ExitError{
			UUID: step.UUID,
			Code: waitState.ExitCode,
		}
	}
	if errors.Is(waitState.Error, pipeline_errors.ErrCancel) {
		return waitState, nil
	}

	return waitState, waitState.Error
}

// stepTimeoutKey is the context value stepContext keeps a step's timeout in.
type stepTimeoutKey struct{}

// stepContext is the context a step runs in: the workflow's (the
// finalizers' for a finalizer), ended by the step's timeout if it has one. A
// detached step runs as long as the workflow: it has no timeout of its own.
func (r *Runtime) stepContext(step *backend_types.Step) (context.Context, context.CancelFunc) {
	timeout := r.stepTimeout(step)
	if timeout <= 0 {
		return r.backendCtx(step), func() {}
	}
	ctx, cancel := context.WithTimeout(r.backendCtx(step), timeout)
	return context.WithValue(ctx, stepTimeoutKey{}, timeout), cancel
}

// stepTimeout is the timeout that ends a step, zero for none: its own, but
// none for a detached step; a finalizer's is bounded by what is left of the
// finalizers' time.
func (r *Runtime) stepTimeout(step *backend_types.Step) time.Duration {
	if r.isFinalizer(step) {
		deadline := r.finalDeadline.Load()
		if deadline == 0 {
			return max(step.Timeout, 0)
		}
		left := max(time.Until(time.Unix(0, deadline)), time.Nanosecond)
		if step.Timeout > 0 {
			return min(step.Timeout, left)
		}
		return left
	}
	if step.Detached {
		return 0
	}
	return max(step.Timeout, 0)
}

// timeoutOf is the timeout stepCtx gave step.
func (r *Runtime) timeoutOf(step *backend_types.Step, stepCtx context.Context) time.Duration {
	if timeout, ok := stepCtx.Value(stepTimeoutKey{}).(time.Duration); ok {
		if timeout >= time.Second {
			// a finalizer's is what was left, to the nanosecond
			timeout = timeout.Round(time.Second)
		}
		return timeout
	}
	return r.stepTimeout(step)
}

// exited reports whether a wait found the step ended by itself, whatever
// its exit code.
func exited(state *backend_types.State, err error) bool {
	return err == nil && state != nil && state.Exited && state.Error == nil
}

// timedOut reports whether stepCtx ended by the step's own timeout, not by
// the workflow's (or the finalizers') end.
func (r *Runtime) timedOut(step *backend_types.Step, stepCtx context.Context) bool {
	return !r.stepStopped(step) && errors.Is(stepCtx.Err(), context.DeadlineExceeded)
}

// runBlockingStep starts the step and blocks until it fully completes.
// The error is traced and returned to runStage, which feeds it into the
// stage error group.
func (r *Runtime) runBlockingStep(runnerCtx context.Context, step *backend_types.Step) error {
	logger := r.makeLogger()

	stepCtx, release, waitForLogs, startTime, err := r.startStep(step)
	if err != nil {
		// The step never ran — trace the start failure and surface it.
		if r.cancelFallout(step, err) {
			err = r.cancelErr(err, step)
		}
		return r.traceStep(nil, err, step)
	}

	processState, err := r.completeStep(runnerCtx, stepCtx, step, waitForLogs, startTime)
	release()
	logger.Debug().Str("step", step.Name).Msg("complete")

	if r.cancelFallout(step, err) {
		err = r.cancelErr(err, step)
	}

	err = r.traceStep(processState, err, step)
	if err != nil && metadata.Failure(step.Failure) == metadata.FailureIgnore {
		return nil
	}
	if _, ok := errors.AsType[*pipeline_errors.TimeoutError](err); ok {
		r.stepTimedOut.Store(true)
	}
	return err
}

// runDetachedStep starts the step and returns as soon as the container is running
// and log streaming is set up. The rest of the step lifecycle runs in the background.
//
// Any error that occurs after setup is logged but not propagated — it cannot
// influence the pipeline outcome at that point.
func (r *Runtime) runDetachedStep(runnerCtx context.Context, step *backend_types.Step) error {
	stepCtx, release, waitForLogs, startTime, err := r.startStep(step)
	if err != nil {
		// Setup failed before the container was running — treat it like a
		// blocking failure so the pipeline is aware.
		if r.cancelFallout(step, err) {
			err = r.cancelErr(err, step)
		}
		return r.traceStep(nil, err, step)
	}

	// Container is up and logging is streaming — hand off to background.
	r.uploadWait.Go(func() {
		logger := r.makeLogger()

		processState, err := r.completeStep(runnerCtx, stepCtx, step, waitForLogs, startTime)
		release()
		logger.Debug().Str("step", step.Name).Msg("complete")

		if r.cancelFallout(step, err) {
			err = r.cancelErr(err, step)
		} else if err != nil {
			logger.Error().Err(err).Str("step", step.Name).Msg("detached step failed while running")
		}

		if traceErr := r.traceStep(processState, err, step); traceErr != nil {
			logger.Error().Err(traceErr).Str("step", step.Name).Msg("failed to trace detached step result")
		}
	})

	return nil
}

// traceStep reports the current state of a step to the tracer.
//
//   - processState == nil, err == nil  →  step is being marked as started
//   - processState == nil, err != nil  →  step failed to start
//   - processState != nil              →  step has finished (err may or may not be set)
//
// Always returns err unchanged so callers can write: return r.traceStep(state, err, step).
func (r *Runtime) traceStep(processState *backend_types.State, err error, step *backend_types.Step) error {
	s := new(state.State)
	s.Workflow.Started = r.started
	s.CurrStep = step
	s.CurrStepFinalizer = r.isFinalizer(step)
	s.Workflow.Error = r.err.Get()

	switch {
	case processState == nil && err != nil:
		// Step failed to start — create an dummy exited process state.
		s.CurrStepState = backend_types.State{
			Error:     err,
			Exited:    true,
			OOMKilled: false,
		}
	case processState != nil:
		s.CurrStepState = *processState
		// processState == nil && err == nil: step just started, leave s.CurrStepState zero-valued.
	}

	if traceErr := r.tracer.Trace(s); traceErr != nil {
		logger := r.makeLogger()
		logger.Error().Err(traceErr).Msg("could not trace step state change")
	}

	return err
}
