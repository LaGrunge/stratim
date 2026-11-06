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

package runtime

import (
	"context"
	"errors"
	"time"

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
)

// finalizeGrace is how long the finalizers' context outlives their time:
// their steps time out at its end, this bounds what does not, such as a
// backend pulling an image.
const finalizeGrace = 30 * time.Second

// errFinalizeTimeUp ends the finalizers' context once their time and its
// grace are up.
var errFinalizeTimeUp = errors.New("the finalizers' time is up")

// splitFinalizers separates the finalizer stages from the others.
func splitFinalizers(all []*backend_types.Stage) (stages, finalizers []*backend_types.Stage) {
	for _, stage := range all {
		if stage.Finalizer {
			finalizers = append(finalizers, stage)
		} else {
			stages = append(stages, stage)
		}
	}
	return stages, finalizers
}

// finalizerSteps is the set of the steps of stages.
func finalizerSteps(stages []*backend_types.Stage) map[*backend_types.Step]struct{} {
	steps := make(map[*backend_types.Step]struct{})
	for _, stage := range stages {
		for _, step := range stage.Steps {
			steps[step] = struct{}{}
		}
	}
	return steps
}

// runFinalizers runs the finalizer stages once the other steps ended, or
// were stopped when stopped is set. The workflow's context may be done by
// then, so they run in one of their own, all of them within the config's
// FinalizeTimeout: each step's timeout is at most what is left of it, and
// once it is spent the finalizers left do not start. A cancel that comes
// while they run stops them, the workflow's timeout does not: they may have
// started just before it. It reports whether a cancel stopped them.
func (r *Runtime) runFinalizers(runnerCtx context.Context, stages []*backend_types.Stage, stopped bool) bool {
	// a cancel that came after the last regular stage ended and before now
	// is the cancel that stopped them: it does not stop the finalizers, but
	// the workflow was canceled
	canceledMeanwhile := !stopped && r.canceledMeanwhile()
	switch {
	case stopped:
		r.stopped.Set(r.stoppedStatus())
	case canceledMeanwhile:
		r.stopped.Set(statusKilled)
	}

	ctx, cancel := r.finalizerContext(runnerCtx)
	defer cancel()
	r.finalCtx.Store(&ctx)

	for i, stage := range stages {
		if ctx.Err() != nil {
			break
		}
		if r.finalizeTimeSpent() {
			r.traceFinalizeTimeSpent(stages[i:])
			break
		}
		if err := <-r.runStage(runnerCtx, stage.Steps); err != nil {
			r.err.Set(err)
		}
	}
	return canceledMeanwhile || ctx.Err() != nil && !errors.Is(context.Cause(ctx), errFinalizeTimeUp)
}

// canceledMeanwhile reports whether the workflow was canceled (not timed
// out) by now, its context or the cancel signal.
func (r *Runtime) canceledMeanwhile() bool {
	if r.cancelSignal != nil && r.cancelSignal.Err() != nil {
		return true
	}
	return r.ctx.Err() != nil && r.stoppedStatus() == statusKilled
}

// FinalizeDeadline is when the finalizers' context ends, their time and its
// grace up; zero while they did not start, or if they have no time set.
func (r *Runtime) FinalizeDeadline() time.Time {
	if deadline := r.finalHardDeadline.Load(); deadline != 0 {
		return time.Unix(0, deadline)
	}
	return time.Time{}
}

// finalizeTimeUp reports whether the finalizers' context ended, step being
// one, by their time and its grace being up.
func (r *Runtime) finalizeTimeUp(step *backend_types.Step) bool {
	if !r.isFinalizer(step) {
		return false
	}
	ctx := r.finalCtx.Load()
	return ctx != nil && errors.Is(context.Cause(*ctx), errFinalizeTimeUp)
}

// finalizerContext is the context finalizers run in. It keeps the values of
// the workflow's context and ends with runnerCtx, with the workflow's by a
// cancel (not by its deadline) when that is not done yet, with the cancel
// signal when that did not come yet, with the kill signal, and once the
// finalizers' time and its grace are up.
func (r *Runtime) finalizerContext(runnerCtx context.Context) (context.Context, func()) {
	base := context.WithoutCancel(r.ctx)
	stopDeadline := context.CancelFunc(func() {})
	if budget := r.spec.FinalizeTimeout; budget > 0 {
		end := time.Now().Add(budget)
		grace := r.finalizeGrace
		if grace == 0 {
			grace = finalizeGrace
		}
		r.finalDeadline.Store(end.UnixNano())
		r.finalHardDeadline.Store(end.Add(grace).UnixNano())
		base, stopDeadline = context.WithDeadlineCause(base, end.Add(grace), errFinalizeTimeUp)
	}
	ctx, cancel := context.WithCancelCause(base)

	stops := []func() bool{
		context.AfterFunc(runnerCtx, func() { cancel(context.Cause(runnerCtx)) }),
	}
	if workflowCtx := r.ctx; workflowCtx.Err() == nil {
		stops = append(stops, context.AfterFunc(workflowCtx, func() {
			if !errors.Is(context.Cause(workflowCtx), context.DeadlineExceeded) {
				cancel(context.Cause(workflowCtx))
			}
		}))
	}
	if signal := r.cancelSignal; signal != nil && signal.Err() == nil {
		stops = append(stops, context.AfterFunc(signal, func() { cancel(pipeline_errors.ErrCancel) }))
	}
	if signal := r.killSignal; signal != nil {
		if signal.Err() != nil {
			// given already: no finalizer starts
			cancel(pipeline_errors.ErrCancel)
		} else {
			stops = append(stops, context.AfterFunc(signal, func() { cancel(pipeline_errors.ErrCancel) }))
		}
	}

	return ctx, func() {
		for _, stop := range stops {
			stop()
		}
		cancel(nil)
		stopDeadline()
	}
}

// finalizeTimeSpent reports whether the finalizers' time is up.
func (r *Runtime) finalizeTimeSpent() bool {
	deadline := r.finalDeadline.Load()
	return deadline != 0 && !time.Now().Before(time.Unix(0, deadline))
}

// traceFinalizeTimeSpent reports the steps of stages, finalizers the
// finalizers' time did not leave room for, as timed out.
func (r *Runtime) traceFinalizeTimeSpent(stages []*backend_types.Stage) {
	logger := r.makeLogger()
	for _, stage := range stages {
		for _, step := range stage.Steps {
			logger.Debug().Str("step", step.Name).Msg("not started: the finalizers' time is up")
			timeoutErr := &pipeline_errors.TimeoutError{UUID: step.UUID, Timeout: r.spec.FinalizeTimeout}
			_ = r.traceStep(&backend_types.State{
				Exited:   true,
				ExitCode: pipeline_errors.TimeoutExitCode,
				Error:    timeoutErr,
			}, timeoutErr, step)
		}
	}
}
