// Copyright 2023 Woodpecker Authors
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline"
	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/logging"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/tracing"
	"go.woodpecker-ci.org/woodpecker/v3/shared/utils"
)

// Runtime represents a workflow state executed by a specific backend.
// Each workflow gets its own Runtime instance.
type Runtime struct {
	// err holds the first error that occurred in the workflow.
	err utils.Protected[error]

	spec    *backend_types.Config
	engine  backend_types.Backend
	started int64

	// ctx is the context for the current workflow execution.
	// All normal (non-cleanup) step operations must use this context.
	// Cleanup operations should use the runnerCtx passed to Run().
	ctx context.Context

	tracer tracing.Tracer
	logger logging.Logger

	uploadWait sync.WaitGroup

	taskUUID    string
	description map[string]string

	// stopped is how the regular steps were stopped, statusKilled or
	// statusTimeout, set before the finalizers run.
	stopped utils.Protected[string]
	// stepTimedOut is set when a step's own timeout failed the workflow.
	stepTimedOut atomic.Bool
	// skipped is set after a stage in which a step was skipped by its status;
	// skippedInStage collects that while the stage runs.
	skipped, skippedInStage atomic.Bool
	// finalizers are the steps of the finalizer stages, set before any runs.
	finalizers map[*backend_types.Step]struct{}
	// finalCtx is the context the finalizers run in, set when they start.
	finalCtx atomic.Pointer[context.Context]
	// finalDeadline is when the finalizers' time is up, in unix nanoseconds:
	// zero for none, set when they start.
	finalDeadline atomic.Int64
	// finalHardDeadline is when the finalizers' context ends, their time
	// and its grace up, in unix nanoseconds: zero for none.
	finalHardDeadline atomic.Int64
	// finalizeGrace replaces the const of the name when set; only tests do.
	finalizeGrace time.Duration
	// detachedCtx is the context detached steps run in when there are
	// finalizers: it outlives the workflow's until they ended. Nil otherwise.
	detachedCtx context.Context
	// cancelSignal ends when the workflow is canceled, also once its context
	// ended (see WithCancelSignal). Nil if not set.
	cancelSignal context.Context
	// killSignal ends when the finalizers are to be stopped too (see
	// WithKillSignal). Nil if not set.
	killSignal context.Context
}

// The outcome of a workflow so far, as STRATIM_PIPELINE_STATUS gives it.
const (
	statusSuccess = pipeline.StatusSuccess
	statusFailure = pipeline.StatusFailure
	statusTimeout = pipeline.StatusTimeout
	statusKilled  = pipeline.StatusKilled
)

// New returns a new Runtime for the given workflow spec and options.
func New(spec *backend_types.Config, backend backend_types.Backend, opts ...Option) *Runtime {
	r := new(Runtime)
	r.err = utils.NewProtected[error](nil)
	r.stopped = utils.NewProtected("")
	r.description = make(map[string]string)
	r.spec = spec
	r.engine = backend
	r.ctx = context.Background()
	r.taskUUID = ulid.Make().String()
	r.tracer = tracing.NoOpTracer
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Err returns the error the workflow ended with, including step failures that
// Run does not report as runtime errors. It is nil if the workflow succeeded.
func (r *Runtime) Err() error {
	return r.err.Get()
}

// stepParent is the context that stops a step: the finalizers' for a
// finalizer, the workflow's for any other step.
func (r *Runtime) stepParent(step *backend_types.Step) context.Context {
	if r.isFinalizer(step) {
		if ctx := r.finalCtx.Load(); ctx != nil {
			return *ctx
		}
	}
	return r.ctx
}

// backendCtx is the context a step's backend calls run in: the one that
// stops it, but for a detached step while there are finalizers one that
// outlives the workflow's, so the finalizers can still reach it.
func (r *Runtime) backendCtx(step *backend_types.Step) context.Context {
	if step.Detached && !r.isFinalizer(step) && r.detachedCtx != nil {
		return r.detachedCtx
	}
	return r.stepParent(step)
}

// isFinalizer reports whether step is in a finalizer stage.
func (r *Runtime) isFinalizer(step *backend_types.Step) bool {
	_, ok := r.finalizers[step]
	return ok
}

// stepStopped reports whether the context that stops step is done: the
// workflow (or, for a finalizer, the finalizers) was canceled or timed out
// and everything still running is being torn down.
func (r *Runtime) stepStopped(step *backend_types.Step) bool {
	return r.stepParent(step).Err() != nil
}

// status is the outcome of the workflow so far: how its regular steps were
// stopped, else whether a step timed out or failed.
func (r *Runtime) status() string {
	if stopped := r.stopped.Get(); stopped != "" {
		return stopped
	}
	switch {
	case r.stepTimedOut.Load():
		return statusTimeout
	case r.err.Get() != nil:
		return statusFailure
	default:
		return statusSuccess
	}
}

// stoppedStatus is how the workflow's context stopped it: by its timeout or
// by a cancel.
func (r *Runtime) stoppedStatus() string {
	if errors.Is(context.Cause(r.ctx), context.DeadlineExceeded) {
		return statusTimeout
	}
	return statusKilled
}

// makeLogger returns a logger enriched with all runtime description fields.
func (r *Runtime) makeLogger() zerolog.Logger {
	logCtx := log.With()
	for key, val := range r.description {
		logCtx = logCtx.Str(key, val)
	}
	return logCtx.Logger()
}
