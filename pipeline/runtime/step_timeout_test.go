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
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types/mocks"
	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
)

// expired is a step context past its deadline, in a workflow that goes on.
func expired(t *testing.T, r *Runtime) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(r.ctx, time.Nanosecond)
	t.Cleanup(cancel)
	<-ctx.Done()
	return ctx
}

func TestCompleteStepTimeout(t *testing.T) {
	t.Parallel()

	t.Run("TimedOut", func(t *testing.T) {
		t.Parallel()
		engine := mocks.NewMockBackend(t)
		engine.On("WaitStep", mock.Anything, mock.Anything, mock.Anything).
			Return(&backend_types.State{Exited: true, ExitCode: 130, Error: context.DeadlineExceeded}, nil)
		engine.On("DestroyStep", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		r := New(&backend_types.Config{}, engine, WithTracer(newTestTracer(t)), WithLogger(newTestLogger(t)))
		step := dummyStep("s1")
		step.Timeout = time.Minute

		ws, err := r.completeStep(t.Context(), expired(t, r), step, func() {}, time.Now().Unix())

		var timeoutErr *pipeline_errors.TimeoutError
		require.True(t, errors.As(err, &timeoutErr))
		assert.Equal(t, time.Minute, timeoutErr.Timeout)
		assert.Equal(t, pipeline_errors.TimeoutExitCode, ws.ExitCode)
		assert.True(t, pipeline_errors.IsStepFailure(err))
	})

	t.Run("WaitErrorAtTheDeadline", func(t *testing.T) {
		t.Parallel()
		engine := mocks.NewMockBackend(t)
		engine.On("WaitStep", mock.Anything, mock.Anything, mock.Anything).Return(nil, context.DeadlineExceeded)
		engine.On("DestroyStep", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		r := New(&backend_types.Config{}, engine, WithTracer(newTestTracer(t)), WithLogger(newTestLogger(t)))
		step := dummyStep("s1")
		step.Timeout = time.Minute

		ws, err := r.completeStep(t.Context(), expired(t, r), step, func() {}, time.Now().Unix())

		var timeoutErr *pipeline_errors.TimeoutError
		require.True(t, errors.As(err, &timeoutErr))
		assert.Equal(t, pipeline_errors.TimeoutExitCode, ws.ExitCode)
	})

	t.Run("ExitedCleanlyJustBefore", func(t *testing.T) {
		t.Parallel()
		engine := mocks.NewMockBackend(t)
		engine.On("WaitStep", mock.Anything, mock.Anything, mock.Anything).
			Return(&backend_types.State{Exited: true, ExitCode: 0}, nil)
		engine.On("DestroyStep", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		r := New(&backend_types.Config{}, engine, WithTracer(newTestTracer(t)), WithLogger(newTestLogger(t)))
		step := dummyStep("s1")
		step.Timeout = time.Minute

		ws, err := r.completeStep(t.Context(), expired(t, r), step, func() {}, time.Now().Unix())

		require.NoError(t, err, "a step that succeeded is not a timeout")
		assert.Equal(t, 0, ws.ExitCode)
	})

	t.Run("FailedJustBefore", func(t *testing.T) {
		t.Parallel()
		engine := mocks.NewMockBackend(t)
		engine.On("WaitStep", mock.Anything, mock.Anything, mock.Anything).
			Return(&backend_types.State{Exited: true, ExitCode: 1}, nil)
		engine.On("DestroyStep", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		r := New(&backend_types.Config{}, engine, WithTracer(newTestTracer(t)), WithLogger(newTestLogger(t)))
		step := dummyStep("s1")
		step.Timeout = time.Minute

		ws, err := r.completeStep(t.Context(), expired(t, r), step, func() {}, time.Now().Unix())

		var exitErr *pipeline_errors.ExitError
		require.True(t, errors.As(err, &exitErr), "the step's own exit code is kept: %v", err)
		assert.Equal(t, 1, ws.ExitCode)
	})

	t.Run("WorkflowCanceled", func(t *testing.T) {
		t.Parallel()
		engine := mocks.NewMockBackend(t)
		engine.On("WaitStep", mock.Anything, mock.Anything, mock.Anything).Return(nil, context.Canceled)
		engine.On("DestroyStep", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		r := canceledRuntime(t, engine)
		step := dummyStep("s1")
		step.Timeout = time.Minute
		stepCtx, cancel := context.WithTimeout(r.ctx, time.Minute)
		t.Cleanup(cancel)

		ws, err := r.completeStep(t.Context(), stepCtx, step, func() {}, time.Now().Unix())

		var timeoutErr *pipeline_errors.TimeoutError
		assert.False(t, errors.As(err, &timeoutErr), "a cancel is not a timeout")
		assert.ErrorIs(t, ws.Error, pipeline_errors.ErrCancel)
	})
}

// TestStepTimeoutClosesTheLog: a hung step that prints nothing, on a backend
// whose log stream ignores the context (as the local one does), still ends
// at its timeout.
func TestStepTimeoutClosesTheLog(t *testing.T) {
	t.Parallel()
	logs, _ := io.Pipe() // never written: the step prints nothing
	engine := mocks.NewMockBackend(t)
	engine.On("StartStep", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	engine.On("TailStep", mock.Anything, mock.Anything, mock.Anything).Return(logs, nil)
	engine.On("WaitStep", mock.Anything, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, _ *backend_types.Step, _ string) (*backend_types.State, error) {
			return &backend_types.State{Exited: true, Error: ctx.Err()}, nil
		},
	)
	engine.On("DestroyStep", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	r := New(&backend_types.Config{}, engine, WithTracer(newTestTracer(t)), WithLogger(newTestLogger(t)))
	step := dummyStep("s1")
	step.Timeout = 50 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- r.runBlockingStep(t.Context(), step) }()
	select {
	case err := <-done:
		var timeoutErr *pipeline_errors.TimeoutError
		assert.True(t, errors.As(err, &timeoutErr), "%v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the step was not ended by its timeout")
	}
}

// TestStepContext: a step's timeout bounds a step that is not detached; a
// detached step, services among them, runs as long as the workflow.
func TestStepContext(t *testing.T) {
	t.Parallel()
	r := New(&backend_types.Config{}, mocks.NewMockBackend(t), WithTracer(newTestTracer(t)), WithLogger(newTestLogger(t)))

	step := dummyStep("s1")
	step.Timeout = time.Minute
	ctx, cancel := r.stepContext(step)
	defer cancel()
	_, ok := ctx.Deadline()
	assert.True(t, ok)

	step.Detached = true
	ctx, cancel = r.stepContext(step)
	defer cancel()
	assert.Equal(t, r.ctx, ctx)
}
