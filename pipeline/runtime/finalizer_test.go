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

//go:build test

package runtime

import (
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/dummy"
	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types/mocks"
	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/state"
)

// testFinalizerKey marks the steps finalizer makes, for stages to put them
// in finalizer stages.
const testFinalizerKey = "TEST_FINALIZER"

// finalizer is an on_finish step that runs whatever the outcome.
func finalizer(name string, opts ...func(*backend_types.Step)) *backend_types.Step {
	s := cmdStep(name)
	s.Environment[testFinalizerKey] = "true"
	s.OnSuccess = true
	s.OnFailure = true
	for _, o := range opts {
		o(s)
	}
	return s
}

// onlyOn makes a step run on statuses only.
func onlyOn(statuses ...string) func(*backend_types.Step) {
	return func(s *backend_types.Step) {
		s.RunOn = statuses
		s.OnSuccess = slices.Contains(statuses, "success")
		s.OnFailure = slices.Contains(statuses, "failure")
	}
}

func withTimeout(d time.Duration) func(*backend_types.Step) {
	return func(s *backend_types.Step) { s.Timeout = d }
}

// stages puts each step in a stage of its own, a finalizer stage for one
// finalizer made.
func stages(steps ...*backend_types.Step) *backend_types.Config {
	config := &backend_types.Config{}
	for _, step := range steps {
		config.Stages = append(config.Stages, &backend_types.Stage{
			Steps:     []*backend_types.Step{step},
			Finalizer: step.Environment[testFinalizerKey] != "",
		})
	}
	return config
}

// recorder is a tracer keeping a copy of every state traced, safe for the
// traces detached steps send after Run returned.
type recorder struct {
	mu     sync.Mutex
	traced []state.State
	// onTrace, if set, sees every state as it is traced.
	onTrace func(*state.State)
}

func (r *recorder) Trace(s *state.State) error {
	if r.onTrace != nil {
		r.onTrace(s)
	}
	traced := *s
	if s.CurrStep != nil {
		step := *s.CurrStep
		step.Environment = maps.Clone(step.Environment)
		traced.CurrStep = &step
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.traced = append(r.traced, traced)
	return nil
}

func (r *recorder) states() []state.State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.traced)
}

// cancelOnStart is a tracer that cancels with cause once the step named
// name reports it started.
func cancelOnStart(name string, cancel context.CancelCauseFunc) *recorder {
	return &recorder{onTrace: func(s *state.State) {
		if s.CurrStep != nil && s.CurrStep.Name == name && !s.CurrStepState.Exited {
			cancel(pipeline_errors.ErrCancel)
		}
	}}
}

// ran reports whether a step ran: it was traced as started and not skipped.
func ran(traces []state.State, name string) bool {
	last := findLastTraceByName(traces, name)
	return last != nil && !last.CurrStepState.Skipped
}

// statusSeen is the STRATIM_PIPELINE_STATUS a step started with.
func statusSeen(t *testing.T, traces []state.State, name string) string {
	t.Helper()
	last := findLastTraceByName(traces, name)
	require.NotNil(t, last, "step %s was not traced", name)
	return last.CurrStep.Environment["STRATIM_PIPELINE_STATUS"]
}

func TestFinalizerAfterSuccess(t *testing.T) {
	t.Parallel()
	tracer := &recorder{}
	r := New(stages(
		cmdStep("build"),
		finalizer("logs"),
		finalizer("on-failure", onlyOn("failure")),
	), dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)))

	require.NoError(t, r.Run(t.Context()))

	traces := tracer.states()
	assert.True(t, ran(traces, "logs"))
	assert.Equal(t, "success", statusSeen(t, traces, "logs"))
	assert.Equal(t, "success", findLastTraceByName(traces, "logs").CurrStep.Environment["CI_PIPELINE_STATUS"])
	assert.False(t, ran(traces, "on-failure"), "status: failure does not match a success")
}

func TestFinalizerAfterFailure(t *testing.T) {
	t.Parallel()
	tracer := &recorder{}
	r := New(stages(
		cmdStep("build", withExitCode(1)),
		cmdStep("test"),
		finalizer("logs"),
		finalizer("on-failure", onlyOn("failure")),
		finalizer("on-success", onlyOn("success")),
		finalizer("on-timeout", onlyOn("timeout")),
	), dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)))

	require.NoError(t, r.Run(t.Context()), "a step failure is no runtime error")

	traces := tracer.states()
	assert.False(t, ran(traces, "test"))
	assert.True(t, ran(traces, "logs"))
	assert.Equal(t, "failure", statusSeen(t, traces, "logs"))
	assert.Equal(t, "failure", findLastTraceByName(traces, "logs").CurrStep.Environment["CI_PIPELINE_STATUS"])
	assert.True(t, ran(traces, "on-failure"))
	assert.False(t, ran(traces, "on-success"))
	assert.False(t, ran(traces, "on-timeout"), "a plain failure is no timeout")
}

// A step's own timeout: the finalizer sees timeout, status: timeout and
// status: failure match, and CI_PIPELINE_STATUS keeps saying failure.
func TestFinalizerAfterStepTimeout(t *testing.T) {
	t.Parallel()
	tracer := &recorder{}
	r := New(stages(
		cmdStep("hangs", withSleep("30s"), withTimeout(50*time.Millisecond)),
		cmdStep("on-timeout-step", onlyOn("timeout")),
		finalizer("logs"),
		finalizer("on-timeout", onlyOn("timeout")),
		finalizer("on-failure", onlyOn("failure")),
	), dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)))

	require.NoError(t, r.Run(t.Context()))

	traces := tracer.states()
	assert.True(t, ran(traces, "on-timeout-step"), "a regular step with status: timeout runs too")
	assert.Equal(t, "timeout", statusSeen(t, traces, "on-timeout-step"))
	assert.Equal(t, "timeout", statusSeen(t, traces, "logs"))
	assert.Equal(t, "failure", findLastTraceByName(traces, "logs").CurrStep.Environment["CI_PIPELINE_STATUS"])
	assert.True(t, ran(traces, "on-timeout"))
	assert.True(t, ran(traces, "on-failure"), "a timeout is a failure")
}

// A canceled workflow stops its regular steps, then runs its finalizers,
// which see killed; Run still reports the cancel.
func TestFinalizerAfterCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	tracer := cancelOnStart("slow", cancel)
	r := New(stages(
		cmdStep("slow", withSleep("30s")),
		cmdStep("next"),
		finalizer("logs", withSleep("50ms")),
		finalizer("on-success", onlyOn("success")),
		finalizer("on-failure", onlyOn("failure")),
	), dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)), WithContext(ctx))

	start := time.Now()
	err := r.Run(t.Context())
	assert.ErrorIs(t, err, pipeline_errors.ErrCancel)
	assert.Less(t, time.Since(start), 10*time.Second)

	traces := tracer.states()
	assert.Nil(t, findFirstTraceByName(traces, "next"), "a regular step after the cancel never starts")
	require.True(t, ran(traces, "logs"), "the finalizer runs after a cancel")
	logs := findLastTraceByName(traces, "logs")
	assert.True(t, logs.CurrStepState.Exited)
	assert.NoError(t, logs.CurrStepState.Error, "the finalizer was not canceled itself")
	assert.Equal(t, 0, logs.CurrStepState.ExitCode)
	assert.Equal(t, "killed", statusSeen(t, traces, "logs"))
	assert.False(t, ran(traces, "on-success"))
	assert.True(t, ran(traces, "on-failure"), "a kill is a failure")
}

// A workflow that timed out runs its finalizers, which see timeout.
func TestFinalizerAfterWorkflowTimeout(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	tracer := &recorder{}
	r := New(stages(
		cmdStep("slow", withSleep("30s")),
		finalizer("logs"),
		finalizer("on-timeout", onlyOn("timeout")),
	), dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)), WithContext(ctx))

	assert.ErrorIs(t, r.Run(t.Context()), pipeline_errors.ErrCancel)

	traces := tracer.states()
	assert.True(t, ran(traces, "logs"))
	assert.Equal(t, "timeout", statusSeen(t, traces, "logs"))
	assert.True(t, ran(traces, "on-timeout"))
	assert.NoError(t, findLastTraceByName(traces, "logs").CurrStepState.Error)
}

// A cancel that comes while the finalizers run stops them: the running one
// is canceled, the ones after it never start.
func TestFinalizerCanceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	tracer := cancelOnStart("hangs", cancel)
	r := New(stages(
		cmdStep("build"),
		finalizer("hangs", withSleep("30s")),
		finalizer("after"),
	), dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)), WithContext(ctx))

	start := time.Now()
	assert.ErrorIs(t, r.Run(t.Context()), pipeline_errors.ErrCancel)
	assert.Less(t, time.Since(start), 10*time.Second)

	traces := tracer.states()
	hangs := findLastTraceByName(traces, "hangs")
	require.NotNil(t, hangs)
	assert.ErrorIs(t, hangs.CurrStepState.Error, pipeline_errors.ErrCancel)
	assert.Nil(t, findFirstTraceByName(traces, "after"))
}

// A second cancel, the kill signal, stops the finalizers the first cancel
// left running: the one running is canceled, the ones after it never start.
func TestFinalizerKilled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	kill, signalKill := context.WithCancelCause(t.Context())
	defer signalKill(nil)
	tracer := &recorder{onTrace: func(s *state.State) {
		if s.CurrStep == nil || s.CurrStepState.Exited {
			return
		}
		switch s.CurrStep.Name {
		case "slow":
			cancel(pipeline_errors.ErrCancel)
		case "hangs":
			signalKill(pipeline_errors.ErrCancel)
		}
	}}
	r := New(stages(
		cmdStep("slow", withSleep("30s")),
		finalizer("hangs", withSleep("30s")),
		finalizer("after"),
	), dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)), WithContext(ctx),
		WithCancelSignal(ctx), WithKillSignal(kill))

	start := time.Now()
	assert.ErrorIs(t, r.Run(t.Context()), pipeline_errors.ErrCancel)
	assert.Less(t, time.Since(start), 10*time.Second)

	traces := tracer.states()
	hangs := findLastTraceByName(traces, "hangs")
	require.NotNil(t, hangs)
	assert.ErrorIs(t, hangs.CurrStepState.Error, pipeline_errors.ErrCancel)
	assert.Nil(t, findFirstTraceByName(traces, "after"))
}

// A kill signal given before the finalizers start leaves them all unstarted.
func TestFinalizerKilledBefore(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(pipeline_errors.ErrCancel)
	tracer := &recorder{}
	r := New(stages(cmdStep("build"), finalizer("logs")), dummy.New(),
		WithTracer(tracer), WithLogger(newTestLogger(t)), WithContext(ctx), WithKillSignal(ctx))

	assert.ErrorIs(t, r.Run(t.Context()), pipeline_errors.ErrCancel)
	assert.Nil(t, findFirstTraceByName(tracer.states(), "logs"))
}

// The workflow's timeout passing while the finalizers run does not stop
// them: they may have started just before it.
func TestFinalizerOutlivesWorkflowTimeout(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	tracer := &recorder{}
	r := New(stages(
		cmdStep("build"),
		finalizer("slow", withSleep("300ms")),
		finalizer("after"),
	), dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)), WithContext(ctx))

	require.NoError(t, r.Run(t.Context()))

	traces := tracer.states()
	slow := findLastTraceByName(traces, "slow")
	require.NotNil(t, slow)
	assert.NoError(t, slow.CurrStepState.Error)
	assert.Equal(t, 0, slow.CurrStepState.ExitCode)
	assert.Equal(t, "success", statusSeen(t, traces, "after"), "the regular steps succeeded")
}

// A finalizer's own timeout ends it, as any step's; a failed finalizer makes
// the outcome a failure for the ones after it.
func TestFinalizerOwnTimeoutAndFailure(t *testing.T) {
	t.Parallel()
	tracer := &recorder{}
	r := New(stages(
		cmdStep("build"),
		finalizer("fails", withExitCode(2)),
		finalizer("after-failure"),
		finalizer("hangs", withSleep("30s"), withTimeout(50*time.Millisecond)),
		finalizer("after-timeout"),
	), dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)))

	require.NoError(t, r.Run(t.Context()))

	traces := tracer.states()
	assert.Equal(t, "failure", statusSeen(t, traces, "after-failure"))
	var timeoutErr *pipeline_errors.TimeoutError
	assert.True(t, errors.As(findLastTraceByName(traces, "hangs").CurrStepState.Error, &timeoutErr))
	assert.Equal(t, "timeout", statusSeen(t, traces, "after-timeout"))
	assert.Error(t, r.Err())
}

// Services stay up while the finalizers run, after a cancel too.
func TestFinalizerSeesServices(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	tracer := cancelOnStart("slow", cancel)
	r := New(&backend_types.Config{Stages: []*backend_types.Stage{
		{Steps: []*backend_types.Step{cmdStep("db", withUnboundedService())}},
		{Steps: []*backend_types.Step{cmdStep("slow", withSleep("30s"))}},
		{Steps: []*backend_types.Step{finalizer("logs", withSleep("100ms"))}, Finalizer: true},
	}}, dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)), WithContext(ctx))

	assert.ErrorIs(t, r.Run(t.Context()), pipeline_errors.ErrCancel)
	time.Sleep(200 * time.Millisecond) // the service's trace comes from its own goroutine

	traces := tracer.states()
	logsExited := indexOfTrace(traces, func(s state.State) bool {
		return s.CurrStep != nil && s.CurrStep.Name == "logs" && s.CurrStepState.Exited
	})
	dbExited := indexOfTrace(traces, func(s state.State) bool {
		return s.CurrStep != nil && s.CurrStep.Name == "db" && s.CurrStepState.Exited
	})
	require.NotEqual(t, -1, logsExited)
	require.NotEqual(t, -1, dbExited)
	assert.Less(t, logsExited, dbExited, "the service ends after the finalizer")
	assert.ErrorIs(t, traces[dbExited].CurrStepState.Error, pipeline_errors.ErrCancel, "the service was stopped by the cancel")
}

// status: skipped runs a step once a step of an earlier stage was skipped,
// not one of its own stage.
func TestStatusSkipped(t *testing.T) {
	t.Parallel()
	tracer := &recorder{}
	r := New(&backend_types.Config{Stages: []*backend_types.Stage{
		{Steps: []*backend_types.Step{
			cmdStep("only-on-failure", withOnFailure()),
			cmdStep("same-stage", onlyOn("skipped")),
		}},
		{Steps: []*backend_types.Step{cmdStep("on-skipped", onlyOn("skipped"))}},
	}}, dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)))

	require.NoError(t, r.Run(t.Context()))

	traces := tracer.states()
	assert.False(t, ran(traces, "only-on-failure"))
	assert.False(t, ran(traces, "same-stage"), "a skip in its own stage is not earlier")
	assert.True(t, ran(traces, "on-skipped"))
}

// A step skipped waiting for a skip that did not come is no skip for the
// ones after it waiting for one: no cascade.
func TestStatusSkippedNoCascade(t *testing.T) {
	t.Parallel()
	tracer := &recorder{}
	r := New(stages(
		cmdStep("build"),
		cmdStep("first", onlyOn("skipped")),
		cmdStep("second", onlyOn("skipped")),
	), dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)))

	require.NoError(t, r.Run(t.Context()))

	traces := tracer.states()
	assert.False(t, ran(traces, "first"), "nothing was skipped")
	assert.False(t, ran(traces, "second"), "first's skip is no skip")
}

// status: killed runs a finalizer after a cancel, not after a failure.
func TestStatusKilled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	tracer := cancelOnStart("slow", cancel)
	r := New(stages(
		cmdStep("slow", withSleep("30s")),
		finalizer("on-killed", onlyOn("killed")),
		finalizer("on-timeout", onlyOn("timeout")),
	), dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)), WithContext(ctx))
	assert.ErrorIs(t, r.Run(t.Context()), pipeline_errors.ErrCancel)
	traces := tracer.states()
	assert.True(t, ran(traces, "on-killed"))
	assert.False(t, ran(traces, "on-timeout"))

	tracer = &recorder{}
	r = New(stages(
		cmdStep("fails", withExitCode(1)),
		finalizer("on-killed", onlyOn("killed")),
	), dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)))
	require.NoError(t, r.Run(t.Context()))
	assert.False(t, ran(tracer.states(), "on-killed"), "a failure is no kill")
}

// The finalizers share their time: a finalizer's timeout is at most what is
// left of it, and the ones it leaves no room for are reported timed out
// without starting.
func TestFinalizeTimeout(t *testing.T) {
	t.Parallel()
	tracer := &recorder{}
	config := stages(
		cmdStep("build"),
		finalizer("hangs", withSleep("30s"), withTimeout(time.Hour)),
		finalizer("after"),
	)
	config.FinalizeTimeout = 200 * time.Millisecond
	r := New(config, dummy.New(), WithTracer(tracer), WithLogger(newTestLogger(t)))

	start := time.Now()
	require.NoError(t, r.Run(t.Context()))
	assert.Less(t, time.Since(start), 10*time.Second)

	traces := tracer.states()
	hangs := findLastTraceByName(traces, "hangs")
	require.NotNil(t, hangs)
	_, ok := errors.AsType[*pipeline_errors.TimeoutError](hangs.CurrStepState.Error)
	assert.True(t, ok, "the finalizers' time ended its own hour: %v", hangs.CurrStepState.Error)
	after := findLastTraceByName(traces, "after")
	require.NotNil(t, after)
	assert.Nil(t, findStartedTrace(traces, "after"), "no room left to start it")
	_, ok = errors.AsType[*pipeline_errors.TimeoutError](after.CurrStepState.Error)
	assert.True(t, ok)
	assert.Equal(t, pipeline_errors.TimeoutExitCode, after.CurrStepState.ExitCode)
	assert.True(t, after.CurrStepFinalizer)
}

func TestSplitFinalizers(t *testing.T) {
	t.Parallel()
	regular := &backend_types.Stage{Steps: []*backend_types.Step{cmdStep("a")}}
	final := &backend_types.Stage{Steps: []*backend_types.Step{cmdStep("d"), cmdStep("e")}, Finalizer: true}
	empty := &backend_types.Stage{}

	stages, finalizers := splitFinalizers([]*backend_types.Stage{final, regular, empty})
	assert.Equal(t, []*backend_types.Stage{regular, empty}, stages)
	assert.Equal(t, []*backend_types.Stage{final}, finalizers)
	assert.Len(t, finalizerSteps(finalizers), 2)
}

func TestStepTimeout(t *testing.T) {
	t.Parallel()
	final := cmdStep("final", withTimeout(time.Hour))
	free := cmdStep("free")
	r := New(&backend_types.Config{Stages: []*backend_types.Stage{{Steps: []*backend_types.Step{final, free}, Finalizer: true}}}, dummy.New())
	r.finalizers = finalizerSteps(r.spec.Stages)

	assert.Equal(t, time.Minute, r.stepTimeout(&backend_types.Step{Timeout: time.Minute}))
	assert.Zero(t, r.stepTimeout(&backend_types.Step{}))
	assert.Zero(t, r.stepTimeout(&backend_types.Step{Timeout: time.Minute, Detached: true}), "a detached step has none")
	assert.Equal(t, time.Hour, r.stepTimeout(final), "no finalizers' time: its own")
	assert.Zero(t, r.stepTimeout(free))

	r.finalDeadline.Store(time.Now().Add(time.Minute).UnixNano())
	assert.InDelta(t, time.Minute, r.stepTimeout(final), float64(time.Second), "at most what is left")
	assert.InDelta(t, time.Minute, r.stepTimeout(free), float64(time.Second), "what is left")
}

// A cancel that came after the last regular stage ended and before the
// finalizers started is the workflow's cancel: it does not stop them, they
// see killed, and the run reports the cancel.
func TestFinalizersCanceledInTheWindow(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(pipeline_errors.ErrCancel)
	tracer := &recorder{}
	config := stages(finalizer("logs"))
	engine := dummy.New()
	r := New(config, engine, WithTracer(tracer), WithLogger(newTestLogger(t)), WithContext(ctx), WithCancelSignal(ctx))
	require.NoError(t, engine.SetupWorkflow(t.Context(), config, r.taskUUID))
	t.Cleanup(func() { _ = engine.DestroyWorkflow(t.Context(), config, r.taskUUID) })
	r.finalizers = finalizerSteps(config.Stages)

	assert.True(t, r.runFinalizers(t.Context(), config.Stages, false), "canceled")
	assert.Equal(t, "killed", r.status())
	traces := tracer.states()
	assert.True(t, ran(traces, "logs"), "the finalizer still runs")
	assert.NoError(t, findLastTraceByName(traces, "logs").CurrStepState.Error)
	assert.Equal(t, "killed", statusSeen(t, traces, "logs"))
}

// A finalizer whose backend ignores its context past the finalizers' time
// and its grace times out: the run is not reported canceled.
func TestFinalizerOutlivingItsTime(t *testing.T) {
	t.Parallel()
	engine := mocks.NewMockBackend(t)
	engine.On("SetupWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	engine.On("StartStep", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	engine.On("TailStep", mock.Anything, mock.Anything, mock.Anything).Return(io.NopCloser(strings.NewReader("")), nil)
	engine.On("WaitStep", mock.Anything, mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { time.Sleep(400 * time.Millisecond) }). // its context ignored
		Return(&backend_types.State{Exited: true}, nil)
	engine.On("DestroyStep", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	engine.On("DestroyWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	tracer := &recorder{}
	config := stages(finalizer("hangs"))
	config.FinalizeTimeout = 100 * time.Millisecond
	r := New(config, engine, WithTracer(tracer), WithLogger(newTestLogger(t)))
	r.finalizeGrace = 100 * time.Millisecond

	require.NoError(t, r.Run(t.Context()), "not canceled")
	hangs := findLastTraceByName(tracer.states(), "hangs")
	require.NotNil(t, hangs)
	_, ok := errors.AsType[*pipeline_errors.TimeoutError](hangs.CurrStepState.Error)
	assert.True(t, ok, "timed out: %v", hangs.CurrStepState.Error)
	assert.Equal(t, "timeout", r.status())
	assert.False(t, r.FinalizeDeadline().IsZero())
}

func TestStatusPriority(t *testing.T) {
	t.Parallel()
	r := New(&backend_types.Config{}, dummy.New())
	assert.Equal(t, "success", r.status())
	r.err.Set(errors.New("failed"))
	assert.Equal(t, "failure", r.status())
	r.stepTimedOut.Store(true)
	assert.Equal(t, "timeout", r.status())
	r.stopped.Set(statusKilled)
	assert.Equal(t, "killed", r.status())
}
