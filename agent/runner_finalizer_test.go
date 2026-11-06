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

package agent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/dummy"
	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/shared/constant"
)

// withFinalizer adds to workflow a finalizer sleeping for sleep.
func withFinalizer(workflow *rpc.Workflow, sleep string) *rpc.Workflow {
	workflow.Config.Stages = append(workflow.Config.Stages, &backend_types.Stage{
		Steps: []*backend_types.Step{{
			Name:      "logs",
			UUID:      "logs-uuid",
			Type:      backend_types.StepTypeCommands,
			OnSuccess: true,
			OnFailure: true,
			Environment: map[string]string{
				"CI_REPO":             "octocat/hello-world",
				"CI_PIPELINE_NUMBER":  "1",
				dummy.EnvKeyStepSleep: sleep,
			},
		}},
		Finalizer: true,
	})
	return workflow
}

// A finalizer runs after the server canceled the workflow and is reported as
// a step that ran to its end, not as canceled; the workflow stays canceled.
func TestRunFinalizerAfterCancel(t *testing.T) {
	got := runOnDummy(t, withFinalizer(sleepingWorkflow(0, 0), "10ms"), 0, true)

	assert.True(t, got.steps["step-uuid"].Canceled)
	logs := got.steps["logs-uuid"]
	assert.True(t, logs.Exited, "the finalizer ran")
	assert.False(t, logs.Canceled)
	assert.False(t, logs.TimedOut)
	assert.Equal(t, 0, logs.ExitCode)
	assert.Empty(t, logs.Error)
	assert.True(t, got.done.Canceled)
	assert.False(t, got.done.TimedOut)
}

// A finalizer runs after the workflow timed out, past the deadline, and the
// lease is extended meanwhile: the queue would hand the workflow to another
// agent otherwise.
func TestRunFinalizerAfterWorkflowTimeout(t *testing.T) {
	saved := constant.TaskTimeout
	constant.TaskTimeout = 150 * time.Millisecond
	t.Cleanup(func() { constant.TaskTimeout = saved })

	got := runOnDummy(t, withFinalizer(sleepingWorkflow(1, 0), "600ms"), 200*time.Millisecond, false)

	assert.True(t, got.steps["step-uuid"].TimedOut, "the step the timeout stopped")
	logs := got.steps["logs-uuid"]
	assert.True(t, logs.Exited)
	assert.False(t, logs.Canceled)
	assert.False(t, logs.TimedOut, "the finalizer did not time out")
	assert.True(t, got.done.Canceled)
	assert.True(t, got.done.TimedOut)

	got.Lock()
	defer got.Unlock()
	require.NotEmpty(t, got.extended)
	assert.Greater(t, got.extended[len(got.extended)-1], 400*time.Millisecond,
		"the lease is still extended while the finalizer runs after the deadline")
}

// A cancel that comes while a finalizer runs after the workflow's timeout
// still stops it: the cancel is heard until the run ends. The finalizer is
// killed, not timed out: it outlived the workflow's timeout on purpose.
func TestRunFinalizerCanceledAfterWorkflowTimeout(t *testing.T) {
	start := time.Now()
	got := runOnDummyCancelAfter(t, withFinalizer(sleepingWorkflow(1, 0), "30s"), 200*time.Millisecond, 600*time.Millisecond)
	assert.Less(t, time.Since(start), 10*time.Second, "the cancel stopped the 30s finalizer")

	logs := got.steps["logs-uuid"]
	assert.True(t, logs.Canceled, "the cancel stopped the finalizer")
	assert.False(t, logs.TimedOut, "a stopped finalizer is killed, not timed out")
	assert.True(t, got.done.Canceled)
}

// A second cancel stops the finalizers the first one left running: the
// agent waits again after the first.
func TestRunFinalizerSecondCancel(t *testing.T) {
	start := time.Now()
	// the mocked server cancels 200ms after each Wait: the second comes
	// while the 30s finalizer runs
	got := runOnDummyCancelAfter(t, withFinalizer(sleepingWorkflow(0, 0), "30s"), 0, 200*time.Millisecond)
	assert.Less(t, time.Since(start), 10*time.Second, "the second cancel stopped the 30s finalizer")

	logs := got.steps["logs-uuid"]
	assert.True(t, logs.Exited)
	assert.True(t, logs.Canceled, "the second cancel stopped the finalizer")
	assert.False(t, logs.TimedOut)
	assert.True(t, got.done.Canceled)
}

func TestLeaseExtendable(t *testing.T) {
	now := time.Now()
	assert.True(t, leaseExtendable(true, time.Time{}, now, 0), "no finalizers' time running")
	deadline := now.Add(-time.Minute)
	assert.True(t, leaseExtendable(true, deadline, now, 2*time.Minute), "within the grace past their time")
	assert.True(t, leaseExtendable(true, deadline, now, time.Minute), "at the end of the grace, still")
	assert.False(t, leaseExtendable(true, deadline, now, time.Minute-time.Nanosecond), "past it")
	assert.True(t, leaseExtendable(false, deadline, now, 0), "not canceled: held until the run ended")
}

// The lease is extended while the finalizers run, counted from when they
// started, past the grace after the workflow's own stop: they have their
// time, and the grace follows it.
func TestRunLeaseExtendedThroughFinalizers(t *testing.T) {
	saved := constant.TaskTimeout
	constant.TaskTimeout = 150 * time.Millisecond
	t.Cleanup(func() { constant.TaskTimeout = saved })

	workflow := withFinalizer(sleepingWorkflow(1, 0), "800ms")
	workflow.Config.FinalizeTimeout = 2 * time.Second
	got := runOnDummyWith(t, workflow, dummyRun{unit: 200 * time.Millisecond, leaseGrace: 100 * time.Millisecond})

	assert.True(t, got.done.TimedOut)
	got.Lock()
	defer got.Unlock()
	require.NotEmpty(t, got.extended)
	assert.Greater(t, got.extended[len(got.extended)-1], 700*time.Millisecond,
		"extended while the finalizer runs, well past the grace after the workflow's stop at 200ms")
	require.NotEmpty(t, got.finalizeDeadlines, "the end of the finalizers' time is told to the server")
	// their start, their 2s and the runtime's grace of 30s
	assert.WithinDuration(t, time.Now().Add(32*time.Second), got.finalizeDeadlines[0], 3*time.Second)
}
