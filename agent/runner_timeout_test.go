// Copyright 2026 LaGrunge
// Copyright 2018 Drone.IO Inc.
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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/dummy"
	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	rpc_mocks "go.woodpecker-ci.org/woodpecker/v3/rpc/mocks"
)

func TestWorkflowTimeout(t *testing.T) {
	assert.Equal(t, time.Hour, workflowTimeout(0, 0), "no timeout set: an hour")
	assert.Equal(t, time.Hour, workflowTimeout(0, time.Second), "no timeout set, whatever the unit")
	assert.Equal(t, 2*time.Minute, workflowTimeout(2, 0), "the server counts in minutes")
	assert.Equal(t, 2*time.Second, workflowTimeout(2, time.Second), "a test unit replaces the minute")
	assert.Equal(t, 2*time.Second, Runner{}.WithTimeoutUnit(time.Second).timeoutUnit*2, "WithTimeoutUnit sets the unit")
}

// reported is what a run reported to the server: the last state of each
// step and the workflow's final state.
type reported struct {
	sync.Mutex
	steps map[string]rpc.StepState
	done  rpc.WorkflowState
	// extended is when the lease was extended, since the run started.
	extended []time.Duration
	// finalizeDeadlines are the ends of the finalizers' time the agent told
	// when extending.
	finalizeDeadlines []time.Time
}

// runOnDummy runs workflow on the dummy backend against a mocked server,
// which cancels the workflow 200ms after its Wait is called when cancel is
// set and otherwise waits for the workflow to end.
func runOnDummy(t *testing.T, workflow *rpc.Workflow, unit time.Duration, cancel bool) *reported {
	t.Helper()
	var cancelAfter time.Duration
	if cancel {
		cancelAfter = 200 * time.Millisecond
	}
	return runOnDummyCancelAfter(t, workflow, unit, cancelAfter)
}

// runOnDummyCancelAfter is runOnDummy with the server canceling the
// workflow cancelAfter after its Wait is called, unless the run ended
// before; zero for never. It stops the finalizers too cancelAfter after
// the agent waits for that.
func runOnDummyCancelAfter(t *testing.T, workflow *rpc.Workflow, unit, cancelAfter time.Duration) *reported {
	t.Helper()
	return runOnDummyWith(t, workflow, dummyRun{unit: unit, cancelAfter: cancelAfter})
}

// dummyRun is how runOnDummyWith runs a workflow.
type dummyRun struct {
	// unit is what one unit of the workflow's timeout lasts.
	unit time.Duration
	// cancelAfter is when the server cancels the workflow, after its Wait
	// is called; zero for never. A WaitKilled returns true as long after.
	cancelAfter time.Duration
	// leaseGrace is the runner's finalizeLeaseGrace, zero for the default.
	leaseGrace time.Duration
}

// runOnDummyWith runs workflow on the dummy backend against a mocked
// server, as run says.
func runOnDummyWith(t *testing.T, workflow *rpc.Workflow, run dummyRun) *reported {
	t.Helper()
	cancelAfter := run.cancelAfter
	got := &reported{steps: make(map[string]rpc.StepState)}

	peer := rpc_mocks.NewMockPeer(t)
	peer.On("Next", mock.Anything, mock.Anything).Return(workflow, nil)
	peer.On("Init", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	start := time.Now()
	peer.On("Extend", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) {
			got.Lock()
			got.extended = append(got.extended, time.Since(start))
			got.Unlock()
		}).
		Return(nil).Maybe()
	peer.On("ExtendFinalizing", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			deadline, _ := args.Get(2).(time.Time)
			got.Lock()
			got.extended = append(got.extended, time.Since(start))
			got.finalizeDeadlines = append(got.finalizeDeadlines, deadline)
			got.Unlock()
		}).
		Return(nil).Maybe()
	peer.On("EnqueueLog", mock.Anything).Maybe()
	peer.EXPECT().Wait(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ string) (bool, error) {
		if cancelAfter == 0 {
			<-ctx.Done()
			return false, nil
		}
		select {
		case <-time.After(cancelAfter):
			return true, nil
		case <-ctx.Done():
			return false, nil
		}
	})
	peer.EXPECT().WaitKilled(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ string) (bool, error) {
		select {
		case <-time.After(cancelAfter):
			return true, nil
		case <-ctx.Done():
			return false, nil
		}
	}).Maybe()
	peer.On("Update", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			state, ok := args.Get(2).(rpc.StepState)
			require.True(t, ok)
			got.Lock()
			got.steps[state.StepUUID] = state
			got.Unlock()
		}).
		Return(nil)
	peer.On("Done", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			state, ok := args.Get(2).(rpc.WorkflowState)
			require.True(t, ok)
			got.Lock()
			got.done = state
			got.Unlock()
		}).
		Return(nil)

	counter := &State{Metadata: make(map[string]Info)}
	runner := NewRunner(peer, rpc.Filter{}, "test-agent", 1, counter, dummy.New()).
		WithTimeoutUnit(run.unit).
		WithFinalizeLeaseGrace(run.leaseGrace)
	require.NoError(t, runner.Run(t.Context()))
	return got
}

// sleepingWorkflow is a workflow of one step sleeping for 30s, which
// timeout ends after stepTimeout if set.
func sleepingWorkflow(timeout int64, stepTimeout time.Duration) *rpc.Workflow {
	return &rpc.Workflow{
		ID:      "1",
		Timeout: timeout,
		Config: &backend_types.Config{
			Stages: []*backend_types.Stage{{
				Steps: []*backend_types.Step{{
					Name:      "sleep",
					UUID:      "step-uuid",
					Type:      backend_types.StepTypeCommands,
					OnSuccess: true,
					Timeout:   stepTimeout,
					Environment: map[string]string{
						"CI_REPO":             "octocat/hello-world",
						"CI_PIPELINE_NUMBER":  "1",
						dummy.EnvKeyStepSleep: "30s",
					},
				}},
			}},
		},
	}
}

// A step ended by its own timeout is reported timed out, the workflow it
// ran in is not: it ended by itself.
func TestRunReportsStepTimeout(t *testing.T) {
	got := runOnDummy(t, sleepingWorkflow(0, 100*time.Millisecond), 0, false)

	step := got.steps["step-uuid"]
	assert.True(t, step.TimedOut, "the step timed out")
	assert.False(t, step.Canceled, "a timed out step is not canceled")
	assert.Equal(t, 124, step.ExitCode)
	assert.False(t, got.done.TimedOut, "the workflow ended before its timeout")
	assert.False(t, got.done.Canceled)
}

// A workflow ended by its timeout is reported canceled and timed out, as is
// the step it stopped.
func TestRunReportsWorkflowTimeout(t *testing.T) {
	got := runOnDummy(t, sleepingWorkflow(1, 0), 200*time.Millisecond, false)

	step := got.steps["step-uuid"]
	assert.True(t, step.Canceled, "the step was stopped")
	assert.True(t, step.TimedOut, "by the workflow's timeout")
	assert.True(t, got.done.Canceled, "the workflow was stopped")
	assert.True(t, got.done.TimedOut, "by its timeout")
	assert.Empty(t, got.done.Error, "a timeout is not a workflow error")
}

// A workflow the server canceled did not time out, nor did its steps.
func TestRunReportsCancelNotTimedOut(t *testing.T) {
	got := runOnDummy(t, sleepingWorkflow(0, 0), 0, true)

	step := got.steps["step-uuid"]
	assert.True(t, step.Canceled)
	assert.False(t, step.TimedOut)
	assert.True(t, got.done.Canceled)
	assert.False(t, got.done.TimedOut)
}
