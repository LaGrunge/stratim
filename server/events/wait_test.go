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

package events

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func TestParseUntil(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want Until
	}{
		{"", Until{Kind: UntilFinished}},
		{"finished", Until{Kind: UntilFinished}},
		{"failed", Until{Kind: UntilFailed}},
		{"step:test", Until{Kind: UntilStep, Step: "test"}},
		{"step:build/test", Until{Kind: UntilStep, Step: "build/test"}},
		{"step:a:b", Until{Kind: UntilStep, Step: "a:b"}},
	} {
		got, err := ParseUntil(tt.in)
		require.NoError(t, err, tt.in)
		assert.Equal(t, tt.want, got, tt.in)
		if tt.in != "" {
			assert.Equal(t, tt.in, got.String())
		}
	}
	_, err := ParseUntil("step:" + strings.Repeat("x", MaxUntilStepBytes))
	require.NoError(t, err, "the longest step name")
	for _, bad := range []string{"done", "step", "step:", "step: ", "Finished", "failed,finished", "step:" + strings.Repeat("x", MaxUntilStepBytes+1)} {
		_, err := ParseUntil(bad)
		assert.Error(t, err, bad)
	}
}

func TestUntilTypes(t *testing.T) {
	for _, u := range []Until{{Kind: UntilFinished}, {Kind: UntilFailed}, {Kind: UntilStep, Step: "x"}} {
		assert.True(t, slices.Contains(u.Types(), PipelineFinished), "%s ends with the pipeline", u)
	}
	assert.Equal(t, []Type{PipelineFinished}, Until{Kind: UntilFinished}.Types())
}

func TestUntilCheck(t *testing.T) {
	running := func(workflows ...*model.Workflow) *model.Pipeline {
		return pl(model.StatusRunning, 0, -1, workflows...)
	}
	finished, failed := Until{Kind: UntilFinished}, Until{Kind: UntilFailed}
	step := func(name string) Until { return Until{Kind: UntilStep, Step: name} }
	waiting := Outcome{}

	for _, tt := range []struct {
		name  string
		until Until
		p     *model.Pipeline
		want  Outcome
	}{
		{"finished: running", finished, running(wf(1, "build", model.StatusRunning, 0, -1, st(11, "test", model.StatusRunning))), waiting},
		{"finished: blocked", finished, pl(model.StatusBlocked, -1, -1), waiting},
		{"finished: success", finished, pl(model.StatusSuccess, 0, 60), Outcome{Done: true, Met: true, Reason: ReasonPipelineFinished}},
		{"finished: killed", finished, pl(model.StatusKilled, 0, 60), Outcome{Done: true, Met: true, Reason: ReasonPipelineFinished}},

		{"failed: running", failed, running(wf(1, "build", model.StatusRunning, 0, -1, st(11, "test", model.StatusSuccess))), waiting},
		{
			"failed: a step failed, the pipeline runs", failed,
			running(wf(1, "build", model.StatusRunning, 0, -1, st(11, "lint", model.StatusFailure), st(12, "test", model.StatusRunning))),
			Outcome{Done: true, Met: true, Reason: ReasonStepFailed},
		},
		{
			"failed: a step timed out", failed,
			running(wf(1, "build", model.StatusRunning, 0, -1, st(11, "lint", model.StatusTimeout))),
			Outcome{Done: true, Met: true, Reason: ReasonStepFailed},
		},
		{
			"failed: an ignored failure", failed,
			running(wf(1, "build", model.StatusRunning, 0, -1, st(11, "lint", model.StatusFailure, ignored()), st(12, "test", model.StatusRunning))),
			waiting,
		},
		{
			"failed: a killed step", failed,
			running(wf(1, "build", model.StatusRunning, 0, -1, st(11, "lint", model.StatusKilled))),
			waiting,
		},
		{
			"failed: a workflow failed without a step", failed,
			running(wf(1, "build", model.StatusError, 0, 5, st(11, "lint", model.StatusCanceled))),
			Outcome{Done: true, Met: true, Reason: ReasonWorkflowFailed},
		},
		{"failed: a config error", failed, pl(model.StatusError, -1, 0), Outcome{Done: true, Met: true, Reason: ReasonPipelineFailed}},
		{
			"failed: the pipeline succeeded", failed,
			pl(model.StatusSuccess, 0, 60, wf(1, "build", model.StatusSuccess, 0, 60, st(11, "lint", model.StatusFailure, ignored()))),
			Outcome{Done: true, Reason: ReasonPipelineFinished},
		},
		{"failed: the pipeline was canceled", failed, pl(model.StatusKilled, 0, 60), Outcome{Done: true, Reason: ReasonPipelineFinished}},

		{
			"step: running", step("test"),
			running(wf(1, "build", model.StatusRunning, 0, -1, st(11, "test", model.StatusRunning))),
			waiting,
		},
		{
			"step: pending", step("test"),
			pl(model.StatusPending, -1, -1, wf(1, "build", model.StatusPending, -1, -1, st(11, "test", model.StatusPending))),
			waiting,
		},
		{
			"step: succeeded", step("test"),
			running(wf(1, "build", model.StatusRunning, 0, -1, st(11, "test", model.StatusSuccess), st(12, "deploy", model.StatusRunning))),
			Outcome{Done: true, Met: true, Reason: ReasonStepFinished, StepStatus: model.StatusSuccess},
		},
		{
			"step: failed", step("test"),
			running(wf(1, "build", model.StatusRunning, 0, -1, st(11, "test", model.StatusFailure))),
			Outcome{Done: true, Met: true, Reason: ReasonStepFinished, StepStatus: model.StatusFailure},
		},
		{
			"step: skipped", step("test"),
			running(wf(1, "build", model.StatusRunning, 0, -1, st(11, "test", model.StatusSkipped))),
			Outcome{Done: true, Met: true, Reason: ReasonStepFinished, StepStatus: model.StatusSkipped},
		},
		{
			"step: of a matrix, one of two ended", step("test"),
			running(
				wf(1, "build", model.StatusRunning, 0, -1, st(11, "test", model.StatusSuccess)),
				wf(2, "build", model.StatusRunning, 0, -1, st(21, "test", model.StatusRunning)),
			),
			waiting,
		},
		{
			"step: of a workflow", step("lint/test"),
			running(
				wf(1, "build", model.StatusRunning, 0, -1, st(11, "test", model.StatusRunning)),
				wf(2, "lint", model.StatusRunning, 0, -1, st(21, "test", model.StatusSuccess)),
			),
			Outcome{Done: true, Met: true, Reason: ReasonStepFinished, StepStatus: model.StatusSuccess},
		},
		{
			"step: of a matrix, one failed, one skipped, one succeeded", step("test"),
			running(
				wf(1, "build", model.StatusRunning, 0, -1, st(11, "test", model.StatusSkipped)),
				wf(2, "build", model.StatusRunning, 0, -1, st(21, "test", model.StatusFailure, ignored())),
				wf(3, "build", model.StatusRunning, 0, -1, st(31, "test", model.StatusSuccess)),
			),
			Outcome{Done: true, Met: true, Reason: ReasonStepFinished, StepStatus: model.StatusFailure},
		},
		{
			"step: of a matrix, skipped and succeeded", step("test"),
			running(
				wf(1, "build", model.StatusRunning, 0, -1, st(11, "test", model.StatusSkipped)),
				wf(2, "build", model.StatusRunning, 0, -1, st(21, "test", model.StatusSuccess)),
			),
			Outcome{Done: true, Met: true, Reason: ReasonStepFinished, StepStatus: model.StatusSuccess},
		},
		{
			"step: killed", step("test"),
			pl(model.StatusKilled, 0, 9, wf(1, "build", model.StatusKilled, 0, 9, st(11, "test", model.StatusKilled))),
			Outcome{Done: true, Met: true, Reason: ReasonStepFinished, StepStatus: model.StatusKilled},
		},
		{
			"step: none of the name", step("deploy"),
			running(wf(1, "build", model.StatusRunning, 0, -1, st(11, "test", model.StatusRunning))),
			Outcome{Done: true, Reason: ReasonNoSuchStep},
		},
		{"step: no steps loaded yet", step("deploy"), pl(model.StatusPending, -1, -1), waiting},
		{"step: a config error", step("deploy"), pl(model.StatusError, -1, 0), Outcome{Done: true, Reason: ReasonPipelineFinished}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.until.Check(tt.p))
		})
	}
}

func TestUntilWatch(t *testing.T) {
	p := pl(model.StatusRunning, 0, -1,
		wf(1, "build", model.StatusRunning, 0, -1, st(11, "test", model.StatusRunning), st(12, "lint", model.StatusRunning)),
		wf(2, "deploy", model.StatusPending, -1, -1, st(21, "push", model.StatusPending)),
	)
	ev := func(ty Type, workflowID, stepID int64, status model.StatusValue, ignored bool) *Event {
		return &Event{Type: ty, WorkflowID: workflowID, StepID: stepID, Status: string(status), Ignored: ignored}
	}
	finished := ev(PipelineFinished, 0, 0, model.StatusSuccess, false)
	stepOK := ev(StepFinished, 1, 11, model.StatusSuccess, false)
	otherStepOK := ev(StepFinished, 1, 12, model.StatusSuccess, false)
	stepFailed := ev(StepFailed, 1, 12, model.StatusFailure, false)
	ignoredFailure := ev(StepFailed, 1, 12, model.StatusFailure, true)
	workflowOK := ev(WorkflowFinished, 1, 0, model.StatusSuccess, false)
	otherWorkflowOK := ev(WorkflowFinished, 2, 0, model.StatusSuccess, false)
	workflowFailed := ev(WorkflowFinished, 2, 0, model.StatusFailure, false)
	workflowKilled := ev(WorkflowFinished, 2, 0, model.StatusKilled, false)

	for _, tt := range []struct {
		name  string
		until Until
		p     *model.Pipeline
		want  map[*Event]bool
	}{
		{"finished", Until{Kind: UntilFinished}, p, map[*Event]bool{
			finished: true, stepOK: false, stepFailed: false, workflowFailed: false,
		}},
		{"failed", Until{Kind: UntilFailed}, p, map[*Event]bool{
			finished: true, stepFailed: true, workflowFailed: true,
			stepOK: false, ignoredFailure: false, workflowOK: false, workflowKilled: false,
		}},
		{"step: its events and its workflow's end", Until{Kind: UntilStep, Step: "test"}, p, map[*Event]bool{
			finished: true, stepOK: true, workflowOK: true,
			otherStepOK: false, stepFailed: false, otherWorkflowOK: false,
		}},
		{"step: any while the steps are not known", Until{Kind: UntilStep, Step: "test"}, pl(model.StatusPending, -1, -1), map[*Event]bool{
			finished: true, stepOK: true, otherStepOK: true, workflowOK: true, otherWorkflowOK: true,
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			relevant := tt.until.Watch(tt.p)
			for e, want := range tt.want {
				assert.Equal(t, want, relevant(e), "%s of step %d, workflow %d, %s", e.Type, e.StepID, e.WorkflowID, e.Status)
			}
		})
	}
}
