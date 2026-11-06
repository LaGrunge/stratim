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

package pipeline

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/builder"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/constraint"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func old(id int64, name string, state model.StatusValue, environ map[string]string) ranWorkflow {
	return ranWorkflow{ID: id, Name: name, State: state, Environ: environ, PipelineNumber: 1}
}

func item(name string, environ map[string]string, deps ...string) *builder.Item {
	var dependsOn constraint.DependsOn
	for _, dep := range deps {
		dependsOn = append(dependsOn, constraint.Dependency{Name: dep})
	}
	return &builder.Item{
		Workflow:  &builder.Workflow{Name: name, Environ: environ},
		DependsOn: dependsOn,
	}
}

func itemNames(items []*builder.Item) []string {
	names := make([]string, 0, len(items))
	for _, it := range items {
		names = append(names, workflowKey(it.Workflow.Name, it.Workflow.Environ))
	}
	return names
}

func reusedNames(plan *restartPlan) []string {
	names := make([]string, 0, len(plan.reused))
	for _, r := range plan.reused {
		names = append(names, workflowKey(r.Name, r.Environ))
	}
	return names
}

func TestWorkflowKey(t *testing.T) {
	t.Parallel()

	assert.Equal(t, `"build"`, workflowKey("build", nil))
	assert.Equal(t, workflowKey("build", nil), workflowKey("build", make(map[string]string)))
	assert.Equal(t, `"test" "A"="1" "B"="2"`, workflowKey("test", map[string]string{"B": "2", "A": "1"}))
	// values that print alike with %v
	assert.NotEqual(t,
		workflowKey("test", map[string]string{"A": "1 B:2"}),
		workflowKey("test", map[string]string{"A": "1", "B": "2"}))
}

func TestPlanRestart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		old    []ranWorkflow
		items  []*builder.Item
		rerun  []string
		reused []string
	}{
		{
			name: "the failed workflow and its dependents",
			old: []ranWorkflow{
				old(10, "build", model.StatusSuccess, nil),
				old(11, "test", model.StatusFailure, nil),
				old(12, "deploy", model.StatusSkipped, nil),
			},
			items:  []*builder.Item{item("build", nil), item("test", nil, "build"), item("deploy", nil, "test")},
			rerun:  []string{"test", "deploy"},
			reused: []string{"build"},
		},
		{
			name: "failure, error, killed and timeout count as failed, skipped did not pass either",
			old: []ranWorkflow{
				old(1, "a", model.StatusFailure, nil),
				old(2, "b", model.StatusError, nil),
				old(3, "c", model.StatusKilled, nil),
				old(4, "d", model.StatusTimeout, nil),
				old(5, "e", model.StatusSkipped, nil),
				old(6, "f", model.StatusSuccess, nil),
			},
			items:  []*builder.Item{item("a", nil), item("b", nil), item("c", nil), item("d", nil), item("e", nil), item("f", nil)},
			rerun:  []string{"a", "b", "c", "d", "e"},
			reused: []string{"f"},
		},
		{
			name: "a dependent only in the new config is rerun and not reused",
			old: []ranWorkflow{
				old(10, "test", model.StatusFailure, nil),
				old(11, "report", model.StatusSuccess, nil),
			},
			items:  []*builder.Item{item("test", nil), item("report", nil, "test")},
			rerun:  []string{"test", "report"},
			reused: []string{},
		},
		{
			name: "a workflow removed from the config is neither rerun nor reused",
			old: []ranWorkflow{
				old(10, "build", model.StatusSuccess, nil),
				old(11, "lint", model.StatusSuccess, nil),
				old(12, "test", model.StatusFailure, nil),
			},
			items:  []*builder.Item{item("build", nil), item("test", nil)},
			rerun:  []string{"test"},
			reused: []string{"build"},
		},
		{
			name: "a workflow added to the config since runs",
			old: []ranWorkflow{
				old(10, "build", model.StatusSuccess, nil),
				old(11, "test", model.StatusFailure, nil),
			},
			items:  []*builder.Item{item("build", nil), item("test", nil), item("docs", nil, "build")},
			rerun:  []string{"test", "docs"},
			reused: []string{"build"},
		},
		{
			name: "dependents are found transitively, declared in any order",
			old: []ranWorkflow{
				old(1, "publish", model.StatusSkipped, nil),
				old(2, "deploy", model.StatusSkipped, nil),
				old(3, "test", model.StatusFailure, nil),
				old(4, "lint", model.StatusSuccess, nil),
			},
			items:  []*builder.Item{item("publish", nil, "deploy"), item("deploy", nil, "test"), item("test", nil), item("lint", nil)},
			rerun:  []string{"publish", "deploy", "test"},
			reused: []string{"lint"},
		},
		{
			name: "only the failed matrix variant, all variants of a dependent",
			old: []ranWorkflow{
				old(1, "test", model.StatusSuccess, map[string]string{"GO": "1.25"}),
				old(2, "test", model.StatusFailure, map[string]string{"GO": "1.26"}),
				old(3, "e2e", model.StatusSkipped, map[string]string{"DB": "a"}),
				old(4, "e2e", model.StatusSuccess, map[string]string{"DB": "b"}),
			},
			items: []*builder.Item{
				item("test", map[string]string{"GO": "1.25"}),
				item("test", map[string]string{"GO": "1.26"}),
				item("e2e", map[string]string{"DB": "a"}, "test"),
				item("e2e", map[string]string{"DB": "b"}, "test"),
			},
			rerun:  []string{`test GO=1.26`, `e2e DB=a`, `e2e DB=b`},
			reused: []string{`test GO=1.25`},
		},
	}

	keyOf := func(s string) string {
		name, pair, ok := strings.Cut(s, " ")
		if !ok {
			return workflowKey(name, nil)
		}
		k, v, _ := strings.Cut(pair, "=")
		return workflowKey(name, map[string]string{k: v})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			plan, err := planRestart(tt.old, tt.items)
			require.NoError(t, err)
			wantRerun := make([]string, 0, len(tt.rerun))
			for _, r := range tt.rerun {
				wantRerun = append(wantRerun, keyOf(r))
			}
			wantReused := make([]string, 0, len(tt.reused))
			for _, r := range tt.reused {
				wantReused = append(wantReused, keyOf(r))
			}
			assert.ElementsMatch(t, wantRerun, itemNames(plan.filter(tt.items)), "rerun")
			assert.ElementsMatch(t, wantReused, reusedNames(plan), "reused")
		})
	}
}

func TestPlanRestartReusedAndDepStatus(t *testing.T) {
	t.Parallel()

	oldWorkflows := []ranWorkflow{
		{ID: 10, Name: "build", State: model.StatusSuccess, PipelineNumber: 3},
		{ID: 11, Name: "lint", State: model.StatusSuccess, PipelineNumber: 5},
		{ID: 12, Name: "test", State: model.StatusSuccess, Environ: map[string]string{"GO": "1.25"}, AxisID: 1, PipelineNumber: 3},
		{ID: 13, Name: "test", State: model.StatusFailure, Environ: map[string]string{"GO": "1.26"}, AxisID: 2, PipelineNumber: 5},
		{ID: 14, Name: "deploy", State: model.StatusSkipped, PipelineNumber: 5},
	}
	items := []*builder.Item{
		item("build", nil),
		item("lint", nil),
		item("test", map[string]string{"GO": "1.25"}, "build"),
		item("test", map[string]string{"GO": "1.26"}, "build"),
		item("deploy", nil, "build", "lint", "test"),
	}

	items[2].Workflow.AxisID = 1
	items[3].Workflow.AxisID = 2

	plan, err := planRestart(oldWorkflows, items)
	require.NoError(t, err)

	assert.Equal(t, []*model.ReusedWorkflow{
		{Name: "build", PipelineNumber: 3, WorkflowID: 10, State: model.StatusSuccess},
		{Name: "lint", PipelineNumber: 5, WorkflowID: 11, State: model.StatusSuccess},
		{Name: "test", Environ: map[string]string{"GO": "1.25"}, AxisID: 1, PipelineNumber: 3, WorkflowID: 12, State: model.StatusSuccess},
	}, plan.reused)
	assert.Equal(t, map[string]map[string]model.StatusValue{
		"test":   {"10": model.StatusSuccess},
		"deploy": {"10": model.StatusSuccess, "11": model.StatusSuccess, "12": model.StatusSuccess},
	}, plan.depStatus, "the dependencies that are not rerun keep their old state, including the passed matrix variant")

	numbers := (&model.Pipeline{ReusedWorkflows: plan.reused}).ReusedWorkflowNumbers()
	assert.Equal(t, map[string]int64{"build": 3, "lint": 5, "test/1": 3}, numbers,
		"the variable names a matrix variant by its axis")
}

// TestPlanRestartReorderedMatrix restarts with a config whose matrix lists
// the variants in another order than the runs before: the reused variants
// are named by their position in this config, so they stay apart.
func TestPlanRestartReorderedMatrix(t *testing.T) {
	t.Parallel()

	go125 := map[string]string{"GO": "1.25"}
	go126 := map[string]string{"GO": "1.26"}
	go127 := map[string]string{"GO": "1.27"}
	// 1.25 passed as variant 1 of #1; the config was reordered for #2, where
	// 1.26 passed as variant 1; 1.27 failed in #2.
	oldWorkflows := []ranWorkflow{
		{ID: 20, Name: "test", Environ: go126, AxisID: 1, State: model.StatusSuccess, PipelineNumber: 2},
		{ID: 21, Name: "test", Environ: go127, AxisID: 3, State: model.StatusFailure, PipelineNumber: 2},
		{ID: 10, Name: "test", Environ: go125, AxisID: 1, State: model.StatusSuccess, PipelineNumber: 1},
	}
	items := []*builder.Item{item("test", go126), item("test", go125), item("test", go127)}
	for i, it := range items {
		it.Workflow.AxisID = i + 1
	}

	plan, err := planRestart(oldWorkflows, items)
	require.NoError(t, err)

	numbers := (&model.Pipeline{ReusedWorkflows: plan.reused}).ReusedWorkflowNumbers()
	assert.Equal(t, map[string]int64{"test/1": 2, "test/2": 1}, numbers)
	assert.Equal(t, []string{workflowKey("test", go127)}, itemNames(plan.filter(items)))
}

func TestPlanRestartRejectsDuplicateVariants(t *testing.T) {
	t.Parallel()

	same := map[string]string{"GO": "1.26"}
	_, err := planRestart(nil, []*builder.Item{item("test", same), item("test", same)})
	assert.ErrorContains(t, err, `workflow "test" twice`)
}

func TestPipelineTasksDepStatus(t *testing.T) {
	t.Parallel()

	repo := &model.Repo{ID: 1, FullName: "owner/repo"}
	it := &builder.Item{
		Workflow:  &builder.Workflow{ID: 2, Name: "deploy"},
		DependsOn: constraint.DependsOn{{Name: "test"}},
	}
	tasks, err := pipelineTasks(repo, nil, &model.Pipeline{ID: 3}, []*builder.Item{it},
		map[string]map[string]model.StatusValue{"deploy": {"10": model.StatusSkipped}})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, map[string]model.StatusValue{"10": model.StatusSkipped}, tasks[0].DepStatus)
	assert.False(t, tasks[0].ShouldRun(), "a skipped dependency outside the pipeline still gates the task")

	tasks, err = pipelineTasks(repo, nil, &model.Pipeline{ID: 3}, []*builder.Item{it}, nil)
	require.NoError(t, err)
	assert.Empty(t, tasks[0].DepStatus, "a full run has no outside dependencies")
}

// TestEffectiveWorkflows reads the workflows of the pipeline itself and the
// ones it recorded as reused; no earlier pipeline is read, so a deleted one
// changes nothing.
func TestEffectiveWorkflows(t *testing.T) {
	t.Parallel()

	partial := &model.Pipeline{ID: 5, Number: 5, Parent: 4, PartialRestart: true, ReusedWorkflows: []*model.ReusedWorkflow{
		{Name: "build", PipelineNumber: 3, WorkflowID: 1, State: model.StatusSuccess},
		{Name: "lint", PipelineNumber: 4, WorkflowID: 11, State: model.StatusSuccess},
		// rerun in this pipeline again: its own workflow wins
		{Name: "test", PipelineNumber: 3, WorkflowID: 2, State: model.StatusSuccess},
	}}

	s := store_mocks.NewMockStore(t)
	s.On("WorkflowList", partial).Return([]*model.Workflow{
		{ID: 20, Name: "test", State: model.StatusFailure},
	}, nil)

	effective, err := effectiveWorkflows(s, partial)
	require.NoError(t, err)

	got := make(map[int64]int64)
	for _, wf := range effective {
		got[wf.ID] = wf.PipelineNumber
	}
	assert.Equal(t, map[int64]int64{20: 5, 1: 3, 11: 4}, got)
	s.AssertNotCalled(t, "GetPipelineNumber", mock.Anything, mock.Anything)
	s.AssertNotCalled(t, "WorkflowGetTree", mock.Anything)
}

// TestFailRestartedPipeline ends a restarted pipeline that could not be set
// up: the queue tasks of its workflows are removed, it and its unfinished
// workflows get the error status, written to the status columns only.
func TestFailRestartedPipeline(t *testing.T) {
	q := queue.NewMemoryQueue(t.Context())
	server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), nil, q, memory.New())
	t.Cleanup(func() { server.Config.Services.Scheduler = nil })

	repo := &model.Repo{ID: 1, FullName: "owner/repo"}
	pl := &model.Pipeline{ID: 7, Number: 7, Status: model.StatusPending}
	pending := &model.Workflow{ID: 1, Name: "test", State: model.StatusPending}
	done := &model.Workflow{ID: 2, Name: "lint", State: model.StatusSuccess}

	// starting got as far as queueing the first workflow
	require.NoError(t, q.PushAtOnce(t.Context(), []*model.Task{{ID: "1", PipelineID: 7, Labels: make(map[string]string)}}))
	require.Len(t, q.Info(t.Context()).Pending, 1)

	s := store_mocks.NewMockStore(t)
	s.On("WorkflowList", pl).Return([]*model.Workflow{pending, done}, nil)
	s.On("TaskDelete", "1").Return(nil)
	s.On("TaskDelete", "2").Return(types.ErrRecordNotExist)
	s.On("WorkflowUpdate", pending).Return(nil)
	notStarted := &model.Step{ID: 11, Name: "clone", State: model.StatusPending}
	started := &model.Step{ID: 12, Name: "unit", State: model.StatusRunning, Started: 100}
	finished := &model.Step{ID: 13, Name: "lint", State: model.StatusSuccess, Finished: 90}
	s.On("StepListFromWorkflowFind", pending).Return([]*model.Step{notStarted, started, finished}, nil)
	s.On("StepUpdate", notStarted).Return(nil)
	s.On("StepUpdate", started).Return(nil)
	s.On("UpdatePipelineStatus", pl).Return(nil)
	s.On("WorkflowGetTree", mock.Anything).Return([]*model.Workflow{pending, done}, nil)

	require.NoError(t, failRestartedPipeline(t.Context(), s, pl, repo, errors.New("cannot enqueue")))

	assert.Empty(t, q.Info(t.Context()).Pending, "the queued task is gone")
	s.AssertCalled(t, "TaskDelete", "1")
	assert.Equal(t, model.StatusError, pl.Status)
	require.Len(t, pl.Errors, 1)
	assert.Contains(t, pl.Errors[0].Message, "cannot enqueue")
	assert.NotZero(t, pl.Finished)
	assert.Equal(t, model.StatusError, pending.State)
	assert.Equal(t, "cannot enqueue", pending.Error)
	assert.NotZero(t, pending.Finished)
	assert.Equal(t, model.StatusSuccess, done.State, "finished workflows are left as they are")
	s.AssertNumberOfCalls(t, "WorkflowUpdate", 1)
	s.AssertNotCalled(t, "StepListFromWorkflowFind", done)
	assert.Equal(t, model.StatusCanceled, notStarted.State, "a step that did not start is canceled")
	assert.Equal(t, model.StatusKilled, started.State, "a step that started is killed")
	assert.NotZero(t, started.Finished)
	assert.Equal(t, model.StatusSuccess, finished.State, "a finished step is left as it is")
	s.AssertNumberOfCalls(t, "StepUpdate", 2)
	s.AssertNotCalled(t, "UpdatePipeline", mock.Anything)
}

func TestFailRestartedPipelineWriteFails(t *testing.T) {
	server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), nil, queue.NewMemoryQueue(t.Context()), memory.New())
	t.Cleanup(func() { server.Config.Services.Scheduler = nil })

	pl := &model.Pipeline{ID: 7, Number: 7, Status: model.StatusPending}
	s := store_mocks.NewMockStore(t)
	s.On("WorkflowList", pl).Return(nil, nil)
	s.On("UpdatePipelineStatus", pl).Return(errors.New("db down"))

	err := failRestartedPipeline(t.Context(), s, pl, &model.Repo{ID: 1}, errors.New("cannot enqueue"))
	assert.ErrorContains(t, err, "db down", "a pipeline that could not be ended is reported")
}
