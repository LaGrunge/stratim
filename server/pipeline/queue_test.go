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

package pipeline

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/builder"
	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// TestWorkflowTimeout: a workflow's own timeout, rounded up to minutes, in
// place of the repository's, which is also its ceiling.
func TestWorkflowTimeout(t *testing.T) {
	for _, tt := range []struct {
		own  time.Duration
		repo int64
		want int64
	}{
		{0, 60, 60},
		{10 * time.Minute, 60, 10},
		{60 * time.Second, 60, 1},
		{61 * time.Second, 60, 2},
		{time.Nanosecond, 60, 1},
		{60 * time.Minute, 60, 60},
		{90 * time.Minute, 60, 60},
		{time.Duration(1<<63 - 1), 60, 60},
		{90 * time.Minute, 0, 90},
	} {
		assert.Equal(t, tt.want, workflowTimeout(tt.own, tt.repo), "own %s, repository %d", tt.own, tt.repo)
	}
}

func TestQueuePipelineConcurrency(t *testing.T) {
	repo := &model.Repo{ID: 7}
	activePipeline := &model.Pipeline{ID: 42}

	tests := []struct {
		name          string
		item          *builder.Item
		expectedLimit int
		expectedGroup string
	}{
		{
			name: "no limit leaves concurrency unset",
			item: &builder.Item{
				Workflow: &builder.Workflow{ID: 1, Name: "build"},
			},
			expectedLimit: 0,
			expectedGroup: "",
		},
		{
			name: "explicit group is scoped to the repo",
			item: &builder.Item{
				Workflow:         &builder.Workflow{ID: 2, Name: "build"},
				ConcurrencyLimit: 2,
				ConcurrencyGroup: "deploy",
			},
			expectedLimit: 2,
			expectedGroup: "7//deploy",
		},
		{
			name: "empty group defaults to the workflow name",
			item: &builder.Item{
				Workflow:         &builder.Workflow{ID: 3, Name: "test"},
				ConcurrencyLimit: 1,
			},
			expectedLimit: 1,
			expectedGroup: "7/test/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tasks, err := pipelineTasks(repo, nil, activePipeline, []*builder.Item{tt.item}, nil)
			require.NoError(t, err)
			require.Len(t, tasks, 1)

			task := tasks[0]
			assert.Equal(t, tt.expectedLimit, task.ConcurrencyLimit)
			assert.Equal(t, tt.expectedGroup, task.ConcurrencyGroup)
		})
	}
}

// TestFinalizeTimeout: the finalizers' time is at most the workflow's own
// timeout, the repository's policy.
func TestFinalizeTimeout(t *testing.T) {
	assert.Equal(t, 10*time.Minute, finalizeTimeout(10*time.Minute, 60))
	assert.Equal(t, 5*time.Minute, finalizeTimeout(10*time.Minute, 5), "a 5 minute workflow gets 5 for its finalizers")
	assert.Equal(t, 10*time.Minute, finalizeTimeout(10*time.Minute, 0), "no timeout set: the compiler's")
	assert.Zero(t, finalizeTimeout(0, 60), "no finalizers")
}

// The task carries the finalizers' time, bounded by the workflow's, and so
// does the config the agent gets.
func TestQueuePipelineFinalizeTimeout(t *testing.T) {
	repo := &model.Repo{ID: 7, Timeout: 5}
	item := &builder.Item{Workflow: &builder.Workflow{ID: 1, Name: "build"}}
	// the server does not name the backend types: the compiled config, as JSON
	require.NoError(t, json.Unmarshal([]byte(`{"finalize_timeout": 600000000000}`), &item.Config), "10 minutes")
	tasks, err := pipelineTasks(repo, nil, &model.Pipeline{ID: 42}, []*builder.Item{item}, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, int64(5*60), tasks[0].FinalizeTimeout)
	var workflow rpc.Workflow
	require.NoError(t, json.Unmarshal(tasks[0].Data, &workflow))
	assert.Equal(t, 5*time.Minute, workflow.Config.FinalizeTimeout)
}

func TestQueuePipelineCreated(t *testing.T) {
	repo := &model.Repo{ID: 7}
	item := &builder.Item{Workflow: &builder.Workflow{ID: 1, Name: "build"}}

	runOnce := func(t *testing.T, pipeline *model.Pipeline) *model.Task {
		t.Helper()
		tasks, err := pipelineTasks(repo, nil, pipeline, []*builder.Item{item}, nil)
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		return tasks[0]
	}

	t.Run("inherits the pipeline creation time", func(t *testing.T) {
		task := runOnce(t, &model.Pipeline{ID: 42, Created: 1700000000})
		assert.Equal(t, int64(1700000000), task.Created)
	})

	t.Run("falls back to now when the pipeline has no creation time", func(t *testing.T) {
		before := time.Now().Unix()
		task := runOnce(t, &model.Pipeline{ID: 42})
		assert.GreaterOrEqual(t, task.Created, before)
	})
}

func TestQueuePipelineOrgAgentsOnly(t *testing.T) {
	repo := &model.Repo{ID: 7, OrgID: 3}
	activePipeline := &model.Pipeline{ID: 42}

	newItem := func() *builder.Item {
		return &builder.Item{
			Workflow: &builder.Workflow{ID: 1, Name: "build"},
			Labels:   map[string]string{"platform": "linux"},
		}
	}

	tests := []struct {
		name     string
		org      *model.Org
		expected bool
	}{
		{name: "no org", org: nil, expected: false},
		{name: "org not requiring org agents", org: &model.Org{ID: 3}, expected: false},
		{name: "org requiring org agents", org: &model.Org{ID: 3, RequireOrgAgents: true}, expected: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tasks, err := pipelineTasks(repo, tt.org, activePipeline, []*builder.Item{newItem()}, nil)
			require.NoError(t, err)
			require.Len(t, tasks, 1)

			value, ok := tasks[0].Labels[pipeline.LabelOrgAgentsOnly]
			assert.Equal(t, tt.expected, ok)
			if tt.expected {
				assert.Equal(t, "true", value)
			}
			assert.Equal(t, "3", tasks[0].Labels[pipeline.LabelFilterOrg])
		})
	}
}
