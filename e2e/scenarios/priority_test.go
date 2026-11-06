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

package scenarios

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

// A workflow on the branch low with no priority, and one on the branch high
// with priority 5: a push to each branch queues one of them. Each sleeps a
// second, so that one agent running one at a time starts them in different
// seconds.
var (
	priorityLowYAML = []byte(`
skip_clone: true
when:
  branch: low
steps:
  - name: work
    image: dummy
    environment:
      SLEEP: 1s
    commands:
      - echo low
`)

	priorityHighYAML = []byte(`
skip_clone: true
when:
  branch: high
priority: 5
steps:
  - name: work
    image: dummy
    environment:
      SLEEP: 1s
    commands:
      - echo high
`)
)

func startPriorityServer(t *testing.T) *setup.ServerEnv {
	t.Helper()
	return setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker/low.yaml", Data: priorityLowYAML},
		{Name: ".woodpecker/high.yaml", Data: priorityHighYAML},
	})
}

// createOnBranch creates a push pipeline on branch, its workflows queued
// with no agent yet to take them.
func createOnBranch(t *testing.T, env *setup.ServerEnv, branch string) *model.Pipeline {
	t.Helper()
	draft := env.DummyPipeline(model.EventPush)
	draft.Branch = branch
	draft.Ref = "refs/heads/" + branch
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, draft)
	require.NoError(t, err, "create pipeline on %s", branch)
	require.NotNil(t, created)
	return created
}

// startOrder starts one agent that runs one workflow at a time, waits for
// the pipelines to succeed, and returns their ids in the order their
// workflows started.
func startOrder(t *testing.T, env *setup.ServerEnv, pipelines ...*model.Pipeline) []int64 {
	t.Helper()
	agent := setup.StartAgent(t, env.GRPCAddr, setup.WithCapacity(1))
	setup.WaitForAgentRegistered(t, env.Store, agent)

	started := make(map[int64]int64, len(pipelines))
	for _, p := range pipelines {
		finished := setup.WaitForPipeline(t, env.Store, p.ID)
		require.Equal(t, model.StatusSuccess, finished.Status, "pipeline %d", p.Number)
		workflows, err := env.Store.WorkflowGetTree(finished)
		require.NoError(t, err)
		require.Len(t, workflows, 1)
		require.NotZero(t, workflows[0].Started)
		started[p.ID] = workflows[0].Started
	}
	order := make([]int64, 0, len(pipelines))
	for _, p := range pipelines {
		order = append(order, p.ID)
	}
	slices.SortFunc(order, func(a, b int64) int { return int(started[a] - started[b]) })
	for i := 1; i < len(order); i++ {
		require.Less(t, started[order[i-1]], started[order[i]], "one agent slot runs one workflow at a time")
	}
	return order
}

// TestQueuePriority: two pipelines wait for one agent; the one whose
// workflow has the higher priority runs first, though created second.
func TestQueuePriority(t *testing.T) {
	env := startPriorityServer(t)
	low := createOnBranch(t, env, "low")
	high := createOnBranch(t, env, "high")

	info := env.Queue.Info(t.Context())
	require.Len(t, info.Pending, 2, "both wait for an agent")

	assert.Equal(t, []int64{high.ID, low.ID}, startOrder(t, env, low, high))
}

// TestQueueMoveToTop: of three pipelines of the same priority waiting for
// one agent, the last created is moved to the top and runs first; the
// others run in the order they were created. One moved runs before one of a
// higher priority too.
func TestQueueMoveToTop(t *testing.T) {
	env := startPriorityServer(t)
	first := createOnBranch(t, env, "low")
	high := createOnBranch(t, env, "high")
	second := createOnBranch(t, env, "low")
	moved := createOnBranch(t, env, "low")

	require.NoError(t, pipeline.Prioritize(t.Context(), env.Store, moved))

	assert.Equal(t, []int64{moved.ID, high.ID, first.ID, second.ID}, startOrder(t, env, first, high, second, moved))

	// nothing of it is pending any more
	finished, err := env.Store.GetPipeline(moved.ID)
	require.NoError(t, err)
	assert.Error(t, pipeline.Prioritize(t.Context(), env.Store, finished))
}
