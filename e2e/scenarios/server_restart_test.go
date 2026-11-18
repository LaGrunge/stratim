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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/dummy"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

var restartBuildYAML = []byte(`
skip_clone: true

steps:
  - name: build
    image: dummy
    environment:
      SLEEP: '4s'
    commands:
      - echo building
`)

var restartTestYAML = []byte(`
skip_clone: true

depends_on:
  - build

steps:
  - name: test
    image: dummy
    commands:
      - echo testing the build
`)

// The server restarts, as on a deploy, while a workflow runs that another
// depends on: the dependent starts only once the dependency finished, its
// agent reporting it done to the restarted server.
func TestServerRestartKeepsWorkflowOrder(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker/build.yaml", Data: restartBuildYAML},
		{Name: ".woodpecker/test.yaml", Data: restartTestYAML},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err, "create pipeline")
	setup.WaitForStepRunning(t, env.Store, created.ID, "build")

	env.RestartServer(t)

	finished := setup.WaitForPipeline(t, env.Store, created.ID)
	require.Equal(t, model.StatusSuccess, finished.Status)
	build := setup.WaitForStep(t, env.Store, finished, "build")
	test := setup.WaitForStep(t, env.Store, finished, "test")
	require.NotZero(t, build.Finished)
	require.NotZero(t, test.Started)
	assert.GreaterOrEqualf(t, test.Started, build.Finished,
		"test started at %d, build finished at %d: the dependent ran before its dependency after the restart",
		test.Started, build.Finished)
	assert.Equal(t, model.StatusSuccess, build.State)
	assert.Equal(t, model.StatusSuccess, test.State)
}

var restartLongYAML = []byte(`
skip_clone: true

steps:
  - name: long-running
    image: dummy
    environment:
      SLEEP: '60s'
    commands:
      - echo a long job
`)

// After a restart, a cancel reaches the agent of a workflow running: it
// stops it and reports it done, long before the workflow would end.
func TestServerRestartThenCancel(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: restartLongYAML},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err, "create pipeline")
	setup.WaitForStepRunning(t, env.Store, created.ID, "long-running")

	env.RestartServer(t)
	setup.WaitForWorkersReady(t, env.Queue, 1) // the agent is back

	forge, err := env.Manager.ForgeByID(env.Fixtures.Forge.ID)
	require.NoError(t, err, "resolve forge")
	running, err := env.Store.GetPipeline(created.ID)
	require.NoError(t, err)
	canceledAt := time.Now()
	require.NoError(t, pipeline.Cancel(t.Context(), forge, env.Store, env.WorkflowEnd, env.Fixtures.Repo, env.Fixtures.Owner, running, nil))

	finished := setup.WaitForPipeline(t, env.Store, created.ID)
	assert.Equal(t, model.StatusKilled, finished.Status)
	workflows := setup.WaitForWorkflowsDone(t, env.Store, finished)
	require.Len(t, workflows, 1)
	// the server alone ends the workflow killed too; only its agent, told,
	// stops the step, which exits as canceled
	require.Eventually(t, func() bool {
		step := setup.WaitForStep(t, env.Store, finished, "long-running")
		return step.ExitCode == dummy.ExitCodeCanceled
	}, 20*time.Second, 100*time.Millisecond, "its agent stopped the step")
	step := setup.WaitForStep(t, env.Store, finished, "long-running")
	assert.Equal(t, model.StatusKilled, step.State)
	assert.Less(t, time.Since(canceledAt), 30*time.Second, "stopped long before the step ran out")
}
