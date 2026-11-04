// Copyright 2026 Woodpecker Authors
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
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

// TestRecordEvents checks the harness's event subscription: the events the
// server publishes for a pipeline reach the recorder, including its start and
// its end.
func TestRecordEvents(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte("skip_clone: true\nsteps:\n  - name: build\n    image: dummy\n    commands: [echo building]\n")},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)
	events := env.RecordEvents(t)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err, "create pipeline")

	finished := events.WaitFor(t, 30*time.Second, setup.PipelineStatus(created.ID, model.StatusSuccess))
	assert.Equal(t, env.Fixtures.Repo.ID, finished.Repo.ID, "event repo")
	events.WaitFor(t, time.Second, setup.PipelineStatus(created.ID, model.StatusRunning))
}

// TestWorkflowTimeoutInSeconds checks the harness's timeout unit: with the
// agent counting in seconds, a repository timeout of 2 stops a step that
// would sleep for 30 seconds.
//
// A timeout currently ends a workflow exactly like a cancel (killed, exit
// code 130, "Canceled"); S0-8 gives it a status of its own and updates the
// expectations here.
func TestWorkflowTimeoutInSeconds(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: too-long
    image: dummy
    commands: [echo sleeping]
    environment:
      SLEEP: 30s
`)},
	})
	repo := env.Fixtures.Repo
	repo.Timeout = 2
	require.NoError(t, env.Store.UpdateRepo(repo), "set repository timeout")

	agent := setup.StartAgent(t, env.GRPCAddr, setup.WithTimeoutUnit(time.Second))
	setup.WaitForAgentRegistered(t, env.Store, agent)

	start := time.Now()
	created, err := pipeline.Create(t.Context(), env.Store, repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err, "create pipeline")
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	elapsed := time.Since(start)
	assert.Equal(t, model.StatusKilled, finished.Status, "pipeline status after the timeout")
	assert.GreaterOrEqual(t, elapsed, 2*time.Second, "the workflow ended before its timeout")
	assert.Less(t, elapsed, 20*time.Second, "the timeout should have stopped the 30s step")

	step := setup.WaitForStep(t, env.Store, finished, "too-long")
	assert.Equal(t, model.StatusKilled, step.State, "step state after the timeout")
	assert.Equal(t, 130, step.ExitCode, "step exit code after the timeout")
}
