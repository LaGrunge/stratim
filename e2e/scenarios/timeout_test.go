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
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

// TestStepTimeout: a step that runs past its own timeout fails with exit
// code 124 and its error, well before the workflow's timeout; the steps
// after it go on as after any failure.
func TestStepTimeout(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
steps:
  - name: too-long
    image: dummy
    commands: [echo sleeping]
    timeout: 1s
    failure: ignore
    environment:
      SLEEP: 30s
  - name: after
    image: dummy
    commands: [echo after]
`)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	start := time.Now()
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)
	assert.Less(t, time.Since(start), 20*time.Second, "the step's timeout should have stopped the 30s step")

	assert.Equal(t, model.StatusSuccess, finished.Status, "the timed-out step's failure is ignored")
	step := setup.WaitForStep(t, env.Store, finished, "too-long")
	assert.Equal(t, model.StatusFailure, step.State)
	assert.Equal(t, 124, step.ExitCode)
	assert.Contains(t, step.Error, "step timed out after 1s")
	assert.Equal(t, model.StatusSuccess, setup.WaitForStep(t, env.Store, finished, "after").State)
}

// TestWorkflowTimeoutFromConfig: a workflow's own timeout takes the place of
// the repository's. The agent counts in seconds here, so 2 is two seconds.
func TestWorkflowTimeoutFromConfig(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(`
skip_clone: true
timeout: 2
steps:
  - name: too-long
    image: dummy
    commands: [echo sleeping]
    environment:
      SLEEP: 30s
`)},
	})
	repo := env.Fixtures.Repo
	repo.Timeout = 60
	require.NoError(t, env.Store.UpdateRepo(repo), "set repository timeout")

	agent := setup.StartAgent(t, env.GRPCAddr, setup.WithTimeoutUnit(time.Second))
	setup.WaitForAgentRegistered(t, env.Store, agent)

	start := time.Now()
	created, err := pipeline.Create(t.Context(), env.Store, repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)

	elapsed := time.Since(start)
	assert.Equal(t, model.StatusKilled, finished.Status)
	assert.GreaterOrEqual(t, elapsed, 2*time.Second, "the workflow ended before its timeout")
	assert.Less(t, elapsed, 20*time.Second, "the workflow's own timeout, not the repository's 60, should have stopped it")
}
