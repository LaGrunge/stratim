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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

// serverAway is how long the server stays away: STRATIM_E2E_SERVER_AWAY,
// minutes on CI's longer job, or 40 s.
func serverAway(t *testing.T) time.Duration {
	t.Helper()
	away := 40 * time.Second
	if v := os.Getenv("STRATIM_E2E_SERVER_AWAY"); v != "" {
		var err error
		away, err = time.ParseDuration(v)
		require.NoError(t, err, "STRATIM_E2E_SERVER_AWAY")
	}
	return away
}

const awayTickYAML = `
skip_clone: true

steps:
  - name: ticking
    image: dummy
    environment:
      SLEEP: '%s'
      TICK: '1s'
    commands:
      - echo ticking
  - name: after
    image: dummy
    commands:
      - echo after
`

// The server goes away for minutes while a step writes its log, the step
// ending meanwhile: the agent waits for it, and once it is back the
// pipeline succeeds, the step's log whole.
func TestServerAwayDuringStep(t *testing.T) {
	away := serverAway(t)
	// the step ends while the server is away: its end waits for its log
	sleep := away / 2
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: fmt.Appendf(nil, awayTickYAML, sleep)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err, "create pipeline")
	setup.WaitForStepRunning(t, env.Store, created.ID, "ticking")

	env.StopServer()
	time.Sleep(away)
	env.StartServerAgain(t)

	finished := setup.WaitForPipelineStatus(t, env.Store, created.ID, model.StatusSuccess, 2*time.Minute)
	ticking := setup.WaitForStep(t, env.Store, finished, "ticking")
	assert.Equal(t, model.StatusSuccess, ticking.State)
	assert.Equal(t, model.StatusSuccess, setup.WaitForStep(t, env.Store, finished, "after").State)

	entries, err := env.Store.LogFind(t.Context(), ticking)
	require.NoError(t, err)
	var ticks []string
	for i, e := range entries {
		assert.Equal(t, i, e.Line, "lines in order, none missing")
		assert.False(t, strings.HasPrefix(string(e.Data), "[stratim]"), "no line lost: %s", e.Data)
		if strings.HasPrefix(string(e.Data), "tick ") {
			ticks = append(ticks, string(e.Data))
		}
	}
	require.Len(t, ticks, int(sleep/time.Second), "every tick reached the server")
	for i, tick := range ticks {
		assert.Equal(t, fmt.Sprintf("tick %d", i+1), tick)
	}
}

const doneYAML = `
skip_clone: true

steps:
  - name: build
    image: dummy
    environment:
      SLEEP: '2s'
    commands:
      - echo building
`

// The database fails as the agent reports its workflow done: the agent
// tries again, and the workflow's and the pipeline's end reach the store,
// the workflow run once.
func TestDoneSurvivesStoreError(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(doneYAML)},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err, "create pipeline")
	setup.WaitForStepRunning(t, env.Store, created.ID, "build")
	env.FailWorkflowFinish(3, mysql.ErrInvalidConn)

	// well before the lease of a workflow not reported done expires, which
	// would run it again
	finished := setup.WaitForPipelineStatus(t, env.Store, created.ID, model.StatusSuccess, 30*time.Second)
	assert.Equal(t, 3, env.WorkflowFinishFailed(), "the agent tried until the store took it")
	workflows := setup.WaitForWorkflowsDone(t, env.Store, finished)
	require.Len(t, workflows, 1)
	assert.Equal(t, model.StatusSuccess, workflows[0].State)
	build := setup.WaitForStep(t, env.Store, finished, "build")
	entries, err := env.Store.LogFind(t.Context(), build)
	require.NoError(t, err)
	runs := 0
	for _, e := range entries {
		if strings.HasPrefix(string(e.Data), "StepName: build") {
			runs++
		}
	}
	assert.Equal(t, 1, runs, "the workflow ran once")
}
