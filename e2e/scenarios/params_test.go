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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

// paramsConfig declares a parameter with a default and the values allowed,
// runs a step only for one of them, and has a command use the parameter and
// an escaped shell variable.
const paramsConfig = `skip_clone: true
params:
  TIER:
    default: light
    options: [light, full]
steps:
  - name: always
    image: dummy
    commands:
      - echo tier ${TIER} home $HOME in $${PWD##*/}
  - name: full-only
    image: dummy
    commands: [echo full]
    when:
      evaluate: 'TIER == "full"'
`

// TestParamDefault: a run without the parameter gets its default.
func TestParamDefault(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{{Name: ".woodpecker.yaml", Data: []byte(paramsConfig)}})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipelineStatus(t, env.Store, created.ID, model.StatusSuccess, 30*time.Second)

	assert.Equal(t, []string{"always"}, stepNames(t, env, finished), "TIER is light: full-only is left out")
}

// TestParamOfManualRun: the value of a manual run reaches `when` and the
// commands.
func TestParamOfManualRun(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{{Name: ".woodpecker.yaml", Data: []byte(paramsConfig)}})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	manual := env.DummyPipeline(model.EventManual)
	manual.AdditionalVariables = map[string]string{"TIER": "full"}
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, manual)
	require.NoError(t, err)
	finished := setup.WaitForPipelineStatus(t, env.Store, created.ID, model.StatusSuccess, 30*time.Second)

	assert.Equal(t, []string{"always", "full-only"}, stepNames(t, env, finished), "TIER is full")

	always := setup.WaitForStep(t, env.Store, finished, "always")
	var log string
	require.Eventually(t, func() bool {
		entries, err := env.Store.LogFind(t.Context(), always)
		require.NoError(t, err)
		var b strings.Builder
		for _, e := range entries {
			b.Write(e.Data)
			b.WriteByte('\n')
		}
		log = b.String()
		return strings.Contains(log, "StepCommands")
	}, 10*time.Second, 50*time.Millisecond, "the step's log")
	assert.Contains(t, log, "echo tier full home $HOME in ${PWD##*/}", "the command the shell gets")
}

// TestParamOutsideOptions: a value the parameter does not allow fails the
// pipeline, naming the parameter.
func TestParamOutsideOptions(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{{Name: ".woodpecker.yaml", Data: []byte(paramsConfig)}})

	manual := env.DummyPipeline(model.EventManual)
	manual.AdditionalVariables = map[string]string{"TIER": "huge"}
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, manual)
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)
	assert.Equal(t, model.StatusError, finished.Status)
	var messages []string
	for _, e := range finished.Errors {
		messages = append(messages, e.Message)
	}
	assert.Contains(t, messages, `parameter TIER: "huge" is not one of light, full`)
}

// TestParamInJsonnet: a jsonnet config reads the run's variables.
func TestParamInJsonnet(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{{Name: ".woodpecker/build.jsonnet", Data: []byte(`
local s = import 'stratim.libsonnet';
{
  ['build-' + p]: { skip_clone: true, steps: [s.step('build', 'dummy', ['echo ' + p])] }
  for p in std.split(s.var('PLATFORMS', 'amd64'), ',')
}
`)}})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	workflows := func(vars map[string]string) []string {
		p := env.DummyPipeline(model.EventManual)
		p.AdditionalVariables = vars
		created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, p)
		require.NoError(t, err)
		finished := setup.WaitForPipelineStatus(t, env.Store, created.ID, model.StatusSuccess, 30*time.Second)
		ws, err := env.Store.WorkflowGetTree(finished)
		require.NoError(t, err)
		var names []string
		for _, w := range ws {
			names = append(names, w.Name)
		}
		return names
	}
	assert.ElementsMatch(t, []string{"build-amd64"}, workflows(nil))
	assert.ElementsMatch(t, []string{"build-amd64", "build-arm64"}, workflows(map[string]string{"PLATFORMS": "amd64,arm64"}))
}

func stepNames(t *testing.T, env *setup.ServerEnv, p *model.Pipeline) []string {
	t.Helper()
	workflows, err := env.Store.WorkflowGetTree(p)
	require.NoError(t, err)
	var names []string
	for _, w := range workflows {
		for _, s := range w.Children {
			names = append(names, s.Name)
		}
	}
	return names
}
