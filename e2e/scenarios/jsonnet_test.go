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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

// TestJsonnetErrorFailsThePipeline checks that a config the server cannot
// evaluate ends the pipeline in error, saying which file and why, and that
// the source is kept for a restart.
func TestJsonnetErrorFailsThePipeline(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker/build.jsonnet", Data: []byte("local s = import 'stratim.libsonnet';\nerror 'no platforms given'\n")},
	})

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err, "the pipeline is stored, in error")
	finished := setup.WaitForPipeline(t, env.Store, created.ID)
	assert.Equal(t, model.StatusError, finished.Status)

	require.NotEmpty(t, finished.Errors)
	assert.Contains(t, finished.Errors[0].Message, "build.jsonnet")
	assert.Contains(t, finished.Errors[0].Message, "no platforms given")

	configs, err := env.Store.ConfigsForPipeline(finished.ID)
	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.Contains(t, configs[0].Name, "build.jsonnet", "the jsonnet source is what is kept")
	assert.Contains(t, string(configs[0].Data), "no platforms given")
}

// TestJsonnetPipelineRestarts checks that a jsonnet pipeline restarts from
// its stored configs, its library among them, into the same workflows.
func TestJsonnetPipelineRestarts(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker/steps.libsonnet", Data: []byte("local s = import 'stratim.libsonnet';\n{ step(n):: s.step(n, 'dummy', ['echo ' + n]) }\n")},
		{Name: ".woodpecker/builds.jsonnet", Data: []byte("local lib = import 'steps.libsonnet';\n{ ['build-' + p]: { skip_clone: true, steps: [lib.step('build-' + p)] } for p in ['a', 'b'] }\n")},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	first := setup.WaitForPipeline(t, env.Store, created.ID)
	require.Equal(t, model.StatusSuccess, first.Status)

	configs, err := env.Store.ConfigsForPipeline(first.ID)
	require.NoError(t, err)
	assert.Len(t, configs, 2, "the config and its library are both stored")

	restarted, err := pipeline.Restart(t.Context(), env.Store, first, env.Fixtures.Owner, env.Fixtures.Repo, nil)
	require.NoError(t, err)
	second := setup.WaitForPipeline(t, env.Store, restarted.ID)
	assert.Equal(t, model.StatusSuccess, second.Status)

	workflows := func(p *model.Pipeline) []string {
		ws, err := env.Store.WorkflowGetTree(p)
		require.NoError(t, err)
		var names []string
		for _, w := range ws {
			names = append(names, w.Name)
		}
		return names
	}
	assert.ElementsMatch(t, []string{"build-a", "build-b"}, workflows(first))
	assert.ElementsMatch(t, workflows(first), workflows(second))
}
