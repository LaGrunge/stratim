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

package builder

import (
	"testing"

	"github.com/stretchr/testify/require"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/jsonnet"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
)

// bigJsonnetMatrix evaluates to a workflow of about 10 MB of YAML with a
// matrix of four axes: 1100 steps of 100 commands, ${...} in most of them.
const bigJsonnetMatrix = `
local s = import 'stratim.libsonnet';
local commands = [
  'echo ${CI_REPO_NAME} ' + j + ' ${OS} awk \'{ printf "%s\\n", $1 }\' some padding to make the line longer'
  for j in std.range(1, 100)
];
s.workflow(
  [
    s.step('step-' + i, 'golang:${GO}', commands, { environment: { N: '' + i, GO: '${GO}' } })
    for i in std.range(1, 1100)
  ],
  { matrix: { GO: ['1.26', '1.27'], OS: ['linux', 'darwin'] } },
)`

// BenchmarkJsonnetMatrix reads a ~10 MB jsonnet workflow for every axis of
// its matrix, the jsonnet evaluated once, before: listing the workflows
// (substitution and parsing) and building them (linting and compiling too).
func BenchmarkJsonnetMatrix(b *testing.B) {
	files, err := jsonnet.Expand([]*YamlFile{{Name: ".woodpecker/big.jsonnet", Data: []byte(bigJsonnetMatrix)}})
	require.NoError(b, err)
	require.Len(b, files, 1)
	require.Greater(b, len(files[0].Data), 10<<20, "the workflow as YAML")
	b.Logf("the workflow: %.1f MB of YAML", float64(len(files[0].Data))/(1<<20))

	for _, listOnly := range []bool{true, false} {
		name := "Build"
		if listOnly {
			name = "List"
		}
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				builder := &PipelineBuilder{
					Yamls:               files,
					RepoTrusted:         &metadata.TrustedConfiguration{},
					GetWorkflowMetadata: func(*Workflow) metadata.Metadata { return metadata.Metadata{} },
					ListOnly:            listOnly,
				}
				items, err := builder.Build()
				if pipeline_errors.HasBlockingErrors(err) {
					b.Fatal(err)
				}
				if len(items) != 4 {
					b.Fatalf("%d workflows, want 4", len(items))
				}
			}
		})
	}
}
