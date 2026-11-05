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

package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
)

var jsonnetConfig = []*YamlFile{
	{Name: ".woodpecker/lint.yaml", Data: []byte("steps:\n  - name: lint\n    image: alpine\n    commands: [echo lint]\n")},
	{Name: ".woodpecker/lib.libsonnet", Data: []byte(`
local s = import 'stratim.libsonnet';
{ test(v):: s.workflow([s.step('test', 'golang:' + v, ['go test ./...'])], { depends_on: ['lint'] }) }`)},
	{Name: ".woodpecker/test.jsonnet", Data: []byte(`
local lib = import 'lib.libsonnet';
{ ['test-' + v]: lib.test(v) for v in ['1.26', '1.27'] }`)},
}

func jsonnetBuilder(files []*YamlFile) *PipelineBuilder {
	return &PipelineBuilder{
		Yamls:               files,
		RepoTrusted:         &metadata.TrustedConfiguration{},
		GetWorkflowMetadata: func(*Workflow) metadata.Metadata { return metadata.Metadata{} },
	}
}

func TestBuildJsonnet(t *testing.T) {
	items, err := jsonnetBuilder(jsonnetConfig).Build()
	require.False(t, pipeline_errors.HasBlockingErrors(err), "build: %v", err)

	names := make(map[string][]string)
	for _, item := range items {
		var deps []string
		for _, d := range item.DependsOn {
			deps = append(deps, d.Name)
		}
		names[item.Workflow.Name] = deps
	}
	assert.Equal(t, map[string][]string{"lint": nil, "test-1.26": {"lint"}, "test-1.27": {"lint"}}, names)
}

func TestBuildJsonnetSelectsAWorkflow(t *testing.T) {
	b := jsonnetBuilder(jsonnetConfig)
	b.SelectedWorkflow = "test-1.27"
	items, err := b.Build()
	require.False(t, pipeline_errors.HasBlockingErrors(err), "build: %v", err)

	var names []string
	for _, item := range items {
		names = append(names, item.Workflow.Name)
	}
	assert.ElementsMatch(t, []string{"test-1.27", "lint"}, names, "the selected workflow and what it depends on")
}

func TestBuildJsonnetError(t *testing.T) {
	_, err := jsonnetBuilder([]*YamlFile{{Name: ".woodpecker/broken.jsonnet", Data: []byte(`error 'no platforms given'`)}}).Build()
	require.True(t, pipeline_errors.HasBlockingErrors(err))
	assert.Contains(t, err.Error(), "broken.jsonnet")
	assert.Contains(t, err.Error(), "no platforms given")
}

func TestBuildJsonnetWithAMatrix(t *testing.T) {
	items, err := jsonnetBuilder([]*YamlFile{{Name: ".woodpecker/test.jsonnet", Data: []byte(`
local s = import 'stratim.libsonnet';
s.workflow([s.step('test', 'golang:${GO}', ['go test ./...'])], { matrix: { GO: ['1.26', '1.27'] } })`)}}).Build()
	require.False(t, pipeline_errors.HasBlockingErrors(err), "build: %v", err)
	require.Len(t, items, 2, "the server expands the matrix of a jsonnet workflow as of a YAML one")
	var images []string
	for _, item := range items {
		images = append(images, item.Config.Stages[len(item.Config.Stages)-1].Steps[0].Image)
	}
	assert.ElementsMatch(t, []string{"golang:1.26", "golang:1.27"}, images)
}
