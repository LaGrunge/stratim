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
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/jsonnet"
)

// TestBuildJsonnetMatrixAxesApart: every axis of a jsonnet workflow's
// matrix is substituted from the workflow as the config wrote it, whatever
// the axes before it substituted, and a value holding ${...} is not
// substituted again by another axis.
func TestBuildJsonnetMatrixAxesApart(t *testing.T) {
	items, err := jsonnetBuilder([]*YamlFile{{Name: ".woodpecker/w.jsonnet", Data: []byte(`
local s = import 'stratim.libsonnet';
s.workflow(
  [s.step('a', 'golang:${GO}', ['echo ${GO} ${OS}', 'echo a\\nb $${HOME}'], { environment: { GO: '${GO}' } })],
  { matrix: { include: [
    { GO: '1.26', OS: '${GO}' },
    { GO: '1.27', OS: 'linux' },
    { GO: '1.26', OS: 'yes' },
  ] } },
)`)}}).Build()
	require.False(t, pipeline_errors.HasBlockingErrors(err), "build: %v", err)
	require.Len(t, items, 3)

	type got struct {
		image    string
		commands []string
		goEnv    string
	}
	var all []got
	for _, item := range items {
		step := item.Config.Stages[len(item.Config.Stages)-1].Steps[0]
		all = append(all, got{step.Image, step.Commands, step.Environment["GO"]})
	}
	assert.Equal(t, []got{
		{"golang:1.26", []string{"echo 1.26 ${GO}", `echo a\nb ${HOME}`}, "1.26"},
		{"golang:1.27", []string{"echo 1.27 linux", `echo a\nb ${HOME}`}, "1.27"},
		{"golang:1.26", []string{"echo 1.26 yes", `echo a\nb ${HOME}`}, "1.26"},
	}, all)
}

// TestJsonnetTreeAsText: a jsonnet workflow substituted on its tree, parsed
// once, reads as the text substituted string by string and parsed again,
// for every axis, the tree left as it was.
func TestJsonnetTreeAsText(t *testing.T) {
	files, err := jsonnet.Expand([]*YamlFile{{Name: ".woodpecker/w.jsonnet", Data: []byte(`
local s = import 'stratim.libsonnet';
s.workflow(
  [
    s.step('build', 'golang:${GO}', [
      'awk \'BEGIN { printf "%09d x\\n", 1 }\'',
      "sed 's/\\\\/\\//g'",
      'echo ${MSG} $${HOME} ${P}',
      'multi\nline ${GO}',
    ], { environment: { A: 'yes', B: '0777', C: '1:20', D: '', E: '${GO}', F: 'off' }, when: { event: ['push', 'manual'] } }),
    s.step('test', 'alpine', ['true'], { depends_on: ['build'], settings: { n: 1, b: true, s: '${OS}' } }),
  ],
  {
    params: { P: { default: 'p\\q ${GO}' } },
    matrix: { GO: ['1.26', '1.27'], OS: ['linux', 'yes'] },
    labels: { os: '${OS}' },
    depends_on: [],
  },
)`)}})
	require.NoError(t, err)
	require.Len(t, files, 1)
	file := files[0]
	require.True(t, file.FromJsonnet())

	state := newBuildState()
	src := state.source(file)
	require.True(t, src.parsed, "a jsonnet workflow is parsed once")
	assert.Same(t, src, state.source(file), "for all its axes")
	asText := &workflowSource{file: file}

	axes, err := src.matrix()
	require.NoError(t, err)
	wantAxes, err := asText.matrix()
	require.NoError(t, err)
	require.Equal(t, wantAxes, axes)
	require.Len(t, axes, 4)
	wantParams, err := asText.params()
	require.NoError(t, err)
	params, err := src.params()
	require.NoError(t, err)
	require.Equal(t, wantParams, params)

	for _, msg := range []string{`say "hi" to C:\temp\new`, "fix: a #1\n- b", "it's"} {
		for _, axis := range axes {
			environ := map[string]string{"MSG": msg, "P": "p"}
			maps.Copy(environ, axis)
			want, err := asText.workflow(environ)
			require.NoError(t, err)
			got, err := src.workflow(environ)
			require.NoError(t, err)
			assert.Equal(t, want, got, "%v, MSG=%q", axis, msg)
		}
	}

	b := jsonnetBuilder(files)
	config, err := b.parseWorkflow(src, axes[0], b.GetWorkflowMetadata(&Workflow{Name: "w"}))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"P": `p\q ` + axes[0]["GO"]}, config.params, "a parameter's default keeps its backslash")

	state.done(file, "w")
	assert.NotSame(t, src, state.source(file), "a file done is not kept")
}
