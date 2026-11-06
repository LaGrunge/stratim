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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
)

// matrixWithFindings is a workflow of three axes with a deprecation, a bad
// habit (no event filter) in two steps, a ${NAME} the server does not
// know, and a step with both settings and environment, which the schema
// reports too.
const matrixWithFindings = `
matrix:
  GO: ['1.25', '1.26', '1.27']
steps:
  - name: build
    image: golang:${GO}
    commands:
      - echo ${CI_COMMIT_PRERELEASE} ${NOWHERE} ${GO}
  - name: publish
    image: plugins/s3
    settings: {bucket: b}
    environment: {X: '1'}
`

// TestBuildMatrixReportsFindingsOnce: the lint findings of a matrix
// workflow, YAML or jsonnet, are reported once, not once per axis, in the
// order the first axis found them, and each axis is built.
func TestBuildMatrixReportsFindingsOnce(t *testing.T) {
	for _, file := range []*YamlFile{
		{Name: ".woodpecker/m.yaml", Data: []byte(matrixWithFindings)},
		{Name: ".woodpecker/m.jsonnet", Data: []byte(`{
  matrix: { GO: ['1.25', '1.26', '1.27'] },
  steps: [
    { name: 'build', image: 'golang:${GO}', commands: ['echo ${CI_COMMIT_PRERELEASE} ${NOWHERE} ${GO}'] },
    { name: 'publish', image: 'plugins/s3', settings: { bucket: 'b' }, environment: { X: '1' } },
  ],
}`)},
	} {
		items, err := jsonnetBuilder([]*YamlFile{file}).Build()
		require.False(t, pipeline_errors.HasBlockingErrors(err), "%s: %v", file.Name, err)
		require.Len(t, items, 3, file.Name)

		var kinds []pipeline_errors.PipelineErrorType
		seen := make(map[string]int)
		for _, e := range pipeline_errors.GetPipelineErrors(err) {
			seen[findingKey(e)]++
			kinds = append(kinds, e.Type)
		}
		for finding, n := range seen {
			assert.Equal(t, 1, n, "%s: %s reported %d times", file.Name, finding, n)
		}
		assert.Equal(t, []pipeline_errors.PipelineErrorType{
			pipeline_errors.PipelineErrorTypeLinter,      // settings and environment
			pipeline_errors.PipelineErrorTypeLinter,      // the same, as the schema reads it
			pipeline_errors.PipelineErrorTypeDeprecation, // CI_COMMIT_PRERELEASE
			pipeline_errors.PipelineErrorTypeBadHabit,    // build has no event filter
			pipeline_errors.PipelineErrorTypeBadHabit,    // publish neither
			pipeline_errors.PipelineErrorTypeLinter,      // ${NOWHERE}
		}, kinds, "%s: %v", file.Name, err)
	}
}

// TestBuildMatrixFilteredAxisKeepsFindings: an axis left out, by the when
// of the workflow or with all its steps filtered, reports nothing, as
// before, and takes nothing from the axis that is built: that one reports
// the findings they share, once.
func TestBuildMatrixFilteredAxisKeepsFindings(t *testing.T) {
	for _, filtered := range []string{"1", "2"} {
		kept := map[string]string{"1": "2", "2": "1"}[filtered]
		for how, config := range map[string]string{
			"workflow when":  "when:\n  evaluate: 'GO == \"" + kept + "\"'\nsteps:\n  - name: build\n    image: golang:${GO}\n    commands: [go test]\n",
			"steps filtered": "skip_clone: true\nsteps:\n  - name: build\n    image: golang:${GO}\n    commands: [go test]\n    when:\n      matrix: {GO: '" + kept + "'}\n",
		} {
			b := jsonnetBuilder([]*YamlFile{{Name: ".woodpecker/m.yaml", Data: []byte("matrix:\n  GO: ['1', '2']\n" + config)}})
			b.GetWorkflowMetadata = func(w *Workflow) metadata.Metadata {
				var m metadata.Metadata
				m.Workflow.Matrix = w.Environ
				return m
			}
			items, err := b.Build()
			require.False(t, pipeline_errors.HasBlockingErrors(err), "%s: %v", how, err)
			require.Len(t, items, 1, "%s: GO=%s is left out", how, filtered)
			assert.Equal(t, kept, items[0].Workflow.Environ["GO"], how)

			var habits int
			for _, e := range pipeline_errors.GetPipelineErrors(err) {
				if e.Type == pipeline_errors.PipelineErrorTypeBadHabit {
					habits++
				}
			}
			assert.Equal(t, 1, habits, "%s, GO=%s left out: the bad habit is reported once: %v", how, filtered, err)
		}
	}
}

// TestReportOnce: a finding is left out when the same file reported it
// before in the Build; another finding, or the same of another file, is
// not.
func TestReportOnce(t *testing.T) {
	b := newBuildState()
	finding := func(field string) error {
		return &pipeline_errors.PipelineError{
			Type: pipeline_errors.PipelineErrorTypeLinter, Message: "m", IsWarning: true,
			Data: &pipeline_errors.LinterErrorData{File: "w", Field: field},
		}
	}
	plain := errors.New("plain")

	got := b.reportOnce("a.yaml", multierr.Combine(finding("x"), plain))
	assert.Len(t, multierr.Errors(got), 2)
	got = b.reportOnce("a.yaml", multierr.Combine(finding("x"), finding("y"), plain))
	assert.Equal(t, []error{finding("y")}, multierr.Errors(got), "only what is new")
	got = b.reportOnce("b.yaml", finding("x"))
	assert.Equal(t, []error{finding("x")}, multierr.Errors(got), "another file")
	assert.NoError(t, b.reportOnce("a.yaml", nil))
}

// TestBuildReadsEachFileOnce: a Build lints the raw text of a matrix
// workflow once for all its axes, and keeps nothing of a file once its
// axes are built, selected workflows and their dependencies included.
func TestBuildReadsEachFileOnce(t *testing.T) {
	const matrixHabit = "matrix:\n  GO: ['1', '2']\nsteps:\n  - name: build\n    image: golang:${GO}\n    commands: [go test]\n"
	for name, files := range map[string][]*YamlFile{
		"yaml":    {{Name: ".woodpecker/a.yaml", Data: []byte(matrixHabit)}},
		"jsonnet": {{Name: ".woodpecker/a.jsonnet", Data: []byte(`{ matrix: { GO: ['1', '2'] }, steps: [{ name: 'build', image: 'golang:${GO}', commands: ['go test'] }] }`)}},
		"selected": {
			{Name: ".woodpecker/a.yaml", Data: []byte(matrixHabit + "depends_on: [b]\n")},
			{Name: ".woodpecker/b.yaml", Data: []byte("steps:\n  - name: b\n    image: alpine\n    commands: [echo]\n")},
		},
	} {
		b := jsonnetBuilder(files)
		if name == "selected" {
			b.SelectedWorkflow = "a"
		}
		state := newBuildState()
		items, err := b.build(state)
		require.False(t, pipeline_errors.HasBlockingErrors(err), "%s: %v", name, err)
		require.Len(t, items, len(files)+1, name)
		assert.Equal(t, len(files), state.lint.Reads(), "%s: the raw text of each file is linted once", name)
		assert.Zero(t, state.lint.Entries(), "%s: nothing is kept of a file built", name)
		assert.Empty(t, state.sources, "%s: nor its source", name)
	}
}
