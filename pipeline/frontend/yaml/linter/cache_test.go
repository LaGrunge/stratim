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

package linter_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/linter"
)

// cachedConfig has findings of every kind: of the parsed workflow (a
// privileged step), of the schema, a deprecation, a bad habit and ${NAME}
// in commands, one of which the variables of a run may know.
const cachedConfig = `
matrix:
  GO: ['1.26', '1.27']
unknown_key: x
steps:
  - name: build
    image: golang:${GO}
    privileged: true
    commands:
      - echo ${CI_COMMIT_PRERELEASE} ${NOWHERE} ${GLOBAL} ${GO} ${DEF:-d}
`

// TestCacheAsWithout: a linter with a cache finds what one without does,
// in the same order, every time, and the variables it is given still count.
func TestCacheAsWithout(t *testing.T) {
	workflow, err := yaml.ParseString(cachedConfig)
	require.NoError(t, err)
	config := func() []*linter.WorkflowConfig {
		return []*linter.WorkflowConfig{{File: "w", RawConfig: cachedConfig, Workflow: workflow}}
	}
	cache := linter.NewCache()
	for _, variables := range []map[string]string{nil, {"GLOBAL": "g"}, nil} {
		want := pipeline_errors.GetPipelineErrors(linter.New(linter.WithVariables(variables)).Lint(config()))
		got := pipeline_errors.GetPipelineErrors(linter.New(linter.WithVariables(variables), linter.WithCache(cache)).Lint(config()))
		assert.Equal(t, want, got, "variables %v", variables)

		var types []pipeline_errors.PipelineErrorType
		for _, e := range got {
			types = append(types, e.Type)
		}
		assert.Contains(t, types, pipeline_errors.PipelineErrorTypeDeprecation)
		assert.Contains(t, types, pipeline_errors.PipelineErrorTypeBadHabit)
		unknown := messages(got, "commands")
		if variables == nil {
			assert.Len(t, unknown, 2, "NOWHERE and GLOBAL")
		} else {
			assert.Len(t, unknown, 1, "NOWHERE: the run has GLOBAL")
		}
	}
}

// TestCacheByFile: two files of one text, linted with one cache, each get
// findings that name them, the raw ones too; a file forgotten is read anew.
func TestCacheByFile(t *testing.T) {
	workflow, err := yaml.ParseString(cachedConfig)
	require.NoError(t, err)
	named := func(errs []*pipeline_errors.PipelineError) map[string]bool {
		out := make(map[string]bool)
		for _, e := range errs {
			switch data := e.Data.(type) {
			case *pipeline_errors.LinterErrorData:
				out[data.File] = true
			case pipeline_errors.DeprecationErrorData:
				out[data.File] = true
			case pipeline_errors.BadHabitErrorData:
				out[data.File] = true
			}
		}
		return out
	}
	cache := linter.NewCache()
	for i, file := range []string{"a", "b", "a"} {
		config := []*linter.WorkflowConfig{{File: file, RawConfig: cachedConfig, Workflow: workflow}}
		got := pipeline_errors.GetPipelineErrors(linter.New(linter.WithCache(cache)).Lint(config))
		var types []pipeline_errors.PipelineErrorType
		for _, e := range got {
			types = append(types, e.Type)
		}
		assert.Contains(t, types, pipeline_errors.PipelineErrorTypeDeprecation, "a raw finding")
		assert.Equal(t, map[string]bool{file: true}, named(got), "findings of %s", file)
		if i == 1 {
			assert.Equal(t, 2, cache.Entries(), "a and b")
			cache.Forget("a")
			assert.Equal(t, 1, cache.Entries(), "b")
		}
	}
	assert.Equal(t, 3, cache.Reads(), "a, b, and a forgotten")
	cache.Forget("a")
	cache.Forget("b")
	assert.Zero(t, cache.Entries())
}
