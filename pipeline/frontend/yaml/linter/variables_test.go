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

package linter_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/linter"
)

func lint(t *testing.T, config string, opts ...linter.Option) []*pipeline_errors.PipelineError {
	t.Helper()
	workflow, err := yaml.ParseString(config)
	require.NoError(t, err)
	err = linter.New(opts...).Lint([]*linter.WorkflowConfig{{File: ".woodpecker.yaml", RawConfig: config, Workflow: workflow}})
	return pipeline_errors.GetPipelineErrors(err)
}

func messages(errs []*pipeline_errors.PipelineError, field string) []string {
	var out []string
	for _, e := range errs {
		if data, ok := e.Data.(*pipeline_errors.LinterErrorData); ok && data.Field == field {
			out = append(out, e.Message)
		}
	}
	return out
}

// TestLintCommandVariables: a ${NAME} in a command the server does not know
// comes out empty, and the linter says so, once per name, as a warning.
func TestLintCommandVariables(t *testing.T) {
	config := `
when:
  event: push
params:
  TIER: light
matrix:
  GO: ['1.26']
steps:
  - name: build
    image: golang
    environment:
      GOOS: linux
    commands:
      - echo ${TIER} ${GO} ${CI_COMMIT_SHA} ${CI_COMMIT_TAG} $HOME $${HOME} $$$${X}
      - echo ${GOOS} ${HOME} ${HOME:-x} ${GIVEN}
  - name: plugin
    image: plugins/x
    settings:
      commands: ${NOPE}
`
	errs := lint(t, config, linter.WithVariables(map[string]string{"GIVEN": "1"}))
	warnings := messages(errs, "commands")
	require.Len(t, warnings, 2, "%v", warnings)
	assert.Contains(t, warnings[0], "${GOOS} in a command")
	assert.Contains(t, warnings[1], "${HOME} in a command")
	for _, e := range errs {
		if data, ok := e.Data.(*pipeline_errors.LinterErrorData); ok && data.Field == "commands" {
			assert.True(t, e.IsWarning)
		}
	}

	// without the run's variables, a name only the run would give warns too
	warnings = messages(lint(t, config), "commands")
	assert.Len(t, warnings, 3)
}

// TestLintCommandVariablesKnows: what the server knows does not warn, nor
// an expression with a default; what only the agent sets does, and so does
// a command shared through an anchor.
func TestLintCommandVariablesKnows(t *testing.T) {
	config := `
when:
  event: push
variables:
  - &check ['echo ${ANCHORED}']
  - &common
    TIER: light
params:
  <<: *common
matrix:
  include:
    - GO: '1.26'
steps:
  - name: build
    image: golang
    commands:
      - echo ${TIER} ${GO} ${CI_COMMIT_TAG} ${TASK:-all} ${X:=y} ${CI_WORKSPACE}
  - name: check
    image: golang
    commands: *check
`
	warnings := messages(lint(t, config), "commands")
	require.Len(t, warnings, 2, "%v", warnings)
	assert.Contains(t, warnings[0], "${CI_WORKSPACE} in a command")
	assert.Contains(t, warnings[1], "${ANCHORED} in a command")
}

func TestLintParams(t *testing.T) {
	errs := lint(t, `
when:
  event: push
params:
  B: ${A}
  A: x
steps:
  - name: a
    image: i
    commands: [echo]
`)
	got := messages(errs, "params")
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "parameter B: it uses A, which is declared after it")
}
