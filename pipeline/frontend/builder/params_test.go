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

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
)

const paramsConfig = `
when:
  event: manual
skip_clone: true
params:
  TIER:
    default: light
    description: how much to test
    options: [light, full]
  RETRIES: 3
  TAG: v-${CI_COMMIT_BRANCH}
steps:
  - name: test
    image: golang:${TIER}
    commands: [echo $TIER]
  - name: full-only
    image: golang
    commands: [go test ./...]
    when:
      evaluate: 'TIER == "full"'
  - name: publish
    image: woodpeckerci/plugin-kaniko
    settings:
      tags: ${TAG}
`

func buildParams(t *testing.T, vars map[string]string) ([]*Item, error) {
	t.Helper()
	m := &testMetadata{pipelineEvent: "manual", branch: "main"}
	b := PipelineBuilder{
		GetWorkflowMetadata: m.GetWorkflowMetadata,
		AdditionalEnvs:      vars,
		RepoTrusted:         &metadata.TrustedConfiguration{},
		Yamls:               []*YamlFile{{Name: ".woodpecker.yaml", Data: []byte(paramsConfig)}},
	}
	return b.Build()
}

func stepsByName(items []*Item) map[string]*backend_types.Step {
	steps := make(map[string]*backend_types.Step)
	for _, item := range items {
		for _, stage := range item.Config.Stages {
			for _, s := range stage.Steps {
				steps[s.Name] = s
			}
		}
	}
	return steps
}

func TestParamDefaults(t *testing.T) {
	t.Parallel()
	items, err := buildParams(t, nil)
	require.False(t, pipeline_errors.HasBlockingErrors(err), "%v", err)
	steps := stepsByName(items)

	require.Contains(t, steps, "test")
	assert.NotContains(t, steps, "full-only", "when sees the default")
	test := steps["test"]
	assert.Equal(t, "golang:light", test.Image, "substituted")
	assert.Equal(t, "light", test.Environment["TIER"])
	assert.Equal(t, "3", test.Environment["RETRIES"])
	assert.Equal(t, "v-main", test.Environment["TAG"], "a default can use the run's variables")
	assert.Equal(t, "v-main", steps["publish"].Environment["PLUGIN_TAGS"])
}

func TestParamGiven(t *testing.T) {
	t.Parallel()
	items, err := buildParams(t, map[string]string{"TIER": "full", "TAG": "release"})
	require.False(t, pipeline_errors.HasBlockingErrors(err), "%v", err)
	steps := stepsByName(items)

	assert.Contains(t, steps, "full-only", "when sees the value given")
	assert.Equal(t, "golang:full", steps["test"].Image)
	assert.Equal(t, "full", steps["test"].Environment["TIER"])
	assert.Equal(t, "release", steps["publish"].Environment["PLUGIN_TAGS"])
	// plugins get no parameters, as no manual variables: they could set
	// the plugin's settings
	assert.NotContains(t, steps["publish"].Environment, "TIER")
	assert.NotContains(t, steps["publish"].Environment, "TAG")
}

func TestParamNotAnOption(t *testing.T) {
	t.Parallel()
	_, err := buildParams(t, map[string]string{"TIER": "huge"})
	require.Error(t, err)
	assert.True(t, pipeline_errors.HasBlockingErrors(err))
	assert.ErrorContains(t, err, `parameter TIER: "huge" is not one of light, full`)
}

func TestParamNames(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]string{
		"1ST":           "a name is letters, digits and _",
		"A-B":           "a name is letters, digits and _",
		"CI_COMMIT_SHA": "the name is reserved",
		"CI":            "the name is reserved",
		"PATH":          "the name is reserved",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := buildConfig(t, nil, nil, `
params:
  "`+name+`": x
steps:
  - name: a
    image: scratch
    commands: [echo]
`)
			assert.ErrorContains(t, err, want)
		})
	}
}

func buildConfig(t *testing.T, vars, global map[string]string, config string) ([]*Item, error) {
	t.Helper()
	m := &testMetadata{pipelineEvent: "manual", branch: "main"}
	b := PipelineBuilder{
		GetWorkflowMetadata: m.GetWorkflowMetadata,
		AdditionalEnvs:      vars,
		Envs:                global,
		RepoTrusted:         &metadata.TrustedConfiguration{},
		Yamls:               []*YamlFile{{Name: ".woodpecker.yaml", Data: []byte("when:\n  event: manual\nskip_clone: true\n" + config)}},
	}
	items, err := b.Build()
	if pipeline_errors.HasBlockingErrors(err) {
		return nil, err
	}
	return items, nil
}

// TestParamDefaultUsesParam: a default sees the parameters declared before
// it.
func TestParamDefaultUsesParam(t *testing.T) {
	t.Parallel()
	items, err := buildConfig(t, map[string]string{"A": "given"}, nil, `
params:
  A: x
  B: ${A}-y
steps:
  - name: a
    image: i:${B}
    commands: ['echo ${B}']
`)
	require.NoError(t, err)
	step := stepsByName(items)["a"]
	assert.Equal(t, "given-y", step.Environment["B"])
	assert.Equal(t, "i:given-y", step.Image)
	assert.Equal(t, []string{"echo given-y"}, step.Commands, "the server knows B: it substitutes it in commands too")
}

// TestParamOverGlobalEnv: a parameter and a global variable of the same
// name: the parameter wins everywhere alike.
func TestParamOverGlobalEnv(t *testing.T) {
	t.Parallel()
	items, err := buildConfig(t, nil, map[string]string{"TIER": "global"}, `
params:
  TIER: light
steps:
  - name: a
    image: i:${TIER}
    commands: [echo]
  - name: b
    image: i
    commands: [echo]
    when:
      evaluate: 'TIER == "light"'
`)
	require.NoError(t, err)
	steps := stepsByName(items)
	assert.Equal(t, "i:light", steps["a"].Image)
	assert.Equal(t, "light", steps["a"].Environment["TIER"])
	assert.Contains(t, steps, "b")
}

func TestParamMatrixAxis(t *testing.T) {
	t.Parallel()
	_, err := buildConfig(t, nil, nil, `
matrix:
  TIER: [a, b]
params:
  TIER: light
steps:
  - name: a
    image: i
    commands: [echo]
`)
	assert.ErrorContains(t, err, "parameter TIER: the name is a matrix axis too")
}

func TestParamOptionsEdges(t *testing.T) {
	t.Parallel()
	_, err := buildConfig(t, map[string]string{"A": "x"}, nil, `
params:
  A: {options: []}
steps:
  - name: a
    image: i
    commands: [echo]
`)
	assert.ErrorContains(t, err, "parameter A: options is empty")

	// null is the empty value, which an option can allow; no default and
	// no value given is that value
	items, err := buildConfig(t, nil, nil, `
params:
  A: {options: [~, x]}
steps:
  - name: a
    image: i
    commands: [echo]
`)
	require.NoError(t, err)
	assert.Empty(t, stepsByName(items)["a"].Environment["A"])

	// options and no default: the run must choose
	_, err = buildConfig(t, nil, nil, `
params:
  A: {options: [x, y]}
steps:
  - name: a
    image: i
    commands: [echo]
`)
	assert.ErrorContains(t, err, `parameter A: "" is not one of x, y`)

	_, err = buildConfig(t, nil, nil, `
params:
  A: {default: light, choices: [light]}
steps:
  - name: a
    image: i
    commands: [echo]
`)
	assert.ErrorContains(t, err, `unknown key "choices"`)
}

// TestParamEmptyValueIsDefault: a run that gives a parameter empty, as a
// form that posts every field does, gets the default.
func TestParamEmptyValueIsDefault(t *testing.T) {
	t.Parallel()
	items, err := buildParams(t, map[string]string{"TIER": ""})
	require.False(t, pipeline_errors.HasBlockingErrors(err), "%v", err)
	assert.Equal(t, "light", stepsByName(items)["test"].Environment["TIER"])
}

// TestParamInDependsOn: a workflow selected to run brings the workflows its
// depends_on names through a parameter.
func TestParamInDependsOn(t *testing.T) {
	t.Parallel()
	m := &testMetadata{pipelineEvent: "manual"}
	workflow := func(name, config string) *YamlFile {
		return &YamlFile{Name: ".woodpecker/" + name + ".yaml", Data: []byte("when:\n  event: manual\nskip_clone: true\n" + config)}
	}
	build := func(vars map[string]string) ([]*Item, error) {
		b := PipelineBuilder{
			GetWorkflowMetadata: m.GetWorkflowMetadata,
			AdditionalEnvs:      vars,
			SelectedWorkflow:    "deploy",
			RepoTrusted:         &metadata.TrustedConfiguration{},
			Yamls: []*YamlFile{
				workflow("build-amd64", "steps: [{name: b, image: i, commands: [echo]}]\n"),
				workflow("build-arm64", "steps: [{name: b, image: i, commands: [echo]}]\n"),
				workflow("deploy", "params:\n  ARCH: {default: amd64, options: [amd64, arm64]}\ndepends_on: ['build-${ARCH}']\nsteps: [{name: d, image: i, commands: [echo]}]\n"),
			},
		}
		return b.Build()
	}
	names := func(items []*Item) []string {
		var out []string
		for _, item := range items {
			out = append(out, item.Workflow.Name)
		}
		return out
	}

	items, err := build(nil)
	require.False(t, pipeline_errors.HasBlockingErrors(err), "%v", err)
	assert.ElementsMatch(t, []string{"build-amd64", "deploy"}, names(items))

	items, err = build(map[string]string{"ARCH": "arm64"})
	require.False(t, pipeline_errors.HasBlockingErrors(err), "%v", err)
	assert.ElementsMatch(t, []string{"build-arm64", "deploy"}, names(items))

	_, err = build(map[string]string{"ARCH": "riscv"})
	assert.True(t, pipeline_errors.HasBlockingErrors(err))
	assert.ErrorContains(t, err, `parameter ARCH: "riscv" is not one of amd64, arm64`)
}

// TestParamNumericOptions: options and defaults may be written as numbers
// or booleans; they are compared as the strings a run gives.
func TestParamNumericOptions(t *testing.T) {
	t.Parallel()
	m := &testMetadata{pipelineEvent: "manual"}
	b := PipelineBuilder{
		GetWorkflowMetadata: m.GetWorkflowMetadata,
		AdditionalEnvs:      map[string]string{"N": "2", "ON": "true"},
		RepoTrusted:         &metadata.TrustedConfiguration{},
		Yamls: []*YamlFile{{Name: ".woodpecker.yaml", Data: []byte(`
when:
  event: manual
skip_clone: true
params:
  N: {default: 1, options: [1, 2]}
  ON: {default: false, options: [true, false]}
steps:
  - name: a
    image: scratch
    commands: [echo]
`)}},
	}
	items, err := b.Build()
	require.False(t, pipeline_errors.HasBlockingErrors(err), "%v", err)
	assert.Equal(t, "2", stepsByName(items)["a"].Environment["N"])
	assert.Equal(t, "true", stepsByName(items)["a"].Environment["ON"])
}

// TestParamOptionsOnlyWhenRunning: a workflow that does not run does not
// fail the pipeline over its parameters' options, and listing the
// workflows of a manual run only warns.
func TestParamOptionsOnlyWhenRunning(t *testing.T) {
	t.Parallel()
	m := &testMetadata{pipelineEvent: "push"}
	yamls := []*YamlFile{
		{Name: ".woodpecker/a.yaml", Data: []byte("when:\n  event: push\nskip_clone: true\nsteps: [{name: a, image: i, commands: [echo]}]\n")},
		{Name: ".woodpecker/nightly.yaml", Data: []byte("when:\n  event: [cron, manual]\nskip_clone: true\nparams:\n  TIER: {options: [x, y]}\nsteps: [{name: n, image: i, commands: [echo]}]\n")},
	}
	b := PipelineBuilder{GetWorkflowMetadata: m.GetWorkflowMetadata, RepoTrusted: &metadata.TrustedConfiguration{}, Yamls: yamls}
	items, err := b.Build()
	require.False(t, pipeline_errors.HasBlockingErrors(err), "%v", err)
	assert.Len(t, items, 1)

	manual := &testMetadata{pipelineEvent: "manual"}
	b = PipelineBuilder{GetWorkflowMetadata: manual.GetWorkflowMetadata, RepoTrusted: &metadata.TrustedConfiguration{}, Yamls: yamls, ListOnly: true}
	items, err = b.Build()
	require.False(t, pipeline_errors.HasBlockingErrors(err), "%v", err)
	assert.Len(t, items, 1, "the manual-run dialog lists the workflow")
	assert.ErrorContains(t, err, `parameter TIER: "" is not one of x, y`)

	b.ListOnly = false
	_, err = b.Build()
	assert.True(t, pipeline_errors.HasBlockingErrors(err), "the run itself needs a value")
}

func TestParamDeclarationErrors(t *testing.T) {
	t.Parallel()
	for config, want := range map[string]string{
		"params:\n  B: ${A}-y\n  A: x\n":              "parameter B: it uses A, which is declared after it",
		"params:\n  B: {options: ['${A}']}\n  A: x\n": "parameter B: it uses A, which is declared after it",
		"params:\n  A: one\n  A: two\n":               "parameter A is declared twice",
		"params:\n  path: x\n":                        "the name is reserved",
		"params:\n  ci_x: x\n":                        "the name is reserved",
		"params:\n  A: {options: [x], b: c}\n":        `unknown key "b"`,
	} {
		t.Run(want, func(t *testing.T) {
			_, err := buildConfig(t, nil, nil, config+"steps: [{name: a, image: i, commands: [echo]}]\n")
			assert.ErrorContains(t, err, want)
		})
	}
}

// TestParamOptionsLikeDefault: options are read like the default, so a
// default with an escape is one of its options.
func TestParamOptionsLikeDefault(t *testing.T) {
	t.Parallel()
	items, err := buildConfig(t, nil, nil, `
params:
  A: {default: 'a$$b', options: ['a$$b', '${CI_COMMIT_BRANCH}']}
steps: [{name: a, image: i, commands: [echo]}]
`)
	require.NoError(t, err)
	assert.Equal(t, "a$b", stepsByName(items)["a"].Environment["A"])

	items, err = buildConfig(t, map[string]string{"A": "main"}, nil, `
params:
  A: {default: x, options: [x, '${CI_COMMIT_BRANCH}']}
steps: [{name: a, image: i, commands: [echo]}]
`)
	require.NoError(t, err)
	assert.Equal(t, "main", stepsByName(items)["a"].Environment["A"])
}

// TestParamMergeKeys: parameters shared through an anchor, one overridden.
func TestParamMergeKeys(t *testing.T) {
	t.Parallel()
	items, err := buildConfig(t, nil, nil, `
variables:
  - &common
    TIER: light
    PIN: main
params:
  <<: *common
  PIN: stable
steps: [{name: a, image: i, commands: [echo]}]
`)
	require.NoError(t, err)
	env := stepsByName(items)["a"].Environment
	assert.Equal(t, "light", env["TIER"])
	assert.Equal(t, "stable", env["PIN"])
}

// TestParamStepEnvironment: a step cannot set a parameter's name in its
// environment: its commands would get the parameter, its environment its
// own value.
func TestParamStepEnvironment(t *testing.T) {
	t.Parallel()
	_, err := buildConfig(t, nil, nil, `
params:
  A: x
steps:
  - name: a
    image: i
    environment:
      A: override
    commands: ['echo ${A}']
`)
	assert.ErrorContains(t, err, "step a: environment A: A is a parameter of the workflow")
}

// TestParamPluginsWithoutGlobal: plugins get no parameters, nor a global
// variable of a parameter's name, which would be another value.
func TestParamPluginsWithoutGlobal(t *testing.T) {
	t.Parallel()
	items, err := buildConfig(t, nil, map[string]string{"TIER": "global", "OTHER": "o"}, `
params:
  TIER: light
steps:
  - name: plugin
    image: woodpeckerci/plugin-kaniko
    settings:
      tags: ${TIER}
`)
	require.NoError(t, err)
	env := stepsByName(items)["plugin"].Environment
	assert.Equal(t, "light", env["PLUGIN_TAGS"])
	assert.NotContains(t, env, "TIER")
	assert.Equal(t, "o", env["OTHER"])
}
