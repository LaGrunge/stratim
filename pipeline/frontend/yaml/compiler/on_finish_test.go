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

package compiler

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml"
)

// compileString compiles a workflow config without a clone step.
func compileString(t *testing.T, config string) (*backend_types.Config, error) {
	t.Helper()
	workflow, err := yaml.ParseString(config)
	require.NoError(t, err)
	return New(WithPrefix("test")).Compile(workflow)
}

// stageNames lists the step names of each stage.
func stageNames(config *backend_types.Config) [][]string {
	var stages [][]string
	for _, stage := range config.Stages {
		var names []string
		for _, step := range stage.Steps {
			names = append(names, step.Name)
		}
		stages = append(stages, names)
	}
	return stages
}

// stepByName finds a compiled step.
func stepByName(t *testing.T, config *backend_types.Config, name string) *backend_types.Step {
	t.Helper()
	for _, stage := range config.Stages {
		for _, step := range stage.Steps {
			if step.Name == name {
				return step
			}
		}
	}
	t.Fatalf("no step %q", name)
	return nil
}

// Finalizers are staged after all other steps, one by one in their declared
// order, wherever they are declared.
func TestCompileFinalizersLast(t *testing.T) {
	config, err := compileString(t, `
skip_clone: true
services:
  - name: db
    image: mysql
steps:
  - name: logs
    image: alpine
    commands: [echo logs]
    on_finish: true
  - name: build
    image: golang
    commands: [go build]
  - name: cleanup
    image: alpine
    commands: [echo cleanup]
    on_finish: true
  - name: test
    image: golang
    commands: [go test]
`)
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"db"}, {"build"}, {"test"}, {"logs"}, {"cleanup"}}, stageNames(config))
	assert.Equal(t, []bool{false, false, false, true, true}, finalizerStages(config))

	logs := stepByName(t, config, "logs")
	assert.True(t, logs.OnSuccess, "a finalizer without status runs on success")
	assert.True(t, logs.OnFailure, "and on failure, timeout and kill")
	assert.Nil(t, logs.RunOn, "which OnSuccess and OnFailure say")
	assert.Equal(t, DefaultFinalizeTimeout, config.FinalizeTimeout)
}

// Without finalizers there is no time for them.
func TestCompileNoFinalizers(t *testing.T) {
	config, err := compileString(t, "skip_clone: true\nsteps:\n  - name: build\n    image: golang\n    commands: [go build]\n")
	require.NoError(t, err)
	assert.Zero(t, config.FinalizeTimeout)
	assert.Equal(t, []bool{false}, finalizerStages(config))
}

// The finalizers' time: their own timeouts summed, at least the default, at
// most the cap.
func TestCompileFinalizeTimeout(t *testing.T) {
	finalizers := func(timeouts ...string) string {
		config := "skip_clone: true\nsteps:\n  - name: build\n    image: golang\n    commands: [go build]\n"
		for i, timeout := range timeouts {
			config += fmt.Sprintf("  - name: f%d\n    image: alpine\n    commands: [echo]\n    on_finish: true\n", i)
			if timeout != "" {
				config += "    timeout: " + timeout + "\n"
			}
		}
		return config
	}
	for _, tt := range []struct {
		timeouts []string
		want     time.Duration
	}{
		{[]string{""}, DefaultFinalizeTimeout},
		{[]string{"2m", ""}, DefaultFinalizeTimeout},
		{[]string{"15m", "10m", ""}, 25 * time.Minute},
		{[]string{"50m", "50m"}, MaxFinalizeTimeout},
	} {
		config, err := compileString(t, finalizers(tt.timeouts...))
		require.NoError(t, err)
		assert.Equal(t, tt.want, config.FinalizeTimeout, "timeouts %v", tt.timeouts)
	}
}

// finalizerStages lists whether each stage is one of finalizers.
func finalizerStages(config *backend_types.Config) []bool {
	var finalizer []bool
	for _, stage := range config.Stages {
		finalizer = append(finalizer, stage.Finalizer)
	}
	return finalizer
}

// In DAG mode the regular steps form their own graph; a finalizer's
// dependency on a regular step is met by the time it runs, one on another
// finalizer orders them.
func TestCompileFinalizersDependsOn(t *testing.T) {
	config, err := compileString(t, `
skip_clone: true
steps:
  - name: build
    image: golang
    commands: [go build]
  - name: test
    image: golang
    commands: [go test]
    depends_on: [build]
  - name: upload
    image: alpine
    commands: [echo upload]
    on_finish: true
    depends_on: [collect, test]
  - name: collect
    image: alpine
    commands: [echo collect]
    on_finish: true
    depends_on: [build]
  - name: notify
    image: alpine
    commands: [echo notify]
    on_finish: true
`)
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"build"}, {"test"}, {"collect", "notify"}, {"upload"}}, stageNames(config))
	assert.Equal(t, []string{"collect"}, stepByName(t, config, "upload").DependsOn)
	assert.Empty(t, stepByName(t, config, "collect").DependsOn)
}

// A regular step cannot depend on a finalizer: it runs after all of them.
func TestCompileDependsOnFinalizer(t *testing.T) {
	_, err := compileString(t, `
skip_clone: true
steps:
  - name: logs
    image: alpine
    commands: [echo logs]
    on_finish: true
  - name: build
    image: golang
    commands: [go build]
    depends_on: [logs]
`)
	assert.ErrorIs(t, err, &ErrStepDependsOnFinalizer{})
	assert.EqualError(t, err, "step 'build' depends on step 'logs', which is on_finish and runs after all other steps")
}

// A finalizer is never detached, and on a service on_finish means nothing.
func TestCompileFinalizerNotDetached(t *testing.T) {
	config, err := compileString(t, `
skip_clone: true
services:
  - name: db
    image: mysql
    on_finish: true
steps:
  - name: logs
    image: alpine
    commands: [echo logs]
    on_finish: true
    detach: true
`)
	require.NoError(t, err)
	assert.False(t, stepByName(t, config, "logs").Detached)
	assert.True(t, stepByName(t, config, "db").Detached)
	assert.Equal(t, [][]string{{"db"}, {"logs"}}, stageNames(config))
	assert.Equal(t, []bool{false, true}, finalizerStages(config), "the service is no finalizer")
}

// when.status sets the flags the agent checks the outcome so far with.
func TestCompileWhenStatus(t *testing.T) {
	config, err := compileString(t, `
skip_clone: true
steps:
  - name: default
    image: alpine
    commands: [echo]
  - name: on-timeout
    image: alpine
    commands: [echo]
    when:
      status: timeout
  - name: on-skipped
    image: alpine
    commands: [echo]
    when:
      status: [success, skipped]
  - name: on-failure-and-kill
    image: alpine
    commands: [echo]
    when:
      status: [failure, killed]
  - name: finalizer-on-failure
    image: alpine
    commands: [echo]
    on_finish: true
    when:
      status: failure
  - name: finalizer-on-timeout
    image: alpine
    commands: [echo]
    on_finish: true
    when:
      - status: timeout
      - branch: never
`)
	require.NoError(t, err)

	type flags struct {
		success, failure bool
		runOn            []string
	}
	got := func(name string) flags {
		s := stepByName(t, config, name)
		return flags{s.OnSuccess, s.OnFailure, s.RunOn}
	}
	assert.Equal(t, flags{success: true}, got("default"))
	assert.Equal(t, flags{runOn: []string{"timeout"}}, got("on-timeout"))
	assert.Equal(t, flags{success: true, runOn: []string{"success", "skipped"}}, got("on-skipped"))
	assert.Equal(t, flags{failure: true, runOn: []string{"failure", "killed"}}, got("on-failure-and-kill"))
	assert.Equal(t, flags{failure: true}, got("finalizer-on-failure"), "a status keeps a finalizer to it")
	assert.Equal(t, flags{runOn: []string{"timeout"}}, got("finalizer-on-timeout"), "a constraint that does not match does not run it always")
}
