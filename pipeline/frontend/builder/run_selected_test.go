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

// TestRunSelected: a cron of a workflow's config runs it and the workflows
// it depends on whatever their when says, and no other.
func TestRunSelected(t *testing.T) {
	t.Parallel()
	m := &testMetadata{pipelineEvent: "cron"}
	workflow := func(name, config string) *YamlFile {
		return &YamlFile{Name: ".woodpecker/" + name + ".yaml", Data: []byte("when:\n  event: push\nskip_clone: true\n" + config)}
	}
	b := PipelineBuilder{
		GetWorkflowMetadata: m.GetWorkflowMetadata,
		SelectedWorkflow:    "nightly",
		RunSelected:         true,
		RepoTrusted:         &metadata.TrustedConfiguration{},
		Yamls: []*YamlFile{
			workflow("build", "steps: [{name: b, image: i, commands: [echo]}]\n"),
			workflow("nightly", "depends_on: [build]\nsteps: [{name: n, image: i, commands: [echo]}]\n"),
			workflow("lint", "steps: [{name: l, image: i, commands: [echo]}]\n"),
		},
	}
	items, err := b.Build()
	require.False(t, pipeline_errors.HasBlockingErrors(err), "%v", err)
	var names []string
	for _, item := range items {
		names = append(names, item.Workflow.Name)
	}
	assert.ElementsMatch(t, []string{"build", "nightly"}, names)

	b.RunSelected = false
	_, err = b.Build()
	assert.True(t, pipeline_errors.HasBlockingErrors(err), "without it, the when of each still holds")
}

// TestRunSelectedKeepsStatus: what the pipeline was started by is dropped
// from a selected workflow's when, not whether it runs after a failure.
func TestRunSelectedKeepsStatus(t *testing.T) {
	t.Parallel()
	m := &testMetadata{pipelineEvent: "cron"}
	b := PipelineBuilder{
		GetWorkflowMetadata: m.GetWorkflowMetadata,
		SelectedWorkflow:    "check",
		RunSelected:         true,
		RepoTrusted:         &metadata.TrustedConfiguration{},
		Yamls: []*YamlFile{
			{Name: ".woodpecker/load.yaml", Data: []byte("when: {event: push}\nskip_clone: true\nsteps: [{name: l, image: i, commands: [echo]}]\n")},
			{Name: ".woodpecker/check.yaml", Data: []byte("when: [{event: push, status: [success, failure]}]\nskip_clone: true\ndepends_on: [load]\nsteps: [{name: c, image: i, commands: [echo]}]\n")},
		},
	}
	items, err := b.Build()
	require.False(t, pipeline_errors.HasBlockingErrors(err), "%v", err)
	for _, item := range items {
		if item.Workflow.Name == "check" {
			assert.Contains(t, item.RunsOn, "failure", "it still runs after its dependency fails")
			return
		}
	}
	t.Fatal("check did not run")
}
