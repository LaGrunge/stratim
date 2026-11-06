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

// Package ciconfig checks this repository's own CI configuration.
package ciconfig

import (
	"flag"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/builder"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/jsonnet"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/compiler"
	"go.woodpecker-ci.org/woodpecker/v3/server/cronconfig"
)

const (
	configDir     = "../../.woodpecker"
	repoOwner     = "LaGrunge"
	repoName      = "stratim"
	defaultBranch = "main"
)

// codeChange and docsChange are the changed files of a code change and of a
// change to the documentation only; path conditions in .woodpecker/ decide
// which workflows and steps each of them starts.
var (
	codeChange = []string{"server/server.go", "go.mod"}
	docsChange = []string{"docs/README.md"}

	// Steps of the test workflow that test on MariaDB, including the
	// service they share.
	mariadbSteps = []string{"mariadb", "test-e2e-mariadb", "codecov"}
)

var update = flag.Bool("update", false, "rewrite the golden files of the CI")

// TestWorkflowsBuild builds .woodpecker/ the way the server does, for every
// event the repository's pipelines run on, and fails on errors the server
// would report only when such a pipeline is created: a step that depends on
// a step its conditions filter out, a lint error.
//
// It also pins the MariaDB tests to pushes to main: they once ran on pull
// requests only, and codecov, which depends on them, broke every push.
func TestWorkflowsBuild(t *testing.T) {
	prev := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.WarnLevel) // the builder traces every variable
	t.Cleanup(func() { zerolog.SetGlobalLevel(prev) })

	yamls, secrets := readConfig(t)

	tests := []struct {
		name    string
		event   metadata.Event
		branch  string
		changed []string
		// wantRuns: the event must start at least one workflow
		wantRuns bool
		// wantTest: steps the test workflow must have
		wantTest []string
		// cron is the cron's name; selected the workflow it runs, which
		// runs with its dependencies whatever their when says
		cron, selected string
		// wantWorkflows, if exact, are the workflows that run, all of them
		wantWorkflows []string
		exact         bool
	}{
		{name: "push to main", event: metadata.EventPush, changed: codeChange, wantRuns: true, wantTest: mariadbSteps},
		{name: "push to main, docs only", event: metadata.EventPush, changed: docsChange},
		{name: "push to main, no file list", event: metadata.EventPush},
		{name: "push to another branch", event: metadata.EventPush, branch: "feature", changed: codeChange},
		{name: "pull request", event: metadata.EventPull, changed: codeChange, wantRuns: true, wantTest: mariadbSteps},
		{name: "pull request, docs only", event: metadata.EventPull, changed: docsChange, wantRuns: true},
		{name: "pull request, no file list", event: metadata.EventPull, wantRuns: true},
		{name: "tag", event: metadata.EventTag, changed: codeChange},
		{name: "cron", event: metadata.EventCron},
		{
			name: "the synthetic cron", event: metadata.EventCron, cron: "synthetic-check/nightly", selected: "synthetic-check",
			wantWorkflows: syntheticWorkflows, exact: true,
		},
		{name: "a cron of the settings", event: metadata.EventCron, cron: "synthetic", exact: true},
		{name: "manual", event: metadata.EventManual},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := metadata.Metadata{}
			m.Repo.Owner, m.Repo.Name, m.Repo.Branch = repoOwner, repoName, defaultBranch
			m.Curr.Event = tt.event
			m.Curr.Commit.Branch = defaultBranch
			if tt.branch != "" {
				m.Curr.Commit.Branch = tt.branch
			}
			m.Curr.Commit.ChangedFiles = tt.changed
			m.Curr.Cron = tt.cron
			if tt.event == metadata.EventTag {
				m.Curr.Commit.Ref = "refs/tags/v0.0.0"
			}

			b := builder.PipelineBuilder{
				Yamls:               yamls,
				RepoTrusted:         &metadata.TrustedConfiguration{},
				CompilerOptions:     []compiler.Option{compiler.WithSecret(secrets...)},
				GetWorkflowMetadata: func(*builder.Workflow) metadata.Metadata { return m },
				SelectedWorkflow:    tt.selected,
				RunSelected:         tt.selected != "",
			}
			items, err := b.Build()
			require.False(t, pipeline_errors.HasBlockingErrors(err), "build: %v", err)
			if tt.wantRuns {
				assert.NotEmpty(t, items, "no workflow runs")
			}
			if tt.exact {
				var names []string
				for _, item := range items {
					names = append(names, item.Workflow.Name)
				}
				assert.ElementsMatch(t, tt.wantWorkflows, names, "the workflows that run")
			}
			if tt.wantTest != nil {
				assert.Subset(t, stepNames(items, "test"), tt.wantTest, "steps of the test workflow")
			}
		})
	}
}

// readConfig returns the config files and a placeholder for every secret
// the workflows they yield read, available to every event.
func readConfig(t *testing.T) ([]*builder.YamlFile, []compiler.Secret) {
	t.Helper()
	var files []string
	for _, pattern := range []string{"*.yaml", "*.jsonnet", "*.libsonnet"} {
		matches, err := filepath.Glob(filepath.Join(configDir, pattern))
		require.NoError(t, err)
		files = append(files, matches...)
	}
	require.NotEmpty(t, files, "no workflows in %s", configDir)

	var yamls []*builder.YamlFile
	for _, f := range files {
		data, err := os.ReadFile(f)
		require.NoError(t, err)
		yamls = append(yamls, &builder.YamlFile{Name: filepath.Join(".woodpecker", filepath.Base(f)), Data: data})
	}

	rendered, err := jsonnet.Expand(yamls)
	require.NoError(t, err)
	seen := make(map[string]bool)
	var secrets []compiler.Secret
	for _, w := range rendered {
		var workflow any
		require.NoError(t, yaml.Unmarshal(w.Data, &workflow), w.Name)
		for _, name := range secretNames(workflow) {
			if !seen[name] {
				seen[name] = true
				secrets = append(secrets, compiler.Secret{Name: name, Value: "placeholder"})
			}
		}
	}
	return yamls, secrets
}

// secretNames finds the secrets a workflow reads: every from_secret in it.
func secretNames(v any) []string {
	var names []string
	switch x := v.(type) {
	case map[string]any:
		if name, ok := x["from_secret"].(string); ok {
			names = append(names, name)
		}
		for _, e := range x {
			names = append(names, secretNames(e)...)
		}
	case []any:
		for _, e := range x {
			names = append(names, secretNames(e)...)
		}
	}
	return names
}

// TestCIGolden renders .woodpecker/ and compares each workflow with
// testdata/golden/<workflow>.yaml, so that a change to the CI shows as a
// diff of what it runs. The file names are the workflow names, which are
// also the commit status contexts branch protection requires and what the
// nightly log pruning matches (synthetic-%): renaming one is a visible
// change too. Run go test -update to rewrite them.
func TestCIGolden(t *testing.T) {
	yamls, _ := readConfig(t)
	rendered, err := jsonnet.Expand(yamls)
	require.NoError(t, err)

	const golden = "testdata/golden"
	if *update {
		require.NoError(t, os.RemoveAll(golden))
		require.NoError(t, os.MkdirAll(golden, 0o755))
	}
	var names []string
	for _, w := range rendered {
		name := jsonnet.WorkflowName(w.Name) + ".yaml"
		names = append(names, name)
		got, err := yaml.JSONToYAML(w.Data)
		require.NoError(t, err, w.Name)
		if *update {
			require.NoError(t, os.WriteFile(filepath.Join(golden, name), got, 0o600))
		}
		want, err := os.ReadFile(filepath.Join(golden, name))
		require.NoError(t, err, "golden file of %s (go test -update writes them)", w.Name)
		assert.Equal(t, string(want), string(got), "workflow %s", name)
	}
	entries, err := os.ReadDir(golden)
	require.NoError(t, err)
	var stale []string
	for _, e := range entries {
		if !slices.Contains(names, e.Name()) {
			stale = append(stale, e.Name())
		}
	}
	assert.Empty(t, stale, "golden files of workflows that no longer exist")
}

// stepNames returns the names of the steps and services of the named
// workflow, or nil when it does not run.
func stepNames(items []*builder.Item, workflow string) []string {
	var names []string
	for _, item := range items {
		if item.Workflow.Name != workflow || item.Config == nil {
			continue
		}
		for _, stage := range item.Config.Stages {
			for _, step := range stage.Steps {
				names = append(names, step.Name)
			}
		}
	}
	return names
}

// syntheticWorkflows are those the nightly synthetic cron runs.
var syntheticWorkflows = []string{"synthetic-check", "synthetic-logs", "synthetic-parallel-a", "synthetic-parallel-b", "synthetic-tests"}

// TestCrons: the server makes exactly one cron of .woodpecker/, the
// synthetic one, nightly at 03:00 UTC.
func TestCrons(t *testing.T) {
	yamls, _ := readConfig(t)
	crons, err := cronconfig.Desired(yamls, time.Now())
	require.NoError(t, err)
	require.Len(t, crons, 1)
	c := crons[0]
	assert.Equal(t, "synthetic-check/nightly", c.Name)
	assert.Equal(t, "synthetic-check", c.Workflow)
	assert.Equal(t, "0 3 * * *", c.Schedule)
	assert.Equal(t, "UTC", c.Timezone)
	assert.Empty(t, c.Variables)
}
