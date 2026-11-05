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
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/builder"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/compiler"
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

var fromSecret = regexp.MustCompile(`from_secret:\s*([A-Za-z0-9_]+)`)

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
			if tt.event == metadata.EventTag {
				m.Curr.Commit.Ref = "refs/tags/v0.0.0"
			}

			b := builder.PipelineBuilder{
				Yamls:               yamls,
				RepoTrusted:         &metadata.TrustedConfiguration{},
				CompilerOptions:     []compiler.Option{compiler.WithSecret(secrets...)},
				GetWorkflowMetadata: func(*builder.Workflow) metadata.Metadata { return m },
			}
			items, err := b.Build()
			require.False(t, pipeline_errors.HasBlockingErrors(err), "build: %v", err)
			if tt.wantRuns {
				assert.NotEmpty(t, items, "no workflow runs")
			}
			if tt.wantTest != nil {
				assert.Subset(t, stepNames(items, "test"), tt.wantTest, "steps of the test workflow")
			}
		})
	}
}

// readConfig returns the workflow files and a placeholder for every secret
// they read, available to every event.
//
// A workflow reads a secret only through from_secret, so scanning the text
// finds them all; one it missed would fail the build as "secret not found"
// rather than pass unnoticed.
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
	seen := make(map[string]bool)
	var secrets []compiler.Secret
	for _, f := range files {
		data, err := os.ReadFile(f)
		require.NoError(t, err)
		yamls = append(yamls, &builder.YamlFile{Name: filepath.Join(".woodpecker", filepath.Base(f)), Data: data})
		for _, match := range fromSecret.FindAllSubmatch(data, -1) {
			if name := string(match[1]); !seen[name] {
				seen[name] = true
				secrets = append(secrets, compiler.Secret{Name: name, Value: "placeholder"})
			}
		}
	}
	return yamls, secrets
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
