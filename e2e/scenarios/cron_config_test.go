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

//go:build test

package scenarios

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	cron_scheduler "go.woodpecker-ci.org/woodpecker/v3/server/cron"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

var cronWorkflows = []*forge_types.FileMeta{
	{Name: ".woodpecker/build.yaml", Data: []byte(`
when:
  event: push
skip_clone: true
steps: [{name: build, image: dummy, commands: [echo build]}]
`)},
	{Name: ".woodpecker/nightly.yaml", Data: []byte(`
when:
  event: push
skip_clone: true
depends_on: [build]
crons:
  full:
    schedule: 0 3 * * *
    variables: {TIER: full}
params:
  TIER: light
steps:
  - name: test
    image: dummy
    commands: [echo test]
  - name: full-only
    image: dummy
    commands: [echo full]
    when:
      evaluate: 'TIER == "full"'
`)},
	{Name: ".woodpecker/lint.yaml", Data: []byte(`
when:
  event: push
skip_clone: true
steps: [{name: lint, image: dummy, commands: [echo lint]}]
`)},
}

// TestCronFromConfig: a push to the default branch makes the crons its
// workflows declare; such a cron runs its workflow and what that depends
// on, whatever their when, with its variables, and nothing else.
func TestCronFromConfig(t *testing.T) {
	env := setup.StartServer(t.Context(), t, cronWorkflows)
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)
	repo := env.Fixtures.Repo
	repo.Branch = "main"
	require.NoError(t, env.Store.UpdateRepo(repo))

	push, err := pipeline.Create(t.Context(), env.Store, repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	setup.WaitForPipelineStatus(t, env.Store, push.ID, model.StatusSuccess, 30*time.Second)

	crons, err := env.Store.CronList(repo, &model.ListOptionsWithAll{All: true})
	require.NoError(t, err)
	require.Len(t, crons, 1)
	cron := crons[0]
	assert.Equal(t, "nightly/full", cron.Name)
	assert.True(t, cron.Managed)
	assert.Equal(t, "nightly", cron.Workflow)

	// what the cron scheduler creates when it fires
	_, run, err := cron_scheduler.CreatePipeline(t.Context(), env.Store, cron)
	require.NoError(t, err)
	created, err := pipeline.Create(t.Context(), env.Store, repo, run)
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)
	require.Equal(t, model.StatusSuccess, finished.Status, "%+v", finished.Errors)

	workflows, err := env.Store.WorkflowGetTree(finished)
	require.NoError(t, err)
	var names []string
	for _, w := range workflows {
		names = append(names, w.Name)
	}
	assert.ElementsMatch(t, []string{"build", "nightly"}, names, "the cron's workflow and its dependency, not lint")
	assert.Contains(t, stepNames(t, env, finished), "full-only", "the cron's variables reach the parameters")
}

// TestCronFromOtherBranch: a push to another branch changes no crons.
func TestCronFromOtherBranch(t *testing.T) {
	env := setup.StartServer(t.Context(), t, cronWorkflows)
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)
	repo := env.Fixtures.Repo
	repo.Branch = "develop"
	require.NoError(t, env.Store.UpdateRepo(repo))

	push, err := pipeline.Create(t.Context(), env.Store, repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	setup.WaitForPipeline(t, env.Store, push.ID)

	crons, err := env.Store.CronList(repo, &model.ListOptionsWithAll{All: true})
	require.NoError(t, err)
	assert.Empty(t, crons)
}

// TestCronFromConfigNeedsApproval: a push that waits for approval makes no
// crons; approving it does.
func TestCronFromConfigNeedsApproval(t *testing.T) {
	env := setup.StartServer(t.Context(), t, cronWorkflows)
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)
	repo := env.Fixtures.Repo
	repo.Branch = "main"
	repo.RequireApproval = model.RequireApprovalAllEvents
	require.NoError(t, env.Store.UpdateRepo(repo))

	blocked, err := pipeline.Create(t.Context(), env.Store, repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	require.Equal(t, model.StatusBlocked, blocked.Status)
	crons, err := env.Store.CronList(repo, &model.ListOptionsWithAll{All: true})
	require.NoError(t, err)
	assert.Empty(t, crons, "not before approval")

	approved, err := pipeline.Approve(t.Context(), env.Store, blocked, env.Fixtures.Owner, repo)
	require.NoError(t, err)
	setup.WaitForPipelineStatus(t, env.Store, approved.ID, model.StatusSuccess, 30*time.Second)
	crons, err = env.Store.CronList(repo, &model.ListOptionsWithAll{All: true})
	require.NoError(t, err)
	require.Len(t, crons, 1)
	assert.Equal(t, "nightly/full", crons[0].Name)
}

// TestCronFollowsConfig: each push to the default branch makes the crons
// what its config declares, keeping whether they are enabled. [skip ci], a
// push with no config and one that is not the head of the branch change
// none; a config whose crons cannot be read changes none and says why; a
// push after a dropped pipeline applies, though it takes its number.
func TestCronFollowsConfig(t *testing.T) {
	env := setup.StartServer(t.Context(), t, cronWorkflows)
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)
	repo := env.Fixtures.Repo
	repo.Branch = "main"
	require.NoError(t, env.Store.UpdateRepo(repo))

	pushCommit := func(commit, message string) (*model.Pipeline, error) {
		p := env.DummyPipeline(model.EventPush)
		p.Commit, p.Message = commit, message
		created, err := pipeline.Create(t.Context(), env.Store, repo, p)
		if err == nil {
			created = setup.WaitForPipeline(t, env.Store, created.ID)
		}
		return created, err
	}
	push := func(message string) (*model.Pipeline, error) {
		return pushCommit("deadbeef", message) // the head the forge reports
	}
	crons := func() []*model.Cron {
		list, err := env.Store.CronList(repo, &model.ListOptionsWithAll{All: true})
		require.NoError(t, err)
		return list
	}
	warned := func(p *model.Pipeline, text string) bool {
		for _, e := range p.Errors {
			if e.IsWarning && strings.Contains(e.Message, text) {
				return true
			}
		}
		return false
	}

	first, err := push("first")
	require.NoError(t, err)
	require.Len(t, crons(), 1)
	cron := crons()[0]
	cron.Enabled = false
	require.NoError(t, env.Store.CronSetEnabled(repo, cron))

	// a new schedule: rescheduled, still disabled
	rescheduled := withCronSchedule(t, "0 4 * * *")
	env.Files.Set(rescheduled)
	_, err = push("reschedule")
	require.NoError(t, err)
	require.Len(t, crons(), 1)
	assert.Equal(t, "0 4 * * *", crons()[0].Schedule)
	assert.False(t, crons()[0].Enabled, "disabled in the settings, it stays so")

	// [skip ci]: nothing read, nothing changed
	env.Files.Set(withCronSchedule(t, "0 5 * * *"))
	_, err = push("docs [skip ci]")
	assert.ErrorIs(t, err, pipeline.ErrFiltered)
	assert.Equal(t, "0 4 * * *", crons()[0].Schedule)

	// a commit the branch has moved past, delivered late: changes nothing
	late, err := pushCommit("0ldc0mmit", "late")
	require.NoError(t, err)
	assert.True(t, warned(late, "is the head of main now"), "%+v", late.Errors)
	assert.Equal(t, "0 4 * * *", crons()[0].Schedule)

	// crons that cannot be read: none changed, and the pipeline says why
	env.Files.Set(withCronSchedule(t, "0 0 30 2 *"))
	bad, err := push("never runs")
	require.NoError(t, err)
	assert.True(t, warned(bad, "none changed"), "%+v", bad.Errors)
	assert.Equal(t, "0 4 * * *", crons()[0].Schedule)

	// no config at all: the pipeline is dropped, the crons stay
	env.Files.Set(nil)
	_, err = push("no config")
	assert.ErrorIs(t, err, pipeline.ErrFiltered)
	require.Len(t, crons(), 1)

	// a config with no workflow to run on a push still says which crons
	// there are; its pipeline is dropped
	tagsOnly := withCronSchedule(t, "0 7 * * *")
	for _, f := range tagsOnly {
		f.Data = bytes.ReplaceAll(f.Data, []byte("event: push"), []byte("event: tag"))
	}
	env.Files.Set(tagsOnly)
	_, err = push("tags only")
	assert.ErrorIs(t, err, pipeline.ErrFiltered)
	assert.Equal(t, "0 7 * * *", crons()[0].Schedule)

	// the next push takes the dropped pipeline's number, and applies
	env.Files.Set(withCronSchedule(t, "0 6 * * *"))
	again, err := push("again")
	require.NoError(t, err)
	assert.Equal(t, bad.Number+1, again.Number, "the number of the dropped pipelines")
	require.Len(t, crons(), 1)
	assert.Equal(t, "0 6 * * *", crons()[0].Schedule)
	assert.Less(t, first.Number, again.Number)
}

// TestCronNameTaken: a declared cron whose name a cron of the settings has
// is not applied, and the pipeline says so; the settings' cron stays.
func TestCronNameTaken(t *testing.T) {
	env := setup.StartServer(t.Context(), t, cronWorkflows)
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)
	repo := env.Fixtures.Repo
	repo.Branch = "main"
	require.NoError(t, env.Store.UpdateRepo(repo))
	require.NoError(t, env.Store.CronCreate(&model.Cron{RepoID: repo.ID, Name: "Nightly/Full", Schedule: "@daily", Timezone: "UTC", Enabled: true}))

	created, err := pipeline.Create(t.Context(), env.Store, repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	finished := setup.WaitForPipeline(t, env.Store, created.ID)
	var messages []string
	for _, e := range finished.Errors {
		messages = append(messages, e.Message)
	}
	assert.Contains(t, messages, "cron nightly/full is not applied: a cron made in the repository's settings has the name")

	crons, err := env.Store.CronList(repo, &model.ListOptionsWithAll{All: true})
	require.NoError(t, err)
	require.Len(t, crons, 1)
	assert.False(t, crons[0].Managed)
	assert.Equal(t, "@daily", crons[0].Schedule)
}

// withCronSchedule is cronWorkflows with the nightly cron on schedule.
func withCronSchedule(t *testing.T, schedule string) []*forge_types.FileMeta {
	t.Helper()
	files := make([]*forge_types.FileMeta, len(cronWorkflows))
	for i, f := range cronWorkflows {
		data := f.Data
		if f.Name == ".woodpecker/nightly.yaml" {
			data = bytes.Replace(data, []byte("0 3 * * *"), []byte(schedule), 1)
			require.NotEqual(t, f.Data, data)
		}
		files[i] = &forge_types.FileMeta{Name: f.Name, Data: data}
	}
	return files
}
