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

package scenarios

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	"go.woodpecker-ci.org/woodpecker/v3/server/api"
	cron_scheduler "go.woodpecker-ci.org/woodpecker/v3/server/cron"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/shared/token"
)

// supersedeStart starts the server with a long-running pipeline that
// events cancel the previous pipelines of, and the first one, running.
func supersedeStart(t *testing.T, events []model.WebhookEvent, first func(env *setup.ServerEnv) *model.Pipeline) (*setup.ServerEnv, *model.Pipeline) {
	t.Helper()
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: cancelPipelineYAML},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)
	repo := env.Fixtures.Repo
	repo.Branch = "main"
	repo.CancelPreviousPipelineEvents = events
	require.NoError(t, env.Store.UpdateRepo(repo))

	running := first(env)
	setup.WaitForStepRunning(t, env.Store, running.ID, "long-running")
	return env, running
}

// assertSuperseded waits for the earlier pipeline to be killed by the
// newer, through the cancel of previous pipelines.
func assertSuperseded(t *testing.T, env *setup.ServerEnv, earlier *model.Pipeline) {
	t.Helper()
	killed := setup.WaitForPipelineStatus(t, env.Store, earlier.ID, model.StatusKilled, 30*time.Second)
	require.NotNil(t, killed.CancelInfo, "canceled by the cancel of previous pipelines")
	assert.Equal(t, earlier.Number+1, killed.CancelInfo.SupersededBy)
	setup.WaitForWorkflowsDone(t, env.Store, killed)
}

// A second push through the webhook kills the first push's pipeline: the
// server's end of workflows, which the request carries, reaches the cancel
// of previous pipelines through the webhook's background context.
func TestHookSupersedesPrevious(t *testing.T) {
	env, first := supersedeStart(t, []model.WebhookEvent{model.EventPush}, func(env *setup.ServerEnv) *model.Pipeline {
		created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
		require.NoError(t, err)
		return created
	})
	repo := env.Fixtures.Repo
	repo.WebhookSecret = "secret-of-the-supersede-test"
	require.NoError(t, env.Store.UpdateRepo(repo))
	env.Forge.On("Hook", mock.Anything, mock.Anything).Return(repo, env.DummyPipeline(model.EventPush), nil).Once()

	postHook(t, env.Store, env.WorkflowEnd, repo)

	assertSuperseded(t, env, first)
}

// postHook delivers a webhook of repo as the server's router does: the
// request carries the store and the server's end of workflows.
func postHook(t *testing.T, s store.Store, end pipeline.WorkflowEnd, repo *model.Repo) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	middleware.Store(s)(c)
	middleware.WorkflowEnd(end)(c)

	hookToken := token.New(token.HookToken)
	hookToken.Set("repo-id", fmt.Sprintf("%d", repo.ID))
	signed, err := hookToken.Sign(repo.WebhookSecret)
	require.NoError(t, err)
	header := http.Header{}
	header.Set("Authorization", "Bearer "+signed)
	c.Request = &http.Request{Header: header, URL: &url.URL{Scheme: "https"}}

	api.PostHook(c)
	require.Less(t, w.Code, http.StatusBadRequest, w.Body.String())
}

// A second run of a cron kills its first run's pipeline: the server's end
// of workflows, which the cron scheduler's context carries, reaches the
// cancel of previous pipelines.
func TestCronSupersedesPrevious(t *testing.T) {
	var cron *model.Cron
	env, first := supersedeStart(t, []model.WebhookEvent{model.EventCron}, func(env *setup.ServerEnv) *model.Pipeline {
		cron = &model.Cron{
			RepoID: env.Fixtures.Repo.ID, Name: "often", Schedule: "@every 1m", Timezone: "UTC",
			Enabled: true, CreatorID: env.Fixtures.Owner.ID, Branch: "main",
		}
		require.NoError(t, env.Store.CronCreate(cron))
		_, run, err := cron_scheduler.CreatePipeline(t.Context(), env.Store, cron)
		require.NoError(t, err)
		created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, run)
		require.NoError(t, err)
		return created
	})

	// the cron due, as the scheduler finds it every minute
	cron.NextExec = time.Now().Add(-time.Minute).Unix()
	require.NoError(t, env.Store.CronUpdate(env.Fixtures.Repo, cron))
	cron_scheduler.RunDue(pipeline.InjectWorkflowEnd(t.Context(), env.WorkflowEnd), env.Store, time.Now())

	assertSuperseded(t, env, first)
}
