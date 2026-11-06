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
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// TestBotToken drives the server's router with a bot token, as an agent
// watching the CI would: it is created by its owner, tells who it is, reads
// events, waits for a pipeline and restarts it; a token without the scope is
// refused; the owner revokes it; and every call is in its audit.
func TestBotToken(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: simpleSuccessYAML},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)
	env.AllowOwner(t, true)

	owner := env.OwnerClient(t)
	created, err := owner.BotTokenCreate(woodpecker.BotTokenCreateOptions{
		Name:    "watcher",
		Scopes:  []string{woodpecker.BotScopeRead, woodpecker.BotScopeRestart},
		RepoIDs: []int64{env.Fixtures.Repo.ID},
	})
	require.NoError(t, err)
	require.True(t, woodpecker.IsBotToken(created.Token))
	bot := env.BotClient(t, created.Token)
	calls := 0

	who, err := bot.Whoami()
	calls++
	require.NoError(t, err)
	assert.Equal(t, env.Fixtures.Owner.Login, who.User.Login)
	require.NotNil(t, who.Bot)
	assert.Equal(t, created.ID, who.Bot.ID)
	assert.Equal(t, []string{woodpecker.BotScopeRead, woodpecker.BotScopeRestart}, who.Bot.Scopes)

	// the events of a pipeline, and a wait for its end
	stream := env.StreamEventsV2Bot(t, fmt.Sprintf("repo=%d", env.Fixtures.Repo.ID), created.Token)
	calls++ // counted when the stream ends, with the test
	first, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	got, _ := stream.Until(t, time.Minute, setup.PipelineFinishedEvent(first.ID))
	assert.Equal(t, first.ID, got[len(got)-1].PipelineID)

	waited, err := bot.PipelineWait(first.ID, woodpecker.PipelineWaitOptions{TimeoutS: 60})
	calls++
	require.NoError(t, err)
	assert.True(t, waited.Met)
	assert.Equal(t, "success", waited.Pipeline.Status)

	// a restart, with the reason in the audit
	restarted, err := bot.PipelineRestart(first.ID, woodpecker.PipelineRestartOptions{Reason: "e2e"})
	calls++
	require.NoError(t, err)
	assert.Equal(t, first.Number, restarted.Parent)
	done := setup.WaitForPipeline(t, env.Store, restarted.ID)
	assert.Equal(t, model.StatusSuccess, done.Status)

	// a v1 route refuses it
	_, err = bot.Repo(env.Fixtures.Repo.ID)
	calls++
	assertStatus(t, err, http.StatusForbidden)

	// a token without the restart scope
	reader, err := owner.BotTokenCreate(woodpecker.BotTokenCreateOptions{Name: "reader", Scopes: []string{woodpecker.BotScopeRead}})
	require.NoError(t, err)
	_, err = env.BotClient(t, reader.Token).PipelineRestart(first.ID, woodpecker.PipelineRestartOptions{})
	assertStatus(t, err, http.StatusForbidden)

	// revoked: refused from then on
	require.NoError(t, owner.BotTokenRevoke(created.ID))
	_, err = bot.Whoami()
	calls++
	assertStatus(t, err, http.StatusUnauthorized)

	// the stream is counted once it ended
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream.Close()
	var audit []*woodpecker.BotTokenCall
	for ctx.Err() == nil {
		audit, err = owner.BotTokenCalls(created.ID, 100)
		require.NoError(t, err)
		if len(audit) >= calls {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Len(t, audit, calls, "an audit line per call")
	byRoute := make(map[string]*woodpecker.BotTokenCall)
	for _, call := range audit {
		byRoute[call.Method+" "+call.Route] = call
	}
	restart := byRoute["POST /api/v2/pipelines/:pipeline_id/restart"]
	require.NotNil(t, restart)
	assert.Equal(t, http.StatusOK, restart.Status)
	assert.Equal(t, "e2e", restart.Reason)
	assert.Equal(t, env.Fixtures.Repo.ID, restart.RepoID)
	assert.Equal(t, first.ID, restart.PipelineID)
	assert.Equal(t, http.StatusOK, byRoute["GET /api/v2/events"].Status)
	assert.Equal(t, http.StatusForbidden, byRoute["GET /api/repos/:repo_id"].Status)
	refused := 0
	for _, call := range audit {
		if call.Status == http.StatusUnauthorized {
			refused++
			assert.Equal(t, "revoked token", call.Reason)
			assert.Equal(t, "/api/v2/whoami", call.Route)
		}
	}
	assert.Equal(t, 1, refused, "the call after the revocation")

	listed, err := owner.BotTokenList()
	require.NoError(t, err)
	require.Len(t, listed, 2)
	assert.Equal(t, "revoked", listed[1].Status)
	assert.NotZero(t, listed[1].LastUsed)
}

func assertStatus(t *testing.T, err error, status int) {
	t.Helper()
	clientErr, ok := errors.AsType[*woodpecker.ClientError](err)
	require.True(t, ok, "a client error: %v", err)
	assert.Equal(t, status, clientErr.StatusCode, clientErr.Message)
}
