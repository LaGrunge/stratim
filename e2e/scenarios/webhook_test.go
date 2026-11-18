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
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

const webhookYAML = `
skip_clone: true

steps:
  - name: build
    image: dummy
    commands:
      - echo building
`

// The longest a webhook may take to be answered.
const webhookAckBudget = 200 * time.Millisecond

// hookAway is how long the server stays away while a webhook is sent:
// STRATIM_E2E_HOOK_AWAY, a minute on CI's longer job, or 5 s.
func hookAway(t *testing.T) time.Duration {
	t.Helper()
	away := 5 * time.Second
	if v := os.Getenv("STRATIM_E2E_HOOK_AWAY"); v != "" {
		var err error
		away, err = time.ParseDuration(v)
		require.NoError(t, err, "STRATIM_E2E_HOOK_AWAY")
	}
	return away
}

func pushPayload(commit string) setup.HookPayload {
	return setup.HookPayload{Event: model.EventPush, Branch: "main", Commit: commit, Message: "build " + commit}
}

// assertOnePipeline asserts the repo has, and keeps a while, the one pipeline
// of commit.
func assertOnePipeline(t *testing.T, env *setup.ServerEnv, commit string, within time.Duration) {
	t.Helper()
	setup.WaitForPipelines(t, env.Store, env.Fixtures.Repo, 1, within)
	// nothing more comes of it
	time.Sleep(time.Second)
	pipelines, err := env.Store.GetPipelineList(env.Fixtures.Repo, &model.ListOptionsWithAll{All: true}, nil)
	require.NoError(t, err)
	require.Len(t, pipelines, 1, "exactly one pipeline")
	assert.Equal(t, commit, pipelines[0].Commit)
	assert.Equal(t, model.EventPush, pipelines[0].Event)
}

// A forge delivering a webhook twice: answered at once both times, one
// pipeline.
func TestWebhookDeliveredTwice(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(webhookYAML)},
	})

	status, took := env.Deliver(t, "delivery-twice", pushPayload("c0ffee"))
	assert.Equal(t, http.StatusAccepted, status)
	assert.LessOrEqual(t, took, webhookAckBudget, "answered before the pipeline is made")
	status, took = env.Deliver(t, "delivery-twice", pushPayload("c0ffee"))
	assert.Equal(t, http.StatusOK, status, "taken already")
	assert.LessOrEqual(t, took, webhookAckBudget)

	assertOnePipeline(t, env, "c0ffee", 10*time.Second)
}

// A webhook sent while the server is away: the forge's delivery fails, and
// the server, back, has it delivered again, once.
func TestWebhookWhileServerAway(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: []byte(webhookYAML)},
	})
	env.APIURL(t)

	env.StopServer()
	status, _ := env.Deliver(t, "delivery-away", pushPayload("beef"))
	assert.Zero(t, status, "nobody answered")
	time.Sleep(hookAway(t))
	started := time.Now()
	env.StartServerAgain(t)

	assertOnePipeline(t, env, "beef", 5*time.Minute)
	t.Logf("pipeline %v after the server started", time.Since(started).Round(time.Second))
	attempts := env.Hooks.Attempts("delivery-away")
	require.Len(t, attempts, 2, "delivered again once")
	assert.Zero(t, attempts[0].StatusCode)
	assert.Equal(t, http.StatusAccepted, attempts[1].StatusCode)

	// reconciled again: nothing more to deliver
	assert.Zero(t, env.Reconciler.Once(t.Context()))
	assertOnePipeline(t, env, "beef", 10*time.Second)
}
