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

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/bottoken"
	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/shared/token"
)

// userAuth is the header of a request made with the personal token of the
// user.
func userAuth(t *testing.T, user *model.User) http.Header {
	t.Helper()
	tok := token.New(token.UserToken)
	tok.Set("user-id", strconv.FormatInt(user.ID, 10))
	signed, err := tok.Sign(user.Hash)
	require.NoError(t, err)
	return http.Header{"Authorization": {"Bearer " + signed}}
}

func botAuth(secret string) http.Header {
	return http.Header{bottoken.Header: {secret}}
}

// call makes a request of the test server and gives its status and body.
func (env *eventsV2Env) call(t *testing.T, method, path string, header http.Header, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(raw)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, env.srv.URL+path, reader)
	require.NoError(t, err)
	maps.Copy(req.Header, header)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out
}

// createBot creates a bot token of the user through the API.
func (env *eventsV2Env) createBot(t *testing.T, req BotTokenRequest) BotTokenCreated {
	t.Helper()
	status, body := env.call(t, http.MethodPost, "/api/v2/bot-tokens", userAuth(t, env.user), req)
	require.Equal(t, http.StatusCreated, status, string(body))
	var created BotTokenCreated
	require.NoError(t, json.Unmarshal(body, &created))
	return created
}

func TestBotTokenManagement(t *testing.T) {
	env := newEventsV2Env(t)
	me := userAuth(t, env.user)

	created := env.createBot(t, BotTokenRequest{Name: "watcher", Scopes: []model.BotScope{model.BotScopeRead}, RepoIDs: []int64{env.mine.ID}})
	assert.True(t, model.IsBotToken(created.Token))
	assert.Equal(t, created.Token[:12], created.Prefix)
	assert.Equal(t, model.BotTokenActive, created.Status)
	assert.Equal(t, env.user.ID, created.OwnerID)
	assert.InDelta(t, time.Now().Add(30*24*time.Hour).Unix(), created.Expires, 5, "30 days by default")

	status, body := env.call(t, http.MethodGet, "/api/v2/bot-tokens", me, nil)
	require.Equal(t, http.StatusOK, status)
	assert.NotContains(t, string(body), created.Token, "the secret is told once")
	assert.NotContains(t, string(body), model.HashBotToken(created.Token), "the hash is never told")
	var listed []BotTokenInfo
	require.NoError(t, json.Unmarshal(body, &listed))
	require.Len(t, listed, 1)
	assert.Equal(t, "watcher", listed[0].Name)
	assert.Equal(t, []int64{env.mine.ID}, listed[0].RepoIDs)

	t.Run("refused", func(t *testing.T) {
		admin, err := env.store.GetUser(env.user.ID)
		require.NoError(t, err)
		for _, tt := range []struct {
			name string
			req  BotTokenRequest
			want string
		}{
			{"no name", BotTokenRequest{Scopes: []model.BotScope{model.BotScopeRead}}, "name"},
			{"annotate is reserved", BotTokenRequest{Name: "a", Scopes: []model.BotScope{model.BotScopeAnnotate}}, "cannot be granted"},
			{"a lifetime too long", BotTokenRequest{Name: "a", Scopes: []model.BotScope{model.BotScopeRead}, ExpiresInDays: 366}, "expires_in_days"},
			{"a repo the owner may not read", BotTokenRequest{Name: "a", Scopes: []model.BotScope{model.BotScopeRead}, RepoIDs: []int64{env.theirs.ID}}, "no repo"},
			{"a repo that does not exist", BotTokenRequest{Name: "a", Scopes: []model.BotScope{model.BotScopeRead}, RepoIDs: []int64{9999}}, "no repo"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				status, body := env.call(t, http.MethodPost, "/api/v2/bot-tokens", userAuth(t, admin), tt.req)
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Contains(t, string(body), tt.want)
			})
		}
		t.Run("an admin's without repos", func(t *testing.T) {
			env.setAdmin(t, true)
			defer env.setAdmin(t, false)
			status, body := env.call(t, http.MethodPost, "/api/v2/bot-tokens", me,
				BotTokenRequest{Name: "a", Scopes: []model.BotScope{model.BotScopeRead}})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Contains(t, string(body), "has to name its repos")
		})
		t.Run("by a bot token", func(t *testing.T) {
			bot := botAuth(created.Token)
			status, _ := env.call(t, http.MethodGet, "/api/v2/bot-tokens", bot, nil)
			assert.Equal(t, http.StatusForbidden, status)
			status, _ = env.call(t, http.MethodPost, "/api/v2/bot-tokens", bot, BotTokenRequest{Name: "b", Scopes: []model.BotScope{model.BotScopeRead}})
			assert.Equal(t, http.StatusForbidden, status)
			status, _ = env.call(t, http.MethodDelete, fmt.Sprintf("/api/v2/bot-tokens/%d", created.ID), bot, nil)
			assert.Equal(t, http.StatusForbidden, status)
		})
		t.Run("all tokens to a user", func(t *testing.T) {
			status, _ := env.call(t, http.MethodGet, "/api/v2/bot-tokens?all=true", me, nil)
			assert.Equal(t, http.StatusForbidden, status)
		})
	})

	t.Run("at most 20 active", func(t *testing.T) {
		other, err := env.store.GetUserByLogin(0, "other")
		require.NoError(t, err)
		for i := range botTokensPerUser {
			status, body := env.call(t, http.MethodPost, "/api/v2/bot-tokens", userAuth(t, other),
				BotTokenRequest{Name: fmt.Sprint("t", i), Scopes: []model.BotScope{model.BotScopeRead}})
			require.Equal(t, http.StatusCreated, status, string(body))
		}
		status, _ := env.call(t, http.MethodPost, "/api/v2/bot-tokens", userAuth(t, other),
			BotTokenRequest{Name: "one more", Scopes: []model.BotScope{model.BotScopeRead}})
		assert.Equal(t, http.StatusConflict, status)
	})

	t.Run("whoami", func(t *testing.T) {
		status, body := env.call(t, http.MethodGet, "/api/v2/whoami", botAuth(created.Token), nil)
		require.Equal(t, http.StatusOK, status)
		var who Whoami
		require.NoError(t, json.Unmarshal(body, &who))
		assert.Equal(t, "me", who.User.Login)
		require.NotNil(t, who.Bot)
		assert.Equal(t, created.ID, who.Bot.ID)
		assert.Equal(t, []model.BotScope{model.BotScopeRead}, who.Bot.Scopes)
		assert.NotContains(t, string(body), created.Token)

		status, body = env.call(t, http.MethodGet, "/api/v2/whoami", me, nil)
		require.Equal(t, http.StatusOK, status)
		who = Whoami{}
		require.NoError(t, json.Unmarshal(body, &who))
		assert.Nil(t, who.Bot)
	})

	t.Run("calls", func(t *testing.T) {
		status, body := env.call(t, http.MethodGet, fmt.Sprintf("/api/v2/bot-tokens/%d/calls?limit=1", created.ID), me, nil)
		require.Equal(t, http.StatusOK, status)
		var calls []model.BotTokenCall
		require.NoError(t, json.Unmarshal(body, &calls))
		require.Len(t, calls, 1)
		assert.Equal(t, "/api/v2/whoami", calls[0].Route)

		status, _ = env.call(t, http.MethodGet, fmt.Sprintf("/api/v2/bot-tokens/%d/calls?limit=0", created.ID), me, nil)
		assert.Equal(t, http.StatusBadRequest, status)
		other, err := env.store.GetUserByLogin(0, "other")
		require.NoError(t, err)
		status, _ = env.call(t, http.MethodGet, fmt.Sprintf("/api/v2/bot-tokens/%d/calls", created.ID), userAuth(t, other), nil)
		assert.Equal(t, http.StatusNotFound, status, "only the owner sees the calls")
	})

	t.Run("revoke", func(t *testing.T) {
		other, err := env.store.GetUserByLogin(0, "other")
		require.NoError(t, err)
		path := fmt.Sprintf("/api/v2/bot-tokens/%d", created.ID)
		status, _ := env.call(t, http.MethodDelete, path, userAuth(t, other), nil)
		assert.Equal(t, http.StatusNotFound, status, "only the owner revokes")
		status, _ = env.call(t, http.MethodGet, "/api/v2/whoami", botAuth(created.Token), nil)
		assert.Equal(t, http.StatusOK, status, "still active")

		status, _ = env.call(t, http.MethodDelete, path, me, nil)
		assert.Equal(t, http.StatusNoContent, status)
		status, _ = env.call(t, http.MethodDelete, path, me, nil)
		assert.Equal(t, http.StatusNoContent, status, "revoking again changes nothing")

		status, body := env.call(t, http.MethodGet, "/api/v2/whoami", botAuth(created.Token), nil)
		assert.Equal(t, http.StatusUnauthorized, status)
		assert.Equal(t, "invalid bot token", string(body))

		status, body = env.call(t, http.MethodGet, "/api/v2/bot-tokens", me, nil)
		require.Equal(t, http.StatusOK, status)
		var listed []BotTokenInfo
		require.NoError(t, json.Unmarshal(body, &listed))
		require.Len(t, listed, 1)
		assert.Equal(t, model.BotTokenRevoked, listed[0].Status, "a revoked token stays listed")
	})
}

func TestEventsV2BotToken(t *testing.T) {
	env := newEventsV2Env(t)
	for _, repo := range []*model.Repo{env.mine, env.theirs, env.open} {
		env.fail(repo, env.pipeline(t, repo, 1))
	}

	t.Run("never more than its owner", func(t *testing.T) {
		bot := env.createBot(t, BotTokenRequest{Name: "all", Scopes: []model.BotScope{model.BotScopeRead}})
		c := env.connect(t, "since=0&min_severity=error", botAuth(bot.Token))
		require.Equal(t, "hello", c.next(t).event)
		assert.Equal(t, []int64{env.mine.ID, env.mine.ID, env.mine.ID, env.open.ID, env.open.ID, env.open.ID}, repoIDs(c.quiet(t)))
	})
	t.Run("only its repos", func(t *testing.T) {
		bot := env.createBot(t, BotTokenRequest{Name: "open", Scopes: []model.BotScope{model.BotScopeRead}, RepoIDs: []int64{env.open.ID}})
		c := env.connect(t, "since=0&min_severity=error", botAuth(bot.Token))
		require.Equal(t, "hello", c.next(t).event)
		assert.Equal(t, []int64{env.open.ID, env.open.ID, env.open.ID}, repoIDs(c.quiet(t)))

		resp := env.get(t, fmt.Sprintf("repo=%d", env.mine.ID), botAuth(bot.Token))
		defer resp.Body.Close()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, "a repo it is restricted away from")
	})
	t.Run("not the read scope", func(t *testing.T) {
		bot := env.createBot(t, BotTokenRequest{Name: "restart", Scopes: []model.BotScope{model.BotScopeRestart}})
		resp := env.get(t, "", botAuth(bot.Token))
		defer resp.Body.Close()
		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	})
	t.Run("the hello of an admin's", func(t *testing.T) {
		bot := env.createBot(t, BotTokenRequest{Name: "admin", Scopes: []model.BotScope{model.BotScopeRead}, RepoIDs: []int64{env.mine.ID}})
		env.setAdmin(t, true)
		defer env.setAdmin(t, false)
		c := env.connect(t, "", botAuth(bot.Token))
		hello := c.next(t)
		require.Equal(t, "hello", hello.event)
		var data events.Hello
		require.NoError(t, json.Unmarshal([]byte(hello.data), &data))
		assert.Nil(t, data.Head, "a bot token is never an admin")
	})
}

func TestEventsV2EndsWhenTheBotTokenIsRevoked(t *testing.T) {
	env := newEventsV2Env(t)
	v2AccessTTL = 100 * time.Millisecond
	bot := env.createBot(t, BotTokenRequest{Name: "watcher", Scopes: []model.BotScope{model.BotScopeRead}})
	c := env.connect(t, "", botAuth(bot.Token))
	require.Equal(t, "hello", c.next(t).event)

	// a reset of the owner's personal token is not of the bot token
	user, err := env.store.GetUser(env.user.ID)
	require.NoError(t, err)
	user.Hash = "h1-reset"
	require.NoError(t, env.store.UpdateUser(user))
	env.fail(env.mine, env.pipeline(t, env.mine, 1))
	assert.Equal(t, "pipeline.started", c.next(t).event, "the stream goes on")

	_, err = env.store.BotTokenRevoke(bot.ID, time.Now().Unix())
	require.NoError(t, err)
	var f sseFrame
	for f = c.next(t); f.event != "error"; f = c.next(t) {
	}
	assert.Contains(t, f.data, `"reason":"unauthorized"`)
	_, open := <-c.frames
	assert.False(t, open, "the stream ends")
}

func TestWaitV2BotToken(t *testing.T) {
	env := newEventsV2Env(t)
	bot := env.createBot(t, BotTokenRequest{Name: "open", Scopes: []model.BotScope{model.BotScopeRead}, RepoIDs: []int64{env.open.ID}})
	open, mine := env.pipeline(t, env.open, 1), env.pipeline(t, env.mine, 1)

	status, body := env.call(t, http.MethodGet, fmt.Sprintf("/api/v2/pipelines/%d/wait?timeout=1", open.ID), botAuth(bot.Token), nil)
	assert.Equal(t, http.StatusNoContent, status, "waited for the pipeline: %s", body)
	status, _ = env.call(t, http.MethodGet, fmt.Sprintf("/api/v2/pipelines/%d/wait?timeout=1", mine.ID), botAuth(bot.Token), nil)
	assert.Equal(t, http.StatusNotFound, status, "a repo it is restricted away from")

	calls, err := env.store.BotTokenCallList(bot.ID, 2)
	require.NoError(t, err)
	require.Len(t, calls, 2)
	assert.Equal(t, http.StatusNotFound, calls[0].Status)
	assert.Zero(t, calls[0].PipelineID, "what a refused call was about is not recorded")
	assert.Zero(t, calls[0].RepoID)
	assert.Equal(t, env.open.ID, calls[1].RepoID)
	assert.Equal(t, open.ID, calls[1].PipelineID)
}

func TestRestartV2Refused(t *testing.T) {
	env := newEventsV2Env(t)
	restarter := env.createBot(t, BotTokenRequest{Name: "restarter", Scopes: []model.BotScope{model.BotScopeRestart}})
	restricted := env.createBot(t, BotTokenRequest{Name: "open", Scopes: []model.BotScope{model.BotScopeRestart}, RepoIDs: []int64{env.open.ID}})
	reader := env.createBot(t, BotTokenRequest{Name: "reader", Scopes: []model.BotScope{model.BotScopeRead}})
	mine, theirs := env.pipeline(t, env.mine, 1), env.pipeline(t, env.theirs, 1)
	path := func(p *model.Pipeline) string { return fmt.Sprintf("/api/v2/pipelines/%d/restart", p.ID) }

	for _, tt := range []struct {
		name   string
		header http.Header
		path   string
		body   any
		status int
		want   string
	}{
		{"without the restart scope", botAuth(reader.Token), path(mine), nil, http.StatusForbidden, "missing scope: restart"},
		{"an owner who may only pull", botAuth(restarter.Token), path(mine), nil, http.StatusNotFound, ""},
		{"an owner who may not pull", botAuth(restarter.Token), path(theirs), nil, http.StatusNotFound, "not found"},
		{"a repo it is restricted away from", botAuth(restricted.Token), path(mine), nil, http.StatusNotFound, "not found"},
		{"no such pipeline", botAuth(restarter.Token), "/api/v2/pipelines/9999/restart", nil, http.StatusNotFound, ""},
		{"a long reason", botAuth(restarter.Token), path(mine), RestartRequest{Reason: strings.Repeat("x", 201)}, http.StatusBadRequest, "reason"},
		{"a user who may only pull", userAuth(t, env.user), path(mine), RestartRequest{FailedOnly: true}, http.StatusNotFound, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, body := env.call(t, http.MethodPost, tt.path, tt.header, tt.body)
			assert.Equal(t, tt.status, status)
			assert.Contains(t, string(body), tt.want)
		})
	}

	status, _ := env.call(t, http.MethodPost, path(mine), botAuth(restarter.Token), RestartRequest{Reason: "flaky"})
	require.Equal(t, http.StatusNotFound, status)
	calls, err := env.store.BotTokenCallList(restarter.ID, 1)
	require.NoError(t, err)
	require.Len(t, calls, 1)
	assert.Equal(t, "flaky", calls[0].Reason)
	assert.Zero(t, calls[0].RepoID, "what a refused call was about is not recorded")
	assert.Zero(t, calls[0].PipelineID)

	// nor the repo of a pipeline of a repo the owner may not read
	status, _ = env.call(t, http.MethodPost, path(theirs), botAuth(restarter.Token), nil)
	require.Equal(t, http.StatusNotFound, status)
	calls, err = env.store.BotTokenCallList(restarter.ID, 1)
	require.NoError(t, err)
	require.Len(t, calls, 1)
	assert.Zero(t, calls[0].RepoID)
	assert.Zero(t, calls[0].PipelineID)
}

// A token its owner made unrestricted before they became an admin is
// refused, at once and in a stream already open.
func TestBotTokenOfAPromotedOwner(t *testing.T) {
	env := newEventsV2Env(t)
	v2AccessTTL = 100 * time.Millisecond
	for _, repo := range []*model.Repo{env.mine, env.theirs, env.open} {
		env.fail(repo, env.pipeline(t, repo, 1))
	}
	bot := env.createBot(t, BotTokenRequest{Name: "all", Scopes: []model.BotScope{model.BotScopeRead}})
	restricted := env.createBot(t, BotTokenRequest{Name: "mine", Scopes: []model.BotScope{model.BotScopeRead}, RepoIDs: []int64{env.mine.ID}})
	c := env.connect(t, "", botAuth(bot.Token))
	require.Equal(t, "hello", c.next(t).event)

	env.setAdmin(t, true)
	var f sseFrame
	for f = c.next(t); f.event != "error"; f = c.next(t) {
	}
	assert.Contains(t, f.data, `"reason":"unauthorized"`, "the open stream ends")
	status, _ := env.call(t, http.MethodGet, "/api/v2/whoami", botAuth(bot.Token), nil)
	assert.Equal(t, http.StatusUnauthorized, status)

	// one that names its repos reads them as its owner did, never as an
	// admin: not the private repo of another user
	stream := env.connect(t, "since=0&min_severity=error", botAuth(restricted.Token))
	require.Equal(t, "hello", stream.next(t).event)
	assert.Equal(t, []int64{env.mine.ID, env.mine.ID, env.mine.ID}, repoIDs(stream.quiet(t)))
}

// A wait ends by the time its bot token expires.
func TestWaitV2EndsWhenTheBotTokenExpires(t *testing.T) {
	env := newEventsV2Env(t)
	secret, hash, shown := model.NewBotTokenSecret()
	require.NoError(t, env.store.BotTokenCreate(&model.BotToken{
		OwnerID: env.user.ID, Name: "short", Hash: hash, Prefix: shown, Scopes: []model.BotScope{model.BotScopeRead},
		Created: time.Now().Unix(), Expires: time.Now().Add(time.Second).Unix(),
	}, 20, time.Now().Unix()))
	p := env.pipeline(t, env.mine, 1)

	started := time.Now()
	status, _ := env.call(t, http.MethodGet, fmt.Sprintf("/api/v2/pipelines/%d/wait?timeout=30", p.ID), botAuth(secret), nil)
	assert.Equal(t, http.StatusNoContent, status)
	assert.Less(t, time.Since(started), 2500*time.Millisecond, "not the 30 s asked")
}
