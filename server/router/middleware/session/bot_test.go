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

package session

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/bottoken"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
)

type botEnv struct {
	store    store.Store
	owner    *model.User
	pipeline *model.Pipeline
	e        *gin.Engine
}

func newBotEnv(t *testing.T) *botEnv {
	t.Helper()
	gin.SetMode(gin.TestMode)
	s := datastore.NewTestStore(t)
	owner := &model.User{Login: "owner", ForgeRemoteID: "1", Hash: "owner-hash"}
	require.NoError(t, s.CreateUser(owner))
	repo := &model.Repo{UserID: owner.ID, ForgeRemoteID: "r1", Owner: "owner", Name: "repo", FullName: "owner/repo"}
	require.NoError(t, s.CreateRepo(repo))
	pipeline := &model.Pipeline{RepoID: repo.ID, Number: 1, Status: model.StatusSuccess}
	require.NoError(t, s.CreatePipeline(pipeline))

	e := gin.New()
	// the address of the connection, whatever X-Forwarded-For says
	require.NoError(t, e.SetTrustedProxies(nil))
	bots := NewBotRoutes("")
	e.Use(func(c *gin.Context) { c.Set("store", s) })
	e.Use(SetUser())
	e.Use(AuditBot(bots))
	e.Use(GuardBot(bots))
	who := func(c *gin.Context) {
		user, bot := User(c), Bot(c)
		switch {
		case user == nil:
			c.String(http.StatusOK, "guest")
		case bot == nil:
			c.String(http.StatusOK, "%s", user.Login)
		default:
			c.String(http.StatusOK, "%s bot %d admin %t", user.Login, bot.ID, user.Admin)
		}
	}
	v2 := e.Group("/api/v2")
	bots.Handle(v2, http.MethodGet, "/read", BotRoute{Scope: model.BotScopeRead}, who)
	bots.Handle(v2, http.MethodPost, "/pipelines/:pipeline_id/restart", BotRoute{Scope: model.BotScopeRestart, Audit: BotAuditPipeline},
		func(c *gin.Context) {
			SetBotAuditReason(c, "flaky runner")
			who(c)
		})
	bots.Handle(v2, http.MethodGet, "/any", BotRoute{}, who)
	bots.Handle(v2, http.MethodGet, "/repos/:repo_id/x", BotRoute{Scope: model.BotScopeRead}, who)
	v2.GET("/unlisted", who)
	e.GET("/api/user", who)
	return &botEnv{store: s, owner: owner, pipeline: pipeline, e: e}
}

// token creates a bot token of the owner, changed by change.
func (env *botEnv) token(t *testing.T, change func(*model.BotToken), scopes ...model.BotScope) (*model.BotToken, string) {
	t.Helper()
	secret, hash, shown := model.NewBotTokenSecret()
	token := &model.BotToken{
		OwnerID: env.owner.ID, Name: "bot", Hash: hash, Prefix: shown, Scopes: scopes,
		Created: time.Now().Unix(), Expires: time.Now().Add(time.Hour).Unix(),
	}
	if change != nil {
		change(token)
	}
	require.NoError(t, env.store.BotTokenCreate(token, 100, time.Now().Unix()))
	return token, secret
}

func (env *botEnv) do(t *testing.T, method, path string, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	maps.Copy(req.Header, header)
	w := httptest.NewRecorder()
	env.e.ServeHTTP(w, req)
	return w
}

// lastCall is the latest audit line of the token.
func (env *botEnv) lastCall(t *testing.T, id int64) *model.BotTokenCall {
	t.Helper()
	calls, err := env.store.BotTokenCallList(id, 1)
	require.NoError(t, err)
	require.Len(t, calls, 1, "an audit line")
	return calls[0]
}

func botHeader(secret string) http.Header {
	return http.Header{bottoken.Header: {secret}}
}

func TestBotTokenAuthenticates(t *testing.T) {
	env := newBotEnv(t)
	token, secret := env.token(t, nil, model.BotScopeRead)
	want := "owner bot " + strconv.FormatInt(token.ID, 10) + " admin false"

	header := botHeader(secret)
	header.Set("X-Forwarded-For", "203.0.113.9")
	w := env.do(t, http.MethodGet, "/api/v2/read", header)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, want, w.Body.String())
	call := env.lastCall(t, token.ID)
	assert.Equal(t, env.owner.ID, call.OwnerID)
	assert.Equal(t, http.MethodGet, call.Method)
	assert.Equal(t, "/api/v2/read", call.Route)
	assert.Equal(t, http.StatusOK, call.Status)
	assert.Equal(t, "192.0.2.1", call.IP, "the address of the connection, not X-Forwarded-For from a peer not trusted")
	assert.Empty(t, call.Reason)
	assert.NotZero(t, call.Created)

	got, err := env.store.BotTokenGet(token.ID)
	require.NoError(t, err)
	assert.NotZero(t, got.LastUsed, "the use is recorded")

	t.Run("as a Bearer token", func(t *testing.T) {
		w := env.do(t, http.MethodGet, "/api/v2/read", http.Header{"Authorization": {"Bearer " + secret}})
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, want, w.Body.String())
	})
	t.Run("the header wins over any other credential", func(t *testing.T) {
		header := botHeader(secret)
		header.Set("Authorization", "Bearer not-a-jwt")
		w := env.do(t, http.MethodGet, "/api/v2/read", header)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, want, w.Body.String())
	})
	t.Run("a route for any scope", func(t *testing.T) {
		w := env.do(t, http.MethodGet, "/api/v2/any", botHeader(secret))
		assert.Equal(t, http.StatusOK, w.Code)
	})
	t.Run("no token is no bot", func(t *testing.T) {
		w := env.do(t, http.MethodGet, "/api/v2/unlisted", nil)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "guest", w.Body.String())
	})
	t.Run("never an admin", func(t *testing.T) {
		owner, err := env.store.GetUser(env.owner.ID)
		require.NoError(t, err)
		owner.Admin = true
		require.NoError(t, env.store.UpdateUser(owner))
		defer func() {
			owner.Admin = false
			require.NoError(t, env.store.UpdateUser(owner))
		}()
		restricted, restrictedSecret := env.token(t, func(b *model.BotToken) { b.RepoIDs = []int64{env.pipeline.RepoID} }, model.BotScopeRead)
		w := env.do(t, http.MethodGet, "/api/v2/read", botHeader(restrictedSecret))
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "owner bot "+strconv.FormatInt(restricted.ID, 10)+" admin false", w.Body.String())

		// the admin's token without repos is refused
		w = env.do(t, http.MethodGet, "/api/v2/read", botHeader(secret))
		assert.Equal(t, http.StatusUnauthorized, w.Code)
		assert.Equal(t, bottoken.ReasonAdminAllRepos, env.lastCall(t, token.ID).Reason)
	})
}

func TestBotTokenRefused(t *testing.T) {
	env := newBotEnv(t)
	_, active := env.token(t, nil, model.BotScopeRead)
	revoked, revokedSecret := env.token(t, func(b *model.BotToken) { b.Revoked = time.Now().Unix() }, model.BotScopeRead)
	expired, expiredSecret := env.token(t, func(b *model.BotToken) { b.Expires = time.Now().Unix() - 1 }, model.BotScopeRead)

	// known tokens: a row of the token
	for _, tt := range []struct {
		name    string
		header  http.Header
		tokenID int64
		reason  string
	}{
		{"revoked", botHeader(revokedSecret), revoked.ID, bottoken.ReasonRevoked},
		{"expired", botHeader(expiredSecret), expired.ID, bottoken.ReasonExpired},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := env.do(t, http.MethodGet, "/api/v2/read", tt.header)
			assert.Equal(t, http.StatusUnauthorized, w.Code)
			assert.Equal(t, "invalid bot token", w.Body.String(), "the reason is not told")
			call := env.lastCall(t, tt.tokenID)
			assert.Equal(t, http.StatusUnauthorized, call.Status)
			assert.Equal(t, tt.reason, call.Reason)
			assert.Equal(t, "/api/v2/read", call.Route)
		})
	}

	// no token known: refused, and only logged
	for _, tt := range []struct {
		name   string
		path   string
		header http.Header
	}{
		{"unknown", "/api/v2/read", botHeader(active + "x")},
		{"malformed", "/api/v2/read", botHeader("not-a-bot-token")},
		{"empty", "/api/v2/read", botHeader("")},
		{"twice", "/api/v2/read", http.Header{bottoken.Header: {active, active}}},
		{"empty, with a valid Bearer token", "/api/v2/read", http.Header{bottoken.Header: {""}, "Authorization": {"Bearer " + active}}},
		{"in the query", "/api/v2/read?access_token=" + active, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := env.do(t, http.MethodGet, tt.path, tt.header)
			assert.Equal(t, http.StatusUnauthorized, w.Code)
			assert.Equal(t, "invalid bot token", w.Body.String())
			calls, err := env.store.BotTokenCallList(0, 10)
			require.NoError(t, err)
			assert.Empty(t, calls, "no row for a token not known")
		})
	}
}

func TestBotTokenRoutesAndScopes(t *testing.T) {
	env := newBotEnv(t)
	reader, readerSecret := env.token(t, nil, model.BotScopeRead)
	restarter, restarterSecret := env.token(t, nil, model.BotScopeRestart)
	restart := "/api/v2/pipelines/" + strconv.FormatInt(env.pipeline.ID, 10) + "/restart"

	t.Run("a scope missing", func(t *testing.T) {
		w := env.do(t, http.MethodPost, restart, botHeader(readerSecret))
		assert.Equal(t, http.StatusForbidden, w.Code)
		assert.Equal(t, "missing scope: restart", w.Body.String())
		call := env.lastCall(t, reader.ID)
		assert.Equal(t, http.StatusForbidden, call.Status)
		assert.Equal(t, "missing scope restart", call.Reason)
		assert.Zero(t, call.PipelineID, "what a refused call was about is not recorded")
		assert.Zero(t, call.RepoID)

		w = env.do(t, http.MethodGet, "/api/v2/read", botHeader(restarterSecret))
		assert.Equal(t, http.StatusForbidden, w.Code)
		assert.Equal(t, "missing scope: read", w.Body.String())
	})
	t.Run("the scope granted", func(t *testing.T) {
		w := env.do(t, http.MethodPost, restart, botHeader(restarterSecret))
		assert.Equal(t, http.StatusOK, w.Code)
		call := env.lastCall(t, restarter.ID)
		assert.Equal(t, http.StatusOK, call.Status)
		assert.Equal(t, "flaky runner", call.Reason, "the reason the handler recorded")
		assert.Equal(t, env.pipeline.RepoID, call.RepoID, "the repo of the pipeline of the route")
		assert.Equal(t, env.pipeline.ID, call.PipelineID)
	})
	for _, path := range []string{"/api/v2/unlisted", "/api/user", "/nowhere"} {
		t.Run("not open: "+path, func(t *testing.T) {
			w := env.do(t, http.MethodGet, path, botHeader(readerSecret))
			assert.Equal(t, http.StatusForbidden, w.Code)
			assert.Equal(t, "bot tokens are not accepted here", w.Body.String())
			call := env.lastCall(t, reader.ID)
			assert.Equal(t, http.StatusForbidden, call.Status)
			assert.Equal(t, "route not open to bot tokens", call.Reason)
			assert.Equal(t, path, call.Route)
		})
	}
	t.Run("the route, not the path, is recorded", func(t *testing.T) {
		w := env.do(t, http.MethodGet, "/api/v2/repos/7/x", botHeader(readerSecret))
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "/api/v2/repos/:repo_id/x", env.lastCall(t, reader.ID).Route)
	})
}

func TestBotRoutesOnlyBelowV2(t *testing.T) {
	e := gin.New()
	bots := NewBotRoutes("/ci")
	base := e.Group("/ci")
	bots.Handle(base.Group("/api/v2"), http.MethodGet, "/x", BotRoute{}, func(*gin.Context) {})
	// registered, but not opened to bot tokens
	bots.Handle(base.Group("/api"), http.MethodGet, "/user", BotRoute{}, func(*gin.Context) {})
	bots.Handle(e.Group("/api/v2"), http.MethodGet, "/y", BotRoute{}, func(*gin.Context) {})
	assert.Equal(t, []string{"GET /ci/api/v2/x"}, bots.Keys(), "below the root path's /api/v2 only")
	assert.Len(t, e.Routes(), 3)
}
