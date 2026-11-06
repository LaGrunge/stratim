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

package router

import (
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/bottoken"
	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	"go.woodpecker-ci.org/woodpecker/v3/server/explain"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
	"go.woodpecker-ci.org/woodpecker/v3/server/summary"
)

var routeParam = regexp.MustCompile(`[:*][a-z_]+`)

// botRouterEnv is the server's router over a sqlite store: an owner who may
// pull and push the repos mine and theirs, each with a pipeline and a
// step.
type botRouterEnv struct {
	store       store.Store
	owner       *model.User
	mine, other *model.Repo
	pipelines   map[int64]*model.Pipeline
	steps       map[int64]*model.Step
	engine      *gin.Engine
	bots        *session.BotRoutes
}

func newBotRouterEnv(t *testing.T) *botRouterEnv {
	t.Helper()
	gin.SetMode(gin.TestMode)
	orig := server.Config
	t.Cleanup(func() { server.Config = orig })
	server.Config.Server.RootPath = ""

	s := datastore.NewTestStore(t)
	env := &botRouterEnv{store: s, pipelines: make(map[int64]*model.Pipeline), steps: make(map[int64]*model.Step)}
	env.owner = &model.User{Login: "owner", ForgeRemoteID: "1", Hash: "h", Expiry: time.Now().Add(time.Hour).Unix()}
	require.NoError(t, s.CreateUser(env.owner))
	repo := func(name string) *model.Repo {
		r := &model.Repo{UserID: env.owner.ID, ForgeRemoteID: model.ForgeRemoteID(name), Owner: "owner", Name: name, FullName: "owner/" + name, Visibility: model.VisibilityPrivate}
		require.NoError(t, s.CreateRepo(r))
		require.NoError(t, s.PermUpsert(&model.Perm{UserID: env.owner.ID, RepoID: r.ID, Pull: true, Push: true, Synced: time.Now().Unix()}))
		p := &model.Pipeline{RepoID: r.ID, Number: 1, Status: model.StatusFailure}
		require.NoError(t, s.CreatePipeline(p))
		st := &model.Step{PipelineID: p.ID, PPID: 1, PID: 2, Name: "test", State: model.StatusFailure}
		w := &model.Workflow{PipelineID: p.ID, PID: 1, Name: "build", State: model.StatusFailure, Children: []*model.Step{st}}
		require.NoError(t, s.WorkflowsCreate([]*model.Workflow{w}))
		env.pipelines[r.ID], env.steps[r.ID] = p, st
		return r
	}
	env.mine, env.other = repo("mine"), repo("other")

	server.Config.Services.Events = events.NewHub(events.Options{})
	server.Config.Services.Durations = summary.NewDurations(0)
	server.Config.Services.Explain = explain.New(s, explain.Options{})
	server.Config.Services.LogWindows = explain.NewWindows(s)
	manager := manager_mocks.NewMockManager(t)
	manager.EXPECT().ForgeFromUser(mock.Anything).Return(forge_mocks.NewMockForge(t), nil).Maybe()
	server.Config.Services.Manager = manager

	env.engine, env.bots = load(http.NotFound, func(c *gin.Context) { c.Set("store", s) })
	return env
}

func (env *botRouterEnv) token(t *testing.T, scopes []model.BotScope, repos ...int64) string {
	t.Helper()
	secret, hash, shown := model.NewBotTokenSecret()
	require.NoError(t, env.store.BotTokenCreate(&model.BotToken{
		OwnerID: env.owner.ID, Name: "bot", Hash: hash, Prefix: shown, Scopes: scopes, RepoIDs: repos,
		Created: time.Now().Unix(), Expires: time.Now().Add(time.Hour).Unix(),
	}, 100, time.Now().Unix()))
	return secret
}

func (env *botRouterEnv) do(method, path string, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	maps.Copy(req.Header, header)
	w := httptest.NewRecorder()
	env.engine.ServeHTTP(w, req)
	return w
}

var allScopes = []model.BotScope{model.BotScopeRead, model.BotScopeRestart, model.BotScopeAnnotate}

// Bot tokens are open on these routes only: a route added to them is
// added here on purpose.
func TestBotRoutes(t *testing.T) {
	env := newBotRouterEnv(t)
	assert.Equal(t, []string{
		"GET /api/v2/events",
		"GET /api/v2/pipelines/:pipeline_id/wait",
		"GET /api/v2/steps/:step_id/explain",
		"GET /api/v2/steps/:step_id/log",
		"GET /api/v2/whoami",
		"POST /api/v2/pipelines/:pipeline_id/restart",
	}, env.bots.Keys())
}

// Every other route of the server refuses a bot token, whatever its
// scopes.
func TestBotTokensOnlyOnTheirRoutes(t *testing.T) {
	env := newBotRouterEnv(t)
	secret := env.token(t, allScopes, env.mine.ID)
	open := make(map[string]bool)
	for _, key := range env.bots.Keys() {
		open[key] = true
	}

	tried := 0
	for _, route := range env.engine.Routes() {
		key := route.Method + " " + route.Path
		if open[key] {
			continue
		}
		tried++
		t.Run(key, func(t *testing.T) {
			w := env.do(route.Method, routeParam.ReplaceAllString(route.Path, "1"), http.Header{bottoken.Header: {secret}})
			assert.Equal(t, http.StatusForbidden, w.Code)
			assert.Equal(t, "bot tokens are not accepted here", w.Body.String())
		})
	}
	assert.Greater(t, tried, 100, "the routes of the server")
}

// A token restricted to a repo gets 404 for another repo its owner may
// use, on every route open to bot tokens that is about a repo.
func TestBotTokenRestrictedToItsRepos(t *testing.T) {
	env := newBotRouterEnv(t)
	secret := env.token(t, allScopes, env.mine.ID)
	header := http.Header{bottoken.Header: {secret}}
	pipeline, step := env.pipelines[env.other.ID], env.steps[env.other.ID]

	foreign := map[string]string{
		"GET /api/v2/events":                          fmt.Sprintf("/api/v2/events?repo=%d", env.other.ID),
		"GET /api/v2/pipelines/:pipeline_id/wait":     fmt.Sprintf("/api/v2/pipelines/%d/wait?timeout=1", pipeline.ID),
		"GET /api/v2/steps/:step_id/explain":          fmt.Sprintf("/api/v2/steps/%d/explain", step.ID),
		"GET /api/v2/steps/:step_id/log":              fmt.Sprintf("/api/v2/steps/%d/log", step.ID),
		"POST /api/v2/pipelines/:pipeline_id/restart": fmt.Sprintf("/api/v2/pipelines/%d/restart", pipeline.ID),
	}
	for _, key := range env.bots.Keys() {
		if key == "GET /api/v2/whoami" {
			continue // about no repo
		}
		path, ok := foreign[key]
		if !assert.True(t, ok, "%s is tried with a repo the token is restricted away from", key) {
			continue
		}
		method, _, _ := strings.Cut(key, " ")
		t.Run(key, func(t *testing.T) {
			w := env.do(method, path, header)
			assert.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
		})
	}

	// the owner's own token, unrestricted, reads the other repo's step
	unrestricted := env.token(t, allScopes)
	w := env.do(http.MethodGet, fmt.Sprintf("/api/v2/steps/%d/log", step.ID), http.Header{bottoken.Header: {unrestricted}})
	assert.NotEqual(t, http.StatusNotFound, w.Code, "the fixture: the owner may read the other repo")
	w = env.do(http.MethodGet, fmt.Sprintf("/api/v2/steps/%d/log", env.steps[env.mine.ID].ID), header)
	assert.NotEqual(t, http.StatusNotFound, w.Code, "the fixture: the token may read its repo")
}

// The address of an audit line is the connection's, unless it comes from
// a trusted proxy.
func TestBotAuditAddress(t *testing.T) {
	env := newBotRouterEnv(t)
	secret := env.token(t, allScopes, env.mine.ID)
	header := http.Header{bottoken.Header: {secret}, "X-Forwarded-For": {"203.0.113.9"}}
	last := func() string {
		token, err := env.store.BotTokenFindByHash(model.HashBotToken(secret))
		require.NoError(t, err)
		calls, err := env.store.BotTokenCallList(token.ID, 1)
		require.NoError(t, err)
		require.Len(t, calls, 1)
		return calls[0].IP
	}

	require.Equal(t, http.StatusOK, env.do(http.MethodGet, "/api/v2/whoami", header).Code)
	assert.Equal(t, "192.0.2.1", last(), "X-Forwarded-For of a peer that is not trusted is not taken")

	server.Config.Server.TrustedProxies = []string{"192.0.2.0/24"}
	env.engine, env.bots = load(http.NotFound, func(c *gin.Context) { c.Set("store", env.store) })
	require.Equal(t, http.StatusOK, env.do(http.MethodGet, "/api/v2/whoami", header).Code)
	assert.Equal(t, "203.0.113.9", last(), "behind a trusted proxy")
}

// A call made with the token of an admin does not change the admin: the
// owner as the token acts, never an admin, is neither refreshed nor
// stored.
func TestBotCallKeepsItsOwnerAnAdmin(t *testing.T) {
	env := newBotRouterEnv(t)
	env.owner.Admin = true
	env.owner.Expiry = time.Now().Add(-time.Hour).Unix() // the forge token is to be refreshed
	require.NoError(t, env.store.UpdateUser(env.owner))
	type refreshingForge struct {
		*forge_mocks.MockForge
		*forge_mocks.MockRefresher
	}
	// no expectations: a refresh fails the test
	f := &refreshingForge{forge_mocks.NewMockForge(t), forge_mocks.NewMockRefresher(t)}
	manager := manager_mocks.NewMockManager(t)
	manager.EXPECT().ForgeFromUser(mock.Anything).Return(f, nil).Maybe()
	server.Config.Services.Manager = manager

	secret := env.token(t, allScopes, env.mine.ID)
	for _, path := range []string{"/api/v2/whoami", fmt.Sprintf("/api/v2/steps/%d/log", env.steps[env.mine.ID].ID), "/api/user"} {
		env.do(http.MethodGet, path, http.Header{bottoken.Header: {secret}})
	}
	owner, err := env.store.GetUser(env.owner.ID)
	require.NoError(t, err)
	assert.True(t, owner.Admin, "still an admin")
	assert.Equal(t, env.owner.Expiry, owner.Expiry, "its forge token untouched")
}
