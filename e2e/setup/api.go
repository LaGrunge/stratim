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

package setup

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware"
	"go.woodpecker-ci.org/woodpecker/v3/shared/token"
	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// APIURL serves the whole router of the server over HTTP, once, as the
// server does: its authentication, its routes and their guards.
func (env *ServerEnv) APIURL(t *testing.T) string {
	t.Helper()
	env.apiOnce.Do(func() {
		gin.SetMode(gin.TestMode)
		handler := router.Load(http.NotFound,
			middleware.Store(env.Store),
			middleware.WorkflowEnd(env.WorkflowEnd),
			middleware.Stopping(t.Context()),
		)
		srv := httptest.NewServer(handler)
		t.Cleanup(srv.Close)
		env.apiBase = srv.URL
	})
	return env.apiBase
}

// OwnerToken is the personal token of the fixture owner, who is given a
// hash to sign it with.
func (env *ServerEnv) OwnerToken(t *testing.T) string {
	t.Helper()
	owner := env.Fixtures.Owner
	if owner.Hash == "" {
		owner.Hash = "e2e-owner-hash"
		require.NoError(t, env.Store.UpdateUser(owner))
	}
	tok := token.New(token.UserToken)
	tok.Set("user-id", strconv.FormatInt(owner.ID, 10))
	signed, err := tok.Sign(owner.Hash)
	require.NoError(t, err)
	return signed
}

// OwnerClient is a woodpecker-go client of APIURL as the fixture owner.
func (env *ServerEnv) OwnerClient(t *testing.T) woodpecker.Client {
	t.Helper()
	return woodpecker.NewClient(env.APIURL(t), &http.Client{Transport: &bearer{token: env.OwnerToken(t)}})
}

// BotClient is a woodpecker-go client of APIURL with the bot token.
func (env *ServerEnv) BotClient(t *testing.T, secret string) woodpecker.Client {
	t.Helper()
	return woodpecker.NewClient(env.APIURL(t), &http.Client{Transport: woodpecker.NewBotTokenTransport(secret, nil)})
}

// AllowOwner stores the owner's permission on the fixture repo, synced
// now, so that the forge is not asked.
func (env *ServerEnv) AllowOwner(t *testing.T, push bool) {
	t.Helper()
	require.NoError(t, env.Store.PermUpsert(&model.Perm{
		UserID: env.Fixtures.Owner.ID, RepoID: env.Fixtures.Repo.ID, Pull: true, Push: push, Synced: time.Now().Unix(),
	}))
}

type bearer struct{ token string }

func (b *bearer) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(req)
}
