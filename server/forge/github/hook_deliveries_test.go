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

package github

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/go-github/v93/github"
	github_mock "github.com/migueleliasweb/go-github-mock/src/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func TestHookDeliveries(t *testing.T) {
	since := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	delivery := func(id int64, guid string, at time.Time, status int) *github.HookDelivery {
		return &github.HookDelivery{
			ID: new(id), GUID: new(guid), Event: new("push"), StatusCode: new(status),
			DeliveredAt: &github.Timestamp{Time: at},
		}
	}
	var cursors []string
	ctx := mockedContext(
		t, t.Context(),
		github_mock.WithRequestMatch(github_mock.GetReposHooksByOwnerByRepo, []*github.Hook{
			{ID: new(int64(1)), Config: &github.HookConfig{URL: new("https://other.example.com/hook")}},
			{ID: new(int64(2)), Config: &github.HookConfig{URL: new("https://ci.example.com/api/hook?access_token=x")}},
		}),
		github_mock.WithRequestMatchHandler(github_mock.GetReposHooksDeliveriesByOwnerByRepoByHookId, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/repos/octocat/hello-world/hooks/2/deliveries", r.URL.Path)
			cursor := r.URL.Query().Get("cursor")
			cursors = append(cursors, cursor)
			if cursor == "" {
				w.Header().Set("Link", `<https://api.github.com/repos/octocat/hello-world/hooks/2/deliveries?cursor=next1>; rel="next"`)
				writeJSON(t, w, []*github.HookDelivery{
					delivery(12, "g3", since.Add(time.Minute), 0),
					delivery(11, "g2", since.Add(30*time.Second), http.StatusAccepted),
				})
				return
			}
			w.Header().Set("Link", `<https://api.github.com/repos/octocat/hello-world/hooks/2/deliveries?cursor=next2>; rel="next"`)
			writeJSON(t, w, []*github.HookDelivery{
				delivery(10, "g1", since, http.StatusBadGateway),
				delivery(9, "g0", since.Add(-time.Second), 0),
			})
		})),
	)

	c := &client{API: defaultAPI, url: defaultURL}
	got, err := c.HookDeliveries(ctx, &model.User{AccessToken: "token"}, &model.Repo{ForgeRemoteID: "paged", Owner: "octocat", Name: "hello-world"}, "https://ci.example.com/api/hook", since)
	require.NoError(t, err)
	assert.Equal(t, []string{"", "next1"}, cursors, "pages read until one older than since")
	assert.Equal(t, []*forge_types.HookDelivery{
		{ID: "2/12", GUID: "g3", Event: "push", DeliveredAt: since.Add(time.Minute), StatusCode: 0},
		{ID: "2/11", GUID: "g2", Event: "push", DeliveredAt: since.Add(30 * time.Second), StatusCode: http.StatusAccepted},
		{ID: "2/10", GUID: "g1", Event: "push", DeliveredAt: since, StatusCode: http.StatusBadGateway},
	}, got)
	assert.False(t, got[0].Reached())
	assert.True(t, got[1].Reached())
}

func TestHookDeliveriesWithoutHook(t *testing.T) {
	ctx := mockedContext(
		t, t.Context(),
		github_mock.WithRequestMatch(github_mock.GetReposHooksByOwnerByRepo, []*github.Hook{
			{ID: new(int64(1)), Config: &github.HookConfig{URL: new("https://other.example.com/hook")}},
		}),
	)
	c := &client{API: defaultAPI, url: defaultURL}
	got, err := c.HookDeliveries(ctx, &model.User{AccessToken: "token"}, &model.Repo{ForgeRemoteID: "hookless", Owner: "octocat", Name: "hello-world"}, "https://ci.example.com/api/hook", time.Now())
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestRedeliverHook(t *testing.T) {
	var redelivered []string
	ctx := mockedContext(
		t, t.Context(),
		github_mock.WithRequestMatchHandler(github_mock.PostReposHooksDeliveriesAttemptsByOwnerByRepoByHookIdByDeliveryId, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			redelivered = append(redelivered, r.URL.Path)
			w.WriteHeader(http.StatusAccepted)
		})),
	)
	c := &client{API: defaultAPI, url: defaultURL}
	u, r := &model.User{AccessToken: "token"}, &model.Repo{Owner: "octocat", Name: "hello-world"}
	require.NoError(t, c.RedeliverHook(ctx, u, r, &forge_types.HookDelivery{ID: "2/12"}))
	assert.Equal(t, []string{"/repos/octocat/hello-world/hooks/2/deliveries/12/attempts"}, redelivered)

	for _, id := range []string{"12", "x/12", "2/x", ""} {
		assert.Error(t, c.RedeliverHook(ctx, u, r, &forge_types.HookDelivery{ID: id}), id)
	}
}

// The webhook's ID is found once, on any page of the repo's hooks, and
// found again once the webhook is replaced.
func TestHookDeliveriesFindsTheHookOnce(t *testing.T) {
	listed, delivered := 0, 0
	hookID := int64(7)
	ctx := mockedContext(
		t, t.Context(),
		github_mock.WithRequestMatchHandler(github_mock.GetReposHooksByOwnerByRepo, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			listed++
			if r.URL.Query().Get("page") == "" {
				w.Header().Set("Link", `<https://api.github.com/repos/octocat/hello-world/hooks?page=2>; rel="next"`)
				writeJSON(t, w, []*github.Hook{{ID: new(int64(1)), Config: &github.HookConfig{URL: new("https://other.example.com/hook")}}})
				return
			}
			writeJSON(t, w, []*github.Hook{{ID: new(hookID), Config: &github.HookConfig{URL: new("https://ci.example.com/api/hook?access_token=x")}}})
		})),
		github_mock.WithRequestMatchHandler(github_mock.GetReposHooksDeliveriesByOwnerByRepoByHookId, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			delivered++
			if r.URL.Path != fmt.Sprintf("/repos/octocat/hello-world/hooks/%d/deliveries", hookID) {
				w.WriteHeader(http.StatusNotFound)
				writeJSON(t, w, map[string]string{"message": "Not Found"})
				return
			}
			writeJSON(t, w, []*github.HookDelivery{})
		})),
	)
	c := &client{API: defaultAPI, url: defaultURL}
	u, r := &model.User{AccessToken: "token"}, &model.Repo{ForgeRemoteID: "cached", Owner: "octocat", Name: "hello-world"}
	for range 2 {
		_, err := c.HookDeliveries(ctx, u, r, "https://ci.example.com/api/hook", time.Now())
		require.NoError(t, err)
	}
	assert.Equal(t, 2, listed, "two pages, once")
	assert.Equal(t, 2, delivered)

	hookID = 8 // replaced
	_, err := c.HookDeliveries(ctx, u, r, "https://ci.example.com/api/hook", time.Now())
	require.NoError(t, err)
	assert.Equal(t, 4, listed, "found again")
	assert.Equal(t, 4, delivered, "the stale ID, then the new one")
}

func TestHookDeliveriesRateLimited(t *testing.T) {
	ctx := mockedContext(
		t, t.Context(),
		github_mock.WithRequestMatchHandler(github_mock.GetReposHooksByOwnerByRepo, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-RateLimit-Limit", "5000")
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", fmt.Sprint(time.Now().Add(time.Hour).Unix()))
			w.WriteHeader(http.StatusForbidden)
			writeJSON(t, w, map[string]string{"message": "API rate limit exceeded"})
		})),
	)
	c := &client{API: defaultAPI, url: defaultURL}
	_, err := c.HookDeliveries(ctx, &model.User{AccessToken: "token"}, &model.Repo{ForgeRemoteID: "limited", Owner: "octocat", Name: "hello-world"}, "https://ci.example.com/api/hook", time.Now())
	assert.ErrorIs(t, err, forge_types.ErrRateLimited)
}
