// Copyright 2024 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build test

package api_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/api"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
	"go.woodpecker-ci.org/woodpecker/v3/server/webhook"
	"go.woodpecker-ci.org/woodpecker/v3/shared/token"
)

const hookSecret = "secret-123-this-is-a-secret"

// postHook posts a webhook of the repo with the delivery ID to PostHook,
// signed with secret; it returns the status answered.
func postHook(t *testing.T, s store.Store, inbox *webhook.Inbox, repo *model.Repo, secret, delivery string, body []byte) int {
	t.Helper()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set("store", s)
	if inbox != nil {
		c.Set(webhook.InboxKey, inbox)
	}

	repoToken := token.New(token.HookToken)
	repoToken.Set("repo-id", fmt.Sprintf("%d", repo.ID))
	signed, err := repoToken.Sign(secret)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/api/hook", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+signed)
	req.Header.Set("X-GitHub-Delivery", delivery)
	req.Header.Set("X-GitHub-Event", "push")
	req.Header.Set("Content-Type", "application/json")
	// as the session's middleware does, looking for a token
	require.NoError(t, req.ParseForm())
	c.Request = req

	api.PostHook(c)
	return c.Writer.Status()
}

func TestHookKeepsDeliveryOnce(t *testing.T) {
	s := datastore.NewTestStore(t)
	repo := &model.Repo{
		ForgeID: 1, ForgeRemoteID: "123", Owner: "owner", Name: "name", FullName: "owner/name",
		IsActive: true, UserID: 1, WebhookSecret: hookSecret,
	}
	require.NoError(t, s.CreateRepo(repo))
	inbox := webhook.New(s, pipeline.WorkflowEnd{}, webhook.Options{})

	assert.Equal(t, http.StatusAccepted, postHook(t, s, inbox, repo, hookSecret, "d-1", []byte(`{"ref":"refs/heads/main"}`)))
	assert.Equal(t, http.StatusOK, postHook(t, s, inbox, repo, hookSecret, "d-1", []byte(`{"ref":"refs/heads/main"}`)), "delivered again")
	assert.Equal(t, http.StatusAccepted, postHook(t, s, inbox, repo, hookSecret, "d-2", []byte(`{}`)))

	known, err := s.WebhookDeliveriesKnown(repo.ForgeID, repo.ID, []string{"d-1", "d-2"})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"d-1", "d-2"}, known)
	due, err := s.WebhookDeliveriesDue(1<<40, 10)
	require.NoError(t, err)
	require.Len(t, due, 1, "the repo's first is due, the next after it")
	kept, err := s.WebhookDeliveryGet(due[0].ID)
	require.NoError(t, err)
	assert.Equal(t, "push", kept.Event)
	assert.Equal(t, repo.ID, kept.RepoID)
	assert.Empty(t, kept.Header["Authorization"], "credentials are not kept")
	assert.Equal(t, []string{"d-1"}, kept.Header["X-Github-Delivery"])
	zr, err := gzip.NewReader(bytes.NewReader(kept.Body))
	require.NoError(t, err)
	body, err := io.ReadAll(zr)
	require.NoError(t, err)
	assert.JSONEq(t, `{"ref":"refs/heads/main"}`, string(body), "a JSON body read once the form was")
}

// GitHub's webhooks are forms, their token in the URL: the body is kept
// whole although the token is looked for in the form, before the handler
// (by the session's middleware) or in it.
func TestHookKeepsAFormWhole(t *testing.T) {
	for _, parsedBefore := range []bool{false, true} {
		t.Run(fmt.Sprintf("parsed before %v", parsedBefore), func(t *testing.T) {
			testHookKeepsAFormWhole(t, parsedBefore)
		})
	}
}

func testHookKeepsAFormWhole(t *testing.T, parsedBefore bool) {
	s := datastore.NewTestStore(t)
	repo := &model.Repo{
		ForgeID: 1, ForgeRemoteID: "123", Owner: "owner", Name: "name", FullName: "owner/name",
		IsActive: true, UserID: 1, WebhookSecret: hookSecret,
	}
	require.NoError(t, s.CreateRepo(repo))
	inbox := webhook.New(s, pipeline.WorkflowEnd{}, webhook.Options{})

	repoToken := token.New(token.HookToken)
	repoToken.Set("repo-id", fmt.Sprintf("%d", repo.ID))
	signed, err := repoToken.Sign(hookSecret)
	require.NoError(t, err)
	form := url.Values{"payload": {`{"ref":"refs/heads/main"}`}}.Encode()
	req := httptest.NewRequest(http.MethodPost, "/api/hook?access_token="+signed, strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-GitHub-Delivery", "d-form")
	if parsedBefore {
		require.NoError(t, req.ParseForm())
	}
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set("store", s)
	c.Set(webhook.InboxKey, inbox)
	c.Request = req
	api.PostHook(c)
	require.Equal(t, http.StatusAccepted, c.Writer.Status())

	due, err := s.WebhookDeliveriesDue(1<<40, 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	kept, err := s.WebhookDeliveryGet(due[0].ID)
	require.NoError(t, err)
	zr, err := gzip.NewReader(bytes.NewReader(kept.Body))
	require.NoError(t, err)
	body, err := io.ReadAll(zr)
	require.NoError(t, err)
	assert.Equal(t, form, string(body))
}

func TestHookRefused(t *testing.T) {
	s := datastore.NewTestStore(t)
	inactive := &model.Repo{
		ForgeID: 1, ForgeRemoteID: "1", Owner: "owner", Name: "off", FullName: "owner/off",
		UserID: 1, WebhookSecret: hookSecret,
	}
	active := &model.Repo{
		ForgeID: 1, ForgeRemoteID: "2", Owner: "owner", Name: "on", FullName: "owner/on",
		IsActive: true, UserID: 1, WebhookSecret: hookSecret,
	}
	require.NoError(t, s.CreateRepo(inactive))
	require.NoError(t, s.CreateRepo(active))
	inbox := webhook.New(s, pipeline.WorkflowEnd{}, webhook.Options{})

	assert.Equal(t, http.StatusNoContent, postHook(t, s, inbox, inactive, hookSecret, "d-1", []byte(`{}`)), "inactive repo")
	assert.Equal(t, http.StatusBadRequest, postHook(t, s, inbox, active, "another-secret-of-thirty-two-bytes", "d-2", []byte(`{}`)), "bad token")
	assert.Equal(t, http.StatusRequestEntityTooLarge, postHook(t, s, inbox, active, hookSecret, "d-3", make([]byte, webhook.MaxBodyBytes+1)))

	due, err := s.WebhookDeliveriesDue(1<<40, 10)
	require.NoError(t, err)
	assert.Empty(t, due, "nothing kept")
}

func TestHookWithoutInbox(t *testing.T) {
	s := datastore.NewTestStore(t)
	repo := &model.Repo{
		ForgeID: 1, ForgeRemoteID: "1", Owner: "owner", Name: "name", FullName: "owner/name",
		IsActive: true, UserID: 1, WebhookSecret: hookSecret,
	}
	require.NoError(t, s.CreateRepo(repo))
	assert.Equal(t, http.StatusInternalServerError, postHook(t, s, nil, repo, hookSecret, "d-1", []byte(`{}`)))
}

func TestHookTakesTheLargestBody(t *testing.T) {
	s := datastore.NewTestStore(t)
	repo := &model.Repo{
		ForgeID: 1, ForgeRemoteID: "1", Owner: "owner", Name: "name", FullName: "owner/name",
		IsActive: true, UserID: 1, WebhookSecret: hookSecret,
	}
	require.NoError(t, s.CreateRepo(repo))
	inbox := webhook.New(s, pipeline.WorkflowEnd{}, webhook.Options{})
	assert.Equal(t, http.StatusAccepted, postHook(t, s, inbox, repo, hookSecret, "d-max", make([]byte, webhook.MaxBodyBytes)))
	assert.Equal(t, http.StatusRequestEntityTooLarge, postHook(t, s, inbox, repo, hookSecret, "d-over", make([]byte, webhook.MaxBodyBytes+1)))
}
