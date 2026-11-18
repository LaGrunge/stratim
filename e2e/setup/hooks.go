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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/shared/token"
)

// TestWebhookSecret signs the webhook token of the fixture repo. Hard-coded
// for tests — not a real secret.
const TestWebhookSecret = "test-webhook-secret-for-integration-tests"

// HookPayload is the body of a webhook of the mock forge: what Hook parses
// into a pipeline of the fixture repo.
type HookPayload struct {
	Event   model.WebhookEvent `json:"event"`
	Branch  string             `json:"branch"`
	Commit  string             `json:"commit"`
	Message string             `json:"message"`
}

// hookForge is the mock forge keeping the deliveries of the webhook it
// sends, as GitHub does: the server's reconciliation reads them and has
// them delivered again.
type hookForge struct {
	*forge_mocks.MockForge
	*Deliveries
}

// Deliveries are the attempts of the mock forge to deliver webhooks.
type Deliveries struct {
	mu       sync.Mutex
	attempts []*forge_types.HookDelivery // oldest first
	requests map[string]*hookRequest     // by GUID
	url      func() string
}

type hookRequest struct {
	header http.Header
	body   []byte
}

// stubHook has the mock forge parse the webhooks of HookPayload into
// pipelines of repo: forms, as GitHub sends them.
func stubHook(m *forge_mocks.MockForge, repo *model.Repo) {
	m.EXPECT().Hook(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, r *http.Request) (*model.Repo, *model.Pipeline, error) {
		var p HookPayload
		if err := json.Unmarshal([]byte(r.FormValue("payload")), &p); err != nil {
			return nil, nil, err
		}
		forgeRepo := *repo
		return &forgeRepo, &model.Pipeline{
			Event:   p.Event,
			Branch:  p.Branch,
			Commit:  p.Commit,
			Ref:     "refs/heads/" + p.Branch,
			Message: p.Message,
			Author:  repo.Owner,
			Sender:  repo.Owner,
		}, nil
	}).Maybe()
}

// Deliver has the mock forge deliver a webhook of the fixture repo to the
// server's API, as the delivery guid, a form with its token in the URL as
// GitHub's: it returns the status the server
// answered, 0 if it did not, and how long it took. The attempt is kept for
// the server's reconciliation.
func (env *ServerEnv) Deliver(t *testing.T, guid string, payload HookPayload) (int, time.Duration) {
	t.Helper()
	env.APIURL(t)
	payloadJSON, err := json.Marshal(payload)
	require.NoError(t, err)
	body := []byte(url.Values{"payload": {string(payloadJSON)}}.Encode())
	header := http.Header{}
	header.Set("Content-Type", "application/x-www-form-urlencoded")
	header.Set("X-Gitea-Delivery", guid)
	header.Set("X-Gitea-Event", string(payload.Event))
	env.Hooks.mu.Lock()
	env.Hooks.requests[guid] = &hookRequest{header: header, body: body}
	env.Hooks.mu.Unlock()
	return env.Hooks.send(t.Context(), env.hookToken(t), guid)
}

// hookToken is the webhook token of the fixture repo, as the server puts
// it in the URL of the webhook it registers.
func (env *ServerEnv) hookToken(t *testing.T) string {
	t.Helper()
	tok := token.New(token.HookToken)
	tok.Set("forge-id", strconv.FormatInt(env.Fixtures.Repo.ForgeID, 10))
	tok.Set("repo-forge-remote-id", string(env.Fixtures.Repo.ForgeRemoteID))
	signed, err := tok.Sign(TestWebhookSecret)
	require.NoError(t, err)
	return signed
}

// send makes an attempt to deliver the webhook guid.
func (d *Deliveries) send(ctx context.Context, hookToken, guid string) (int, time.Duration) {
	d.mu.Lock()
	sent := d.requests[guid]
	url := d.url()
	d.mu.Unlock()

	status := 0
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/api/hook?access_token="+hookToken, bytes.NewReader(sent.body))
	if err == nil {
		req.Header = sent.header.Clone()
		var resp *http.Response
		if resp, err = http.DefaultClient.Do(req); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			status = resp.StatusCode
		}
	}
	took := time.Since(start)

	d.mu.Lock()
	defer d.mu.Unlock()
	d.attempts = append(d.attempts, &forge_types.HookDelivery{
		ID:          strconv.Itoa(len(d.attempts) + 1),
		GUID:        guid,
		Event:       sent.header.Get("X-Gitea-Event"),
		DeliveredAt: time.Now(),
		StatusCode:  status,
	})
	return status, took
}

// Attempts are the attempts to deliver guid so far, oldest first.
func (d *Deliveries) Attempts(guid string) []*forge_types.HookDelivery {
	d.mu.Lock()
	defer d.mu.Unlock()
	var of []*forge_types.HookDelivery
	for _, a := range d.attempts {
		if a.GUID == guid {
			c := *a
			of = append(of, &c)
		}
	}
	return of
}

// HookDeliveries lists the attempts since then, newest first.
func (d *Deliveries) HookDeliveries(_ context.Context, _ *model.User, _ *model.Repo, _ string, since time.Time) ([]*forge_types.HookDelivery, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var list []*forge_types.HookDelivery
	for _, a := range slices.Backward(d.attempts) {
		if !a.DeliveredAt.Before(since) {
			c := *a
			list = append(list, &c)
		}
	}
	return list, nil
}

// RedeliverHook delivers again the webhook of the attempt, the token of
// repo with it.
func (d *Deliveries) RedeliverHook(ctx context.Context, _ *model.User, r *model.Repo, attempt *forge_types.HookDelivery) error {
	tok := token.New(token.HookToken)
	tok.Set("forge-id", strconv.FormatInt(r.ForgeID, 10))
	tok.Set("repo-forge-remote-id", string(r.ForgeRemoteID))
	signed, err := tok.Sign(r.WebhookSecret)
	if err != nil {
		return err
	}
	d.mu.Lock()
	_, ok := d.requests[attempt.GUID]
	d.mu.Unlock()
	if !ok {
		return fmt.Errorf("no delivery %s", attempt.GUID)
	}
	d.send(ctx, signed, attempt.GUID)
	return nil
}
