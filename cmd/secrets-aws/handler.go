// Copyright 2026 Stratim Authors
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

package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/rs/zerolog/log"
	"github.com/yaronf/httpsign"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// signatureName is the name the server signs extension requests under
// (server/services/utils/http.go).
const signatureName = "woodpecker-ci-extensions"

// forgeRemoteIDKey is the reserved key that pins a secret document to one
// repository by its forge id. The document is looked up by owner/name, which
// a rename or a deleted and re-created repository can hand to someone else;
// with the pin, such a repository gets nothing.
const forgeRemoteIDKey = "_forge_remote_id"

// source returns the raw value of a secret, and false when it does not exist.
type source interface {
	Get(ctx context.Context, id string) (string, bool, error)
}

type handler struct {
	publicKey     ed25519.PublicKey
	source        source
	prefix        string
	defaultEvents []string
}

type request struct {
	Repo *model.Repo `json:"repo"`
}

type response struct {
	Secrets []*model.Secret `json:"secrets"`
}

// entry is one secret in the JSON document: a bare string or an object.
type entry struct {
	Value  string   `json:"value"`
	Events []string `json:"events"`
	Images []string `json:"images"`
}

func (e *entry) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		e.Value = s
		return nil
	}
	type plain entry
	return json.Unmarshal(data, (*plain)(e))
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := h.verify(r); err != nil {
		log.Warn().Err(err).Str("remote", r.RemoteAddr).Msg("rejected an unsigned or badly signed request")
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Repo == nil || req.Repo.FullName == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	secrets, err := h.secretsFor(r.Context(), req.Repo)
	if err != nil {
		log.Error().Err(err).Str("repo", req.Repo.FullName).Msg("cannot read secrets")
		http.Error(w, "cannot read secrets", http.StatusBadGateway)
		return
	}
	if len(secrets) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response{Secrets: secrets}); err != nil {
		log.Error().Err(err).Msg("cannot write response")
	}
}

// verify checks the request signature and that the signed Content-Digest
// matches the body, so a captured signature cannot carry another body.
func (h *handler) verify(r *http.Request) error {
	verifier, err := httpsign.NewEd25519Verifier(h.publicKey, httpsign.NewVerifyConfig(),
		httpsign.Headers("@request-target", "content-digest"))
	if err != nil {
		return err
	}
	if err := httpsign.VerifyRequest(signatureName, *verifier, r); err != nil {
		return err
	}
	return httpsign.ValidateContentDigestHeader(r.Header.Values("Content-Digest"), &r.Body,
		[]string{httpsign.DigestSha256, httpsign.DigestSha512})
}

func (h *handler) secretsFor(ctx context.Context, r *model.Repo) ([]*model.Secret, error) {
	repo := r.FullName
	raw, ok, err := h.source.Get(ctx, h.prefix+repo)
	if err != nil || !ok {
		return nil, err
	}

	var doc map[string]entry
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return nil, fmt.Errorf("secret %s%s is not a JSON object of secrets: %w", h.prefix, repo, err)
	}

	if pin, pinned := doc[forgeRemoteIDKey]; pinned {
		if pin.Value != string(r.ForgeRemoteID) {
			log.Warn().Str("repo", repo).Str("forge_remote_id", string(r.ForgeRemoteID)).
				Msg("secret document is pinned to another repository, serving nothing")
			return nil, nil
		}
		delete(doc, forgeRemoteIDKey)
	}

	names := make([]string, 0, len(doc))
	for name := range doc {
		names = append(names, name)
	}
	sort.Strings(names)

	secrets := make([]*model.Secret, 0, len(names))
	for _, name := range names {
		e := doc[name]
		events := e.Events
		if len(events) == 0 {
			events = h.defaultEvents
		}
		s := &model.Secret{Name: name, Value: e.Value, Images: e.Images}
		for _, ev := range events {
			s.Events = append(s.Events, model.WebhookEvent(ev))
		}
		if err := s.Validate(); err != nil {
			return nil, fmt.Errorf("secret %q of %s: %w", name, repo, err)
		}
		secrets = append(secrets, s)
	}
	return secrets, nil
}

type secretsManagerSource struct {
	client *secretsmanager.Client
}

func (s *secretsManagerSource) Get(ctx context.Context, id string) (string, bool, error) {
	out, err := s.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &id})
	var notFound *smtypes.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if out.SecretString == nil {
		return "", false, fmt.Errorf("secret %s has no string value", id)
	}
	return *out.SecretString, true, nil
}

// cachedSource keeps values for ttl, so a busy queue does not call Secrets
// Manager for every pipeline; a rotated secret is picked up within ttl.
type cachedSource struct {
	next source
	ttl  time.Duration
	now  func() time.Time

	mu      sync.Mutex
	entries map[string]cached
}

type cached struct {
	value   string
	ok      bool
	expires time.Time
}

func newCachedSource(next source, ttl time.Duration) *cachedSource {
	return &cachedSource{next: next, ttl: ttl, now: time.Now, entries: map[string]cached{}}
}

func (c *cachedSource) Get(ctx context.Context, id string) (string, bool, error) {
	c.mu.Lock()
	e, hit := c.entries[id]
	c.mu.Unlock()
	if hit && c.now().Before(e.expires) {
		return e.value, e.ok, nil
	}

	value, ok, err := c.next.Get(ctx, id)
	if err != nil {
		return "", false, err
	}
	c.mu.Lock()
	c.entries[id] = cached{value: value, ok: ok, expires: c.now().Add(c.ttl)}
	c.mu.Unlock()
	return value, ok, nil
}
