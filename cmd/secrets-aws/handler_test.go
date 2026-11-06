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

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/services/secret"
	"go.woodpecker-ci.org/woodpecker/v3/server/services/utils"
)

// lister is the part of the server's secret service the extension backs.
type lister interface {
	SecretListPipeline(context.Context, *model.Repo, *model.Pipeline, *model.Netrc) ([]*model.Secret, error)
}

type mapSource map[string]string

func (m mapSource) Get(_ context.Context, id string) (string, bool, error) {
	v, ok := m[id]
	return v, ok, nil
}

type countingSource struct {
	mapSource
	calls int
}

func (c *countingSource) Get(ctx context.Context, id string) (string, bool, error) {
	c.calls++
	return c.mapSource.Get(ctx, id)
}

// setup starts the extension and returns the server's own secret client,
// signing with the matching private key, so the tests speak the real protocol.
func setup(t *testing.T, src source) lister {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	srv := httptest.NewServer(&handler{
		publicKey:     pub,
		source:        src,
		prefix:        "stratim/repos/",
		defaultEvents: []string{"push", "manual"},
	})
	t.Cleanup(srv.Close)

	client, err := utils.NewHTTPClient(priv, "loopback")
	require.NoError(t, err)
	return secret.NewHTTP(srv.URL, client, false)
}

func list(t *testing.T, svc lister, repo string) []*model.Secret {
	t.Helper()
	secrets, err := svc.SecretListPipeline(t.Context(),
		&model.Repo{FullName: repo}, &model.Pipeline{Event: model.EventManual}, nil)
	require.NoError(t, err)
	return secrets
}

func TestServesRepoSecrets(t *testing.T) {
	svc := setup(t, mapSource{
		"stratim/repos/acme/engine": `{
			"aws_secret_access_key": "s3cr3t",
			"deploy_key": {"value": "k", "events": ["tag"], "images": ["alpine"]}
		}`,
	})

	secrets := list(t, svc, "acme/engine")
	require.Len(t, secrets, 2)

	assert.Equal(t, "aws_secret_access_key", secrets[0].Name)
	assert.Equal(t, "s3cr3t", secrets[0].Value)
	assert.Equal(t, []model.WebhookEvent{"push", "manual"}, secrets[0].Events, "a bare value gets the default events")

	assert.Equal(t, "deploy_key", secrets[1].Name)
	assert.Equal(t, []model.WebhookEvent{"tag"}, secrets[1].Events)
	assert.Equal(t, []string{"alpine"}, secrets[1].Images)
}

func TestPinnedToForgeRemoteID(t *testing.T) {
	svc := setup(t, mapSource{
		"stratim/repos/acme/engine": `{"_forge_remote_id": "42", "a": "b"}`,
	})

	secrets, err := svc.SecretListPipeline(t.Context(),
		&model.Repo{FullName: "acme/engine", ForgeRemoteID: "42"}, &model.Pipeline{}, nil)
	require.NoError(t, err)
	require.Len(t, secrets, 1, "the pin itself is not a secret")
	assert.Equal(t, "a", secrets[0].Name)

	secrets, err = svc.SecretListPipeline(t.Context(),
		&model.Repo{FullName: "acme/engine", ForgeRemoteID: "43"}, &model.Pipeline{}, nil)
	require.NoError(t, err)
	assert.Empty(t, secrets, "a re-created acme/engine must not get the secrets")
}

func TestNoSecretsForUnknownRepo(t *testing.T) {
	svc := setup(t, make(mapSource))
	assert.Empty(t, list(t, svc, "acme/other"))
}

func TestRejectsUnsignedRequest(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	srv := httptest.NewServer(&handler{publicKey: pub, source: mapSource{
		"stratim/repos/acme/engine": `{"a": "b"}`,
	}, prefix: "stratim/repos/"})
	defer srv.Close()

	resp, err := http.Post(srv.URL, "application/json", bytes.NewBufferString(`{"repo":{"full_name":"acme/engine"}}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestRejectsAnotherServersKey(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	srv := httptest.NewServer(&handler{publicKey: pub, source: mapSource{
		"stratim/repos/acme/engine": `{"a": "b"}`,
	}, prefix: "stratim/repos/"})
	defer srv.Close()

	client, err := utils.NewHTTPClient(otherPriv, "loopback")
	require.NoError(t, err)
	_, err = secret.NewHTTP(srv.URL, client, false).SecretListPipeline(t.Context(),
		&model.Repo{FullName: "acme/engine"}, &model.Pipeline{}, nil)
	assert.Error(t, err)
}

func TestRejectsInvalidEvent(t *testing.T) {
	svc := setup(t, mapSource{"stratim/repos/acme/engine": `{"a": {"value": "b", "events": ["nonsense"]}}`})
	_, err := svc.SecretListPipeline(t.Context(),
		&model.Repo{FullName: "acme/engine"}, &model.Pipeline{}, nil)
	assert.Error(t, err)
}

func TestCachedSource(t *testing.T) {
	inner := &countingSource{mapSource: mapSource{"x": "1"}}
	now := time.Unix(0, 0)
	c := newCachedSource(inner, time.Minute)
	c.now = func() time.Time { return now }

	for range 3 {
		v, ok, err := c.Get(t.Context(), "x")
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "1", v)
	}
	assert.Equal(t, 1, inner.calls)

	now = now.Add(2 * time.Minute)
	_, _, err := c.Get(t.Context(), "x")
	require.NoError(t, err)
	assert.Equal(t, 2, inner.calls)
}

func TestParsePublicKey(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(pub)
	require.NoError(t, err)

	got, err := parsePublicKey(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
	require.NoError(t, err)
	assert.Equal(t, pub, got)

	_, err = parsePublicKey([]byte("not a key"))
	assert.Error(t, err)
}

func TestParsePublicKeyRaw(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	for name, data := range map[string][]byte{
		"hex":    []byte(hex.EncodeToString(pub) + "\n"),
		"base64": []byte(base64.StdEncoding.EncodeToString(pub) + "\n"),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := parsePublicKey(data)
			require.NoError(t, err)
			assert.Equal(t, pub, got)
		})
	}

	_, err = parsePublicKey([]byte(hex.EncodeToString(pub[:16])))
	assert.Error(t, err)
}

func TestParsePublicKeyRejects(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	rsaDER, err := x509.MarshalPKIXPublicKey(&rsaKey.PublicKey)
	require.NoError(t, err)
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	require.NoError(t, err)

	tests := map[string][]byte{
		"base64 of 31 bytes":         []byte(base64.StdEncoding.EncodeToString(pub[:31])),
		"hex of 33 bytes":            []byte(hex.EncodeToString(append(slices.Clone(pub), 0))),
		"RSA public key":             pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: rsaDER}),
		"ed25519 private key PKCS#8": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}),
		"empty":                      {},
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := parsePublicKey(data)
			assert.Error(t, err)
		})
	}
}

// TestPinnedPublicKey pins the key with STRATIM_SECRETS_PUBLIC_KEY_FILE: the
// server is never asked for its key, requests signed with the pinned key's
// private half are served and requests signed with another key are rejected.
func TestPinnedPublicKey(t *testing.T) {
	pinnedPub, pinnedPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	otherPub, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	// a server that would hand out another key
	var asked atomic.Int32
	keyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		asked.Add(1)
		der, _ := x509.MarshalPKIXPublicKey(otherPub)
		_, _ = w.Write(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
	}))
	defer keyServer.Close()

	der, err := x509.MarshalPKIXPublicKey(pinnedPub)
	require.NoError(t, err)
	keyFile := filepath.Join(t.TempDir(), "public.pem")
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), 0o600))
	t.Setenv("STRATIM_SECRETS_PUBLIC_KEY_FILE", keyFile)

	pub, err := loadPublicKey(t.Context(), keyServer.URL)
	require.NoError(t, err)
	assert.Equal(t, pinnedPub, pub)
	assert.Zero(t, asked.Load(), "the server must not be asked for its key")

	srv := httptest.NewServer(&handler{publicKey: pub, source: mapSource{
		"stratim/repos/acme/engine": `{"a": "b"}`,
	}, prefix: "stratim/repos/", defaultEvents: []string{"manual"}})
	defer srv.Close()

	listWith := func(priv ed25519.PrivateKey) ([]*model.Secret, error) {
		client, err := utils.NewHTTPClient(priv, "loopback")
		require.NoError(t, err)
		return secret.NewHTTP(srv.URL, client, false).SecretListPipeline(t.Context(),
			&model.Repo{FullName: "acme/engine"}, &model.Pipeline{Event: model.EventManual}, nil)
	}

	secrets, err := listWith(pinnedPriv)
	require.NoError(t, err)
	require.Len(t, secrets, 1)
	assert.Equal(t, "b", secrets[0].Value)

	_, err = listWith(otherPriv)
	assert.Error(t, err)
}

func TestPinnedPublicKeyUnreadable(t *testing.T) {
	t.Setenv("STRATIM_SECRETS_PUBLIC_KEY_FILE", filepath.Join(t.TempDir(), "missing.pem"))
	_, err := loadPublicKey(t.Context(), "http://127.0.0.1:1")
	assert.Error(t, err)
}
