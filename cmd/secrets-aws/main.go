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

// secrets-aws is a secret extension for the server that serves pipeline
// secrets from AWS Secrets Manager, so that they never have to be stored in
// the server's database.
//
// Point the server at it with WOODPECKER_SECRET_EXTENSION_ENDPOINT and allow
// its host in WOODPECKER_EXTENSIONS_ALLOWED_HOSTS. For a pipeline of repo
// owner/name it reads the Secrets Manager secret <prefix>owner/name, a JSON
// object from secret name to either a string or
//
//	{"value": "...", "events": ["manual", "push"], "images": ["..."]}
//
// A string value, or an object without events, is available to the events in
// STRATIM_SECRETS_DEFAULT_EVENTS, which leaves out pull requests: a pull
// request from a fork must not see the secrets of the repository. The
// reserved key "_forge_remote_id" pins the document to the repository with
// that forge id, so a renamed or re-created owner/name gets nothing.
//
// The server's request-signing key is read from STRATIM_SECRETS_PUBLIC_KEY_FILE
// or, without it, fetched from the server at start over plain HTTP: run the
// extension on the same private network as the server, never across hosts.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/rs/zerolog/log"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("secrets-aws failed")
	}
}

func run(ctx context.Context) error {
	addr := envOr("STRATIM_SECRETS_ADDR", ":8080")
	prefix := envOr("STRATIM_SECRETS_PREFIX", "stratim/repos/")
	defaultEvents := strings.Split(envOr("STRATIM_SECRETS_DEFAULT_EVENTS", "push,tag,deployment,cron,manual"), ",")

	pub, err := loadPublicKey(ctx, envOr("STRATIM_SERVER", "http://server:8000"))
	if err != nil {
		return err
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	h := &handler{
		publicKey:     pub,
		source:        newCachedSource(&secretsManagerSource{client: secretsmanager.NewFromConfig(cfg)}, time.Minute),
		prefix:        prefix,
		defaultEvents: defaultEvents,
	}

	log.Info().Str("addr", addr).Str("prefix", prefix).Msg("serving secrets from AWS Secrets Manager")
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 10 * time.Second}
	return srv.ListenAndServe()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// loadPublicKey reads the key the server signs extension requests with, from
// STRATIM_SECRETS_PUBLIC_KEY_FILE if set, otherwise from the server itself,
// retrying while it starts.
func loadPublicKey(ctx context.Context, server string) (ed25519.PublicKey, error) {
	if path := os.Getenv("STRATIM_SECRETS_PUBLIC_KEY_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		return parsePublicKey(data)
	}

	url := strings.TrimRight(server, "/") + "/api/signature/public-key"
	var lastErr error
	for range 60 {
		data, err := fetch(ctx, url)
		if err == nil {
			return parsePublicKey(data)
		}
		lastErr = err
		log.Warn().Err(err).Msg("waiting for the server's public key")
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	return nil, fmt.Errorf("fetch %s: %w", url, lastErr)
}

func fetch(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func parsePublicKey(data []byte) (ed25519.PublicKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("public key is not PEM")
	}
	key, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	pub, ok := key.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("public key is %T, not ed25519", key)
	}
	return pub, nil
}
