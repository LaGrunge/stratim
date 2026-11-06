// Copyright 2026 Woodpecker Authors
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

package main

import (
	"encoding/base32"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func TestCheckSqliteFileExist(t *testing.T) {
	t.Parallel()

	t.Run("missing file is tolerated", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "does-not-exist.sqlite")
		assert.NoError(t, checkSqliteFileExist(path))
	})

	t.Run("existing file is ok", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "woodpecker.sqlite")
		require.NoError(t, os.WriteFile(path, []byte("data"), 0o600))
		assert.NoError(t, checkSqliteFileExist(path))
	})

	t.Run("stat error other than not-exist is returned", func(t *testing.T) {
		t.Parallel()
		// a path whose parent is a file, not a dir, yields ENOTDIR (not IsNotExist)
		parent := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(parent, []byte("x"), 0o600))
		path := filepath.Join(parent, "woodpecker.sqlite")
		assert.Error(t, checkSqliteFileExist(path))
	})
}

func TestSetupJWTSecret(t *testing.T) {
	t.Parallel()

	t.Run("generates and persists when none exists", func(t *testing.T) {
		t.Parallel()
		s := store_mocks.NewMockStore(t)
		s.On("ServerConfigGet", jwtSecretID).Return("", types.ErrRecordNotExist)

		var stored string
		s.On("ServerConfigSet", jwtSecretID, mock.AnythingOfType("string")).
			Run(func(args mock.Arguments) { stored = args.String(1) }).
			Return(nil)

		secret, err := setupJWTSecret(s)
		require.NoError(t, err)
		assert.NotEmpty(t, secret)
		assert.Equal(t, stored, secret)
	})

	t.Run("returns existing secret", func(t *testing.T) {
		t.Parallel()
		s := store_mocks.NewMockStore(t)
		s.On("ServerConfigGet", jwtSecretID).Return("existing-secret", nil)

		secret, err := setupJWTSecret(s)
		require.NoError(t, err)
		assert.Equal(t, "existing-secret", secret)
	})

	t.Run("propagates persist error after generation", func(t *testing.T) {
		t.Parallel()
		writeErr := errors.New("write failed")
		s := store_mocks.NewMockStore(t)
		s.On("ServerConfigGet", jwtSecretID).Return("", types.ErrRecordNotExist)
		s.On("ServerConfigSet", jwtSecretID, mock.AnythingOfType("string")).Return(writeErr)

		secret, err := setupJWTSecret(s)
		assert.ErrorIs(t, err, writeErr)
		assert.Empty(t, secret)
	})

	t.Run("propagates read error", func(t *testing.T) {
		t.Parallel()
		readErr := errors.New("db down")
		s := store_mocks.NewMockStore(t)
		s.On("ServerConfigGet", jwtSecretID).Return("", readErr)

		secret, err := setupJWTSecret(s)
		assert.ErrorIs(t, err, readErr)
		assert.Empty(t, secret)
	})
}

func TestSetupGrpcSecret(t *testing.T) {
	t.Parallel()

	t.Run("returns configured secret", func(t *testing.T) {
		t.Parallel()

		secret, generated := setupGrpcSecret("configured-secret")

		assert.Equal(t, "configured-secret", secret)
		assert.False(t, generated)
	})

	t.Run("generates random secret when not configured", func(t *testing.T) {
		t.Parallel()

		secretA, generatedA := setupGrpcSecret("")
		secretB, generatedB := setupGrpcSecret("")

		assert.True(t, generatedA)
		assert.True(t, generatedB)
		assert.NotEmpty(t, secretA)
		assert.NotEqual(t, secretA, secretB)

		decoded, err := base32.StdEncoding.DecodeString(secretA)
		require.NoError(t, err)
		assert.Len(t, decoded, 32)
	})
}

func TestJsonnetExtensionsProblem(t *testing.T) {
	assert.Empty(t, jsonnetExtensionsProblem([]string{".yaml", ".yml", ".jsonnet", ".libsonnet"}))
	assert.Empty(t, jsonnetExtensionsProblem([]string{".yaml", ".yml"}), "no jsonnet at all is a choice, not a problem")
	assert.Empty(t, jsonnetExtensionsProblem(nil))
	assert.Contains(t, jsonnetExtensionsProblem([]string{".yaml", ".jsonnet"}), "cannot import their libraries")
	assert.Contains(t, jsonnetExtensionsProblem([]string{".yaml", ".libsonnet"}), "no config is read")
}

func TestParseTrustedProxies(t *testing.T) {
	proxies, err := parseTrustedProxies([]string{" 10.0.0.0/8 ", "127.0.0.1", "", "  ", "::1"})
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.0/8", "127.0.0.1", "::1"}, proxies)

	proxies, err = parseTrustedProxies(nil)
	require.NoError(t, err)
	assert.Empty(t, proxies)

	for _, invalid := range []string{"10.0.0.0/33", "proxy.example.com", "10.0.0"} {
		_, err := parseTrustedProxies([]string{"127.0.0.1", invalid})
		assert.Error(t, err, invalid)
	}
}
