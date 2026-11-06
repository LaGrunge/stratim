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

package bottoken

import (
	"bytes"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
)

func TestFromRequest(t *testing.T) {
	for _, tt := range []struct {
		name   string
		target string
		header http.Header
		raw    string
		found  bool
		reason string
	}{
		{name: "none", target: "/"},
		{name: "the header", target: "/", header: http.Header{Header: {"stb_a"}}, raw: "stb_a", found: true},
		{name: "the header, not a bot token", target: "/", header: http.Header{Header: {"x"}}, found: true, reason: ReasonMalformed},
		{name: "the header, empty", target: "/", header: http.Header{Header: {""}}, found: true, reason: ReasonMalformed},
		{name: "the header, twice", target: "/", header: http.Header{Header: {"stb_a", "stb_b"}}, found: true, reason: ReasonMalformed},
		{name: "the header, empty, with a Bearer bot token", target: "/", header: http.Header{Header: {""}, "Authorization": {"Bearer stb_b"}}, found: true, reason: ReasonMalformed},
		{name: "a Bearer bot token", target: "/", header: http.Header{"Authorization": {"Bearer stb_b"}}, raw: "stb_b", found: true},
		{name: "a bearer bot token, the scheme in lower case", target: "/", header: http.Header{"Authorization": {"bearer stb_b"}}, raw: "stb_b", found: true},
		{name: "a BEARER bot token", target: "/", header: http.Header{"Authorization": {"BEARER stb_b"}}, raw: "stb_b", found: true},
		{name: "a Basic authorization", target: "/", header: http.Header{"Authorization": {"Basic c3RiX2E6"}}},
		{name: "a Bearer token of a user", target: "/", header: http.Header{"Authorization": {"Bearer eyJ"}}},
		{name: "the header first", target: "/", header: http.Header{Header: {"stb_a"}, "Authorization": {"Bearer stb_b"}}, raw: "stb_a", found: true},
		{name: "in the query", target: "/?access_token=stb_c", found: true, reason: ReasonInQuery},
		{name: "a user token in the query", target: "/?access_token=eyJ"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.target, nil)
			maps.Copy(req.Header, tt.header)
			raw, found, err := FromRequest(req)
			assert.Equal(t, tt.raw, raw)
			assert.Equal(t, tt.found, found)
			if tt.reason == "" {
				assert.NoError(t, err)
				return
			}
			refused, ok := errors.AsType[*RefusedError](err)
			require.True(t, ok)
			assert.Equal(t, tt.reason, refused.Reason)
		})
	}
}

func TestAuthenticateAndRecheck(t *testing.T) {
	s := datastore.NewTestStore(t)
	owner := &model.User{Login: "owner", ForgeRemoteID: "1", Hash: "h"}
	require.NoError(t, s.CreateUser(owner))
	secret, hash, shown := model.NewBotTokenSecret()
	now := time.Now()
	token := &model.BotToken{OwnerID: owner.ID, Name: "bot", Hash: hash, Prefix: shown, Created: now.Unix(), Expires: now.Add(time.Hour).Unix()}
	require.NoError(t, s.BotTokenCreate(token, 20, now.Unix()))

	got, user, err := Authenticate(s, secret, now)
	require.NoError(t, err)
	assert.Equal(t, token.ID, got.ID)
	assert.Equal(t, owner.ID, user.ID)
	user, err = Recheck(s, token, now)
	require.NoError(t, err)
	assert.Equal(t, owner.ID, user.ID)

	_, _, err = Authenticate(s, secret, now.Add(time.Hour))
	refused, ok := errors.AsType[*RefusedError](err)
	require.True(t, ok)
	assert.Equal(t, ReasonExpired, refused.Reason)
	assert.Equal(t, token.ID, refused.Token.ID)

	// an owner made an admin: an admin's token has to name its repos
	owner.Admin = true
	require.NoError(t, s.UpdateUser(owner))
	_, _, err = Authenticate(s, secret, now)
	refused, ok = errors.AsType[*RefusedError](err)
	require.True(t, ok)
	assert.Equal(t, ReasonAdminAllRepos, refused.Reason)
	_, err = Recheck(s, token, now)
	refused, ok = errors.AsType[*RefusedError](err)
	require.True(t, ok)
	assert.Equal(t, ReasonAdminAllRepos, refused.Reason)

	// one that names them acts as its owner, never an admin
	_, restrictedHash, restrictedShown := model.NewBotTokenSecret()
	restricted := &model.BotToken{OwnerID: owner.ID, Name: "repos", Hash: restrictedHash, Prefix: restrictedShown, RepoIDs: []int64{3}, Created: now.Unix(), Expires: now.Add(time.Hour).Unix()}
	require.NoError(t, s.BotTokenCreate(restricted, 20, now.Unix()))
	user, err = Recheck(s, restricted, now)
	require.NoError(t, err)
	assert.False(t, user.Admin, "a bot token is never an admin")
	stored, err := s.GetUser(owner.ID)
	require.NoError(t, err)
	assert.True(t, stored.Admin, "the owner stays an admin")

	_, err = s.BotTokenRevoke(token.ID, now.Unix())
	require.NoError(t, err)
	_, err = Recheck(s, token, now)
	refused, ok = errors.AsType[*RefusedError](err)
	require.True(t, ok)
	assert.Equal(t, ReasonRevoked, refused.Reason)
}

func TestRecordAndPurge(t *testing.T) {
	s := datastore.NewTestStore(t)
	now := time.Now()
	Record(s, &model.BotTokenCall{
		TokenID: 1, Created: now.Add(-48 * time.Hour).Unix(),
		Route: strings.Repeat("r", 300), Reason: strings.Repeat("é", 200),
	})
	Record(s, &model.BotTokenCall{TokenID: 1, Created: now.Unix()})

	calls, err := s.BotTokenCallList(1, 10)
	require.NoError(t, err)
	require.Len(t, calls, 2)
	old := calls[1]
	assert.Len(t, old.Route, 255)
	assert.LessOrEqual(t, len(old.Reason), 255)
	assert.True(t, strings.HasPrefix(strings.Repeat("é", 200), old.Reason), "cut on a character")

	// a call with a token that is not known has no row
	RecordUnknown(&model.BotTokenCall{Created: now.Unix(), Reason: ReasonUnknown})
	calls, err = s.BotTokenCallList(0, 10)
	require.NoError(t, err)
	assert.Empty(t, calls)

	Purge(s, 24*time.Hour, now)
	calls, err = s.BotTokenCallList(1, 10)
	require.NoError(t, err)
	assert.Len(t, calls, 1)
}

// The calls with a token not known are logged by the logger the server is
// configured with, a burst a minute.
func TestRecordUnknownLogs(t *testing.T) {
	var buf bytes.Buffer
	logger, sampler := log.Logger, unknownSampler
	t.Cleanup(func() { log.Logger, unknownSampler = logger, sampler })
	log.Logger = zerolog.New(&buf)
	unknownSampler = &zerolog.BurstSampler{Burst: unknownBurst, Period: time.Minute}

	for range unknownBurst + 5 {
		RecordUnknown(&model.BotTokenCall{Route: "/api/v2/whoami", Status: 401, Reason: ReasonUnknown})
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Len(t, lines, unknownBurst)
	assert.Contains(t, lines[0], `"reason":"unknown token"`)
}
