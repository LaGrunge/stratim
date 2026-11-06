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

package datastore

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

const botNow = int64(1_800_000_000)

// botOwner stores a user to own bot tokens.
func botOwner(t *testing.T, s *storage, login string) *model.User {
	t.Helper()
	user := &model.User{Login: login, ForgeRemoteID: model.ForgeRemoteID(login), Hash: "h-" + login}
	require.NoError(t, s.CreateUser(user))
	return user
}

func newBotToken(t *testing.T, s *storage, owner int64, name string) (*model.BotToken, string) {
	t.Helper()
	secret, hash, shown := model.NewBotTokenSecret()
	token := &model.BotToken{
		OwnerID: owner, Name: name, Hash: hash, Prefix: shown,
		Scopes: []model.BotScope{model.BotScopeRead}, RepoIDs: []int64{4, 9},
		Created: botNow, Expires: botNow + 3600,
	}
	require.NoError(t, s.BotTokenCreate(token, 100, botNow))
	require.NotZero(t, token.ID)
	return token, secret
}

func TestBotTokenCreateFind(t *testing.T) {
	s, closer := newTestStore(t, new(model.User), new(model.Org), new(model.BotToken))
	defer closer()
	owner := botOwner(t, s, "one")

	token, secret := newBotToken(t, s, owner.ID, "watcher")

	found, err := s.BotTokenFindByHash(model.HashBotToken(secret))
	require.NoError(t, err)
	assert.Equal(t, token, found)
	assert.Equal(t, []model.BotScope{model.BotScopeRead}, found.Scopes, "the scopes round-trip")
	assert.Equal(t, []int64{4, 9}, found.RepoIDs, "the repos round-trip")

	got, err := s.BotTokenGet(token.ID)
	require.NoError(t, err)
	assert.Equal(t, token, got)

	_, err = s.BotTokenFindByHash(model.HashBotToken(secret + "x"))
	assert.ErrorIs(t, err, types.ErrRecordNotExist)
	_, err = s.BotTokenFindByHash("")
	assert.Error(t, err)
	_, err = s.BotTokenGet(token.ID + 100)
	assert.ErrorIs(t, err, types.ErrRecordNotExist)

	// the hash is unique
	dup := &model.BotToken{OwnerID: owner.ID, Name: "dup", Hash: token.Hash, Expires: botNow + 1}
	assert.ErrorIs(t, s.BotTokenCreate(dup, 100, botNow), types.ErrInsertDuplicateDetected)
	// of a user that does not exist
	orphan := &model.BotToken{OwnerID: owner.ID + 100, Name: "orphan", Hash: "x", Expires: botNow + 1}
	assert.ErrorIs(t, s.BotTokenCreate(orphan, 100, botNow), types.ErrRecordNotExist)
}

func TestBotTokenCreateCapped(t *testing.T) {
	s, closer := newTestStore(t, new(model.User), new(model.Org), new(model.BotToken))
	defer closer()
	owner := botOwner(t, s, "capped")

	create := func(name string, expires int64) error {
		_, hash, shown := model.NewBotTokenSecret()
		return s.BotTokenCreate(&model.BotToken{OwnerID: owner.ID, Name: name, Hash: hash, Prefix: shown, Created: botNow, Expires: expires}, 2, botNow)
	}
	require.NoError(t, create("expired", botNow))
	require.NoError(t, create("one", botNow+10))
	revoked, _ := newBotToken(t, s, owner.ID, "revoked")
	_, err := s.BotTokenRevoke(revoked.ID, botNow)
	require.NoError(t, err)
	require.NoError(t, create("two", botNow+10), "expired and revoked tokens do not count")
	assert.ErrorIs(t, create("three", botNow+10), types.ErrBotTokenLimit)

	// creations at once do not pass the count together
	other := botOwner(t, s, "racing")
	var wg sync.WaitGroup
	errs := make([]error, 6)
	for i := range errs {
		wg.Go(func() {
			_, hash, shown := model.NewBotTokenSecret()
			errs[i] = s.BotTokenCreate(&model.BotToken{OwnerID: other.ID, Name: fmt.Sprint(i), Hash: hash, Prefix: shown, Created: botNow, Expires: botNow + 10}, 2, botNow)
		})
	}
	wg.Wait()
	created := 0
	for _, err := range errs {
		if err == nil {
			created++
		} else {
			assert.ErrorIs(t, err, types.ErrBotTokenLimit)
		}
	}
	assert.Equal(t, 2, created)
}

func TestBotTokenListRevokeTouch(t *testing.T) {
	s, closer := newTestStore(t, new(model.User), new(model.Org), new(model.BotToken))
	defer closer()
	one, two := botOwner(t, s, "one"), botOwner(t, s, "two")

	first, _ := newBotToken(t, s, one.ID, "first")
	second, _ := newBotToken(t, s, one.ID, "second")
	other, _ := newBotToken(t, s, two.ID, "other")

	mine, err := s.BotTokenList(one.ID, &model.ListOptionsWithAll{All: true})
	require.NoError(t, err)
	require.Len(t, mine, 2)
	assert.Equal(t, second.ID, mine[0].ID, "newest first")
	all, err := s.BotTokenList(0, &model.ListOptionsWithAll{All: true})
	require.NoError(t, err)
	assert.Len(t, all, 3)

	revoked, err := s.BotTokenRevoke(first.ID, botNow+5)
	require.NoError(t, err)
	assert.True(t, revoked)
	revoked, err = s.BotTokenRevoke(first.ID, botNow+6)
	require.NoError(t, err)
	assert.False(t, revoked, "revoked once")
	got, err := s.BotTokenGet(first.ID)
	require.NoError(t, err)
	assert.Equal(t, botNow+5, got.Revoked)

	require.NoError(t, s.BotTokenTouch(other.ID, botNow+10))
	require.NoError(t, s.BotTokenTouch(other.ID, botNow+5), "an earlier use changes nothing")
	got, err = s.BotTokenGet(other.ID)
	require.NoError(t, err)
	assert.Equal(t, botNow+10, got.LastUsed)
}

func TestBotTokenCalls(t *testing.T) {
	s, closer := newTestStore(t, new(model.BotTokenCall))
	defer closer()

	// appended out of the order they were made in
	for i, created := range []int64{botNow - 100, botNow, botNow - 10} {
		require.NoError(t, s.BotTokenCallAppend(&model.BotTokenCall{
			TokenID: 1, OwnerID: 3, Created: created, Method: "GET", Route: "/api/v2/events", Status: 200 + i,
		}))
	}
	require.NoError(t, s.BotTokenCallAppend(&model.BotTokenCall{TokenID: 2, Created: botNow, Status: 401, Reason: "revoked token"}))

	calls, err := s.BotTokenCallList(1, 2)
	require.NoError(t, err)
	require.Len(t, calls, 2)
	assert.Equal(t, botNow, calls[0].Created, "the newest first")
	assert.Equal(t, botNow-10, calls[1].Created)

	purged, err := s.BotTokenCallPurge(botNow - 10)
	require.NoError(t, err)
	assert.Equal(t, int64(1), purged)
	calls, err = s.BotTokenCallList(1, 10)
	require.NoError(t, err)
	assert.Len(t, calls, 2)
	calls, err = s.BotTokenCallList(2, 10)
	require.NoError(t, err)
	require.Len(t, calls, 1)
	assert.Equal(t, "revoked token", calls[0].Reason)
}

// The text of a call comes from the request: it never fails its record.
func TestBotTokenCallText(t *testing.T) {
	s, closer := newTestStore(t, new(model.BotTokenCall))
	defer closer()

	call := &model.BotTokenCall{
		TokenID: 1, Created: botNow,
		Method: "PROPPATCHES",
		Route:  "/api/\xff\x00x" + strings.Repeat("é", 200),
		Reason: strings.Repeat("r", 300),
		IP:     strings.Repeat("1", 80),
	}
	require.NoError(t, s.BotTokenCallAppend(call))
	calls, err := s.BotTokenCallList(1, 1)
	require.NoError(t, err)
	require.Len(t, calls, 1)
	got := calls[0]
	assert.Equal(t, "PROPPATCHE", got.Method, "cut to its column")
	assert.True(t, utf8.ValidString(got.Route))
	assert.NotContains(t, got.Route, "\x00")
	assert.True(t, strings.HasPrefix(got.Route, "/api/�x"))
	assert.LessOrEqual(t, len(got.Route), 255)
	assert.Len(t, got.Reason, 255)
	assert.Len(t, got.IP, 64)
}

func TestBotTokenCallPurgeBatches(t *testing.T) {
	s, closer := newTestStore(t, new(model.BotTokenCall))
	defer closer()

	calls := make([]*model.BotTokenCall, 0, botCallPurgeBatch*2+5)
	for i := range botCallPurgeBatch*2 + 5 {
		calls = append(calls, &model.BotTokenCall{TokenID: 1, Created: botNow - 100 + int64(i%2)})
	}
	_, err := s.engine.Insert(calls)
	require.NoError(t, err)
	require.NoError(t, s.BotTokenCallAppend(&model.BotTokenCall{TokenID: 1, Created: botNow}))

	purged, err := s.BotTokenCallPurge(botNow)
	require.NoError(t, err)
	assert.Equal(t, int64(botCallPurgeBatch*2+5), purged, "every batch")
	left, err := s.BotTokenCallList(1, 10)
	require.NoError(t, err)
	assert.Len(t, left, 1)
}

func TestDeleteUserDeletesBotTokens(t *testing.T) {
	s, closer := newTestStore(t, new(model.User), new(model.Org), new(model.Secret), new(model.Repo), new(model.Perm), new(model.BotToken))
	defer closer()

	user := botOwner(t, s, "bot-owner")
	keeper := botOwner(t, s, "keeper")
	token, _ := newBotToken(t, s, user.ID, "watcher")
	kept, _ := newBotToken(t, s, keeper.ID, "someone else's")

	require.NoError(t, s.DeleteUser(user))
	_, err := s.BotTokenGet(token.ID)
	assert.ErrorIs(t, err, types.ErrRecordNotExist)
	_, err = s.BotTokenGet(kept.ID)
	assert.NoError(t, err)
}
