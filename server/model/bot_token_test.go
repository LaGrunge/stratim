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

package model

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBotTokenSecret(t *testing.T) {
	secret, hash, shown := NewBotTokenSecret()
	assert.Regexp(t, regexp.MustCompile(`^stb_[a-z2-7]{52}$`), secret)
	assert.True(t, IsBotToken(secret))
	assert.Equal(t, HashBotToken(secret), hash)
	assert.Len(t, hash, 64)
	assert.NotContains(t, hash, secret[len(BotTokenPrefix):])
	assert.Equal(t, secret[:12], shown)

	other, otherHash, _ := NewBotTokenSecret()
	assert.NotEqual(t, secret, other)
	assert.NotEqual(t, hash, otherHash)
}

func TestBotTokenStatus(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	for _, tt := range []struct {
		name  string
		token BotToken
		want  BotTokenStatus
	}{
		{"active", BotToken{Expires: now.Unix() + 1}, BotTokenActive},
		{"expired at the second", BotToken{Expires: now.Unix()}, BotTokenExpired},
		{"expired", BotToken{Expires: now.Unix() - 1}, BotTokenExpired},
		{"revoked", BotToken{Expires: now.Unix() + 1, Revoked: now.Unix() - 1}, BotTokenRevoked},
		{"revoked and expired", BotToken{Expires: now.Unix() - 1, Revoked: now.Unix() - 1}, BotTokenRevoked},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.token.Status(now))
		})
	}
}

func TestBotTokenAllowsRepoAndScope(t *testing.T) {
	open := BotToken{Scopes: []BotScope{BotScopeRead}}
	assert.True(t, open.AllowsRepo(7))
	assert.True(t, open.HasScope(BotScopeRead))
	assert.False(t, open.HasScope(BotScopeRestart))

	restricted := BotToken{RepoIDs: []int64{3, 5}}
	assert.True(t, restricted.AllowsRepo(5))
	assert.False(t, restricted.AllowsRepo(4))
}

func TestBotTokenValidate(t *testing.T) {
	valid := func() *BotToken {
		return &BotToken{Name: " watcher ", Scopes: []BotScope{BotScopeRestart, BotScopeRead, BotScopeRead}, RepoIDs: []int64{5, 3, 5}}
	}
	token := valid()
	require.NoError(t, token.Validate())
	assert.Equal(t, "watcher", token.Name)
	assert.Equal(t, []BotScope{BotScopeRead, BotScopeRestart}, token.Scopes)
	assert.Equal(t, []int64{3, 5}, token.RepoIDs)

	for _, tt := range []struct {
		name   string
		change func(*BotToken)
		want   error
	}{
		{"no name", func(b *BotToken) { b.Name = "  " }, ErrBotTokenName},
		{"a long name", func(b *BotToken) { b.Name = strings.Repeat("ä", BotTokenNameMaxLen+1) }, ErrBotTokenName},
		{"no scope", func(b *BotToken) { b.Scopes = nil }, ErrBotTokenScopes},
		{"annotate is reserved", func(b *BotToken) { b.Scopes = []BotScope{BotScopeAnnotate} }, ErrBotTokenScopes},
		{"an unknown scope", func(b *BotToken) { b.Scopes = []BotScope{"admin"} }, ErrBotTokenScopes},
		{"a bad repo id", func(b *BotToken) { b.RepoIDs = []int64{0} }, ErrBotTokenRepos},
		{"too many repos", func(b *BotToken) {
			b.RepoIDs = nil
			for i := range BotTokenMaxRepos + 1 {
				b.RepoIDs = append(b.RepoIDs, int64(i+1))
			}
		}, ErrBotTokenRepos},
	} {
		t.Run(tt.name, func(t *testing.T) {
			token := valid()
			tt.change(token)
			assert.ErrorIs(t, token.Validate(), tt.want)
		})
	}

	name := valid()
	name.Name = strings.Repeat("ä", BotTokenNameMaxLen)
	assert.NoError(t, name.Validate(), "the limit is in characters")
}
