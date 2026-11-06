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
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// BotTokenPrefix starts every bot token, which tells it from the other
// tokens of the server.
const BotTokenPrefix = "stb_"

// botTokenShown is how many characters of a token, its prefix included, are
// kept to tell it in a list.
const botTokenShown = len(BotTokenPrefix) + 8

// BotScope is what a bot token may do on /api/v2.
type BotScope string

const (
	// BotScopeRead reads events, waits for pipelines and reads what a step
	// did.
	BotScopeRead BotScope = "read"
	// BotScopeRestart restarts a pipeline, or its failed workflows.
	BotScopeRestart BotScope = "restart"
	// BotScopeAnnotate is reserved for annotations of pipelines; it cannot
	// be granted yet.
	BotScopeAnnotate BotScope = "annotate"
)

// BotScopesGrantable are the scopes a bot token can be created with.
var BotScopesGrantable = []BotScope{BotScopeRead, BotScopeRestart}

const (
	// BotTokenNameMaxLen is the longest name of a bot token.
	BotTokenNameMaxLen = 100
	// BotTokenMaxRepos is the most repos a bot token can be restricted to.
	BotTokenMaxRepos = 100
)

var (
	ErrBotTokenName   = errors.New("name: want 1 to 100 characters")
	ErrBotTokenScopes = errors.New("scopes: want at least one of read, restart")
	ErrBotTokenRepos  = errors.New("repo_ids: want at most 100 repo ids")
)

// BotToken is a token a user creates for a program to call /api/v2 on
// their behalf: it may do what its scopes allow on the repos it is
// restricted to, never more than its owner may. Only the SHA-256 hash of
// its secret is kept.
type BotToken struct {
	ID      int64  `json:"id"       xorm:"pk autoincr 'id'"`
	OwnerID int64  `json:"owner_id" xorm:"INDEX 'owner_id'"`
	Name    string `json:"name"     xorm:"varchar(100) 'name'"`
	// Hash is the hex SHA-256 of the token.
	Hash string `json:"-" xorm:"UNIQUE varchar(64) 'token_hash'"`
	// Prefix is the start of the token, to tell it in a list.
	Prefix string     `json:"prefix" xorm:"varchar(16) 'token_prefix'"`
	Scopes []BotScope `json:"scopes" xorm:"json 'scopes'"`
	// RepoIDs restrict the token to these repos; none restricts it to
	// nothing more than its owner's.
	RepoIDs []int64 `json:"repo_ids" xorm:"json 'repo_ids'"`
	// Created, Expires, LastUsed and Revoked are unix seconds; 0 is never.
	Created  int64 `json:"created"   xorm:"'created'"`
	Expires  int64 `json:"expires"   xorm:"'expires'"`
	LastUsed int64 `json:"last_used" xorm:"'last_used'"`
	Revoked  int64 `json:"revoked"   xorm:"'revoked'"`
} //	@name	BotToken

// TableName return database table name for xorm.
func (BotToken) TableName() string {
	return "bot_tokens"
}

// BotTokenStatus is the state of a bot token at a time.
type BotTokenStatus string

const (
	BotTokenActive  BotTokenStatus = "active"
	BotTokenExpired BotTokenStatus = "expired"
	BotTokenRevoked BotTokenStatus = "revoked"
)

// Status tells whether the token is active at now.
func (t *BotToken) Status(now time.Time) BotTokenStatus {
	switch {
	case t.Revoked != 0:
		return BotTokenRevoked
	case t.Expires <= now.Unix():
		return BotTokenExpired
	default:
		return BotTokenActive
	}
}

// HasScope tells whether the token has the scope.
func (t *BotToken) HasScope(scope BotScope) bool {
	return slices.Contains(t.Scopes, scope)
}

// AllowsRepo tells whether the token is not restricted away from the repo.
func (t *BotToken) AllowsRepo(repoID int64) bool {
	return len(t.RepoIDs) == 0 || slices.Contains(t.RepoIDs, repoID)
}

// Validate checks the name, the scopes and the repos of a token to create,
// and sorts its scopes and repos, without duplicates.
func (t *BotToken) Validate() error {
	t.Name = strings.TrimSpace(t.Name)
	if t.Name == "" || len([]rune(t.Name)) > BotTokenNameMaxLen {
		return ErrBotTokenName
	}
	if len(t.Scopes) == 0 {
		return ErrBotTokenScopes
	}
	for _, scope := range t.Scopes {
		if !slices.Contains(BotScopesGrantable, scope) {
			return fmt.Errorf("%w: %q cannot be granted", ErrBotTokenScopes, scope)
		}
	}
	slices.Sort(t.Scopes)
	t.Scopes = slices.Compact(t.Scopes)
	for _, id := range t.RepoIDs {
		if id <= 0 {
			return fmt.Errorf("%w: %d is no repo id", ErrBotTokenRepos, id)
		}
	}
	slices.Sort(t.RepoIDs)
	t.RepoIDs = slices.Compact(t.RepoIDs)
	if len(t.RepoIDs) > BotTokenMaxRepos {
		return ErrBotTokenRepos
	}
	return nil
}

// NewBotTokenSecret makes the secret of a new bot token: its prefix and 32
// random bytes. It returns the secret, its hash and the start to show.
func NewBotTokenSecret() (secret, hash, shown string) {
	raw := make([]byte, 32)
	_, _ = rand.Read(raw) // never fails, see crypto/rand
	secret = BotTokenPrefix + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw))
	return secret, HashBotToken(secret), secret[:botTokenShown]
}

// HashBotToken is the hash of a bot token the store keeps.
func HashBotToken(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// IsBotToken tells whether a token is a bot token by its prefix.
func IsBotToken(raw string) bool {
	return strings.HasPrefix(raw, BotTokenPrefix)
}

// BotTokenCall is the audit record of one request made with a bot token.
type BotTokenCall struct {
	ID int64 `json:"id" xorm:"pk autoincr 'id'"`
	// TokenID is 0 when the token was not known.
	TokenID int64 `json:"token_id" xorm:"INDEX(token_created) 'token_id'"`
	OwnerID int64 `json:"owner_id" xorm:"'owner_id'"`
	// Created is unix seconds.
	Created    int64  `json:"created"     xorm:"INDEX(token_created) INDEX(created) 'created'"`
	Method     string `json:"method"      xorm:"varchar(10) 'method'"`
	Route      string `json:"route"       xorm:"varchar(255) 'route'"`
	RepoID     int64  `json:"repo_id"     xorm:"'repo_id'"`
	PipelineID int64  `json:"pipeline_id" xorm:"'pipeline_id'"`
	Status     int    `json:"status"      xorm:"'status'"`
	LatencyMS  int64  `json:"latency_ms"  xorm:"'latency_ms'"`
	IP         string `json:"ip"          xorm:"varchar(64) 'ip'"`
	// Reason is the reason of a write the caller gave, or why the call was
	// refused.
	Reason string `json:"reason" xorm:"varchar(255) 'reason'"`
} //	@name	BotTokenCall

// TableName return database table name for xorm.
func (BotTokenCall) TableName() string {
	return "bot_token_calls"
}
