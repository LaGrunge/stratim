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

package woodpecker

import (
	"fmt"
	"net/http"
	"strings"
)

const (
	pathWhoami          = "%s/api/v2/whoami"
	pathBotTokens       = "%s/api/v2/bot-tokens"
	pathBotToken        = "%s/api/v2/bot-tokens/%d"
	pathBotTokenCalls   = "%s/api/v2/bot-tokens/%d/calls?limit=%d"
	pathPipelineRestart = "%s/api/v2/pipelines/%d/restart"
)

// BotTokenHeader carries a bot token.
const BotTokenHeader = "X-Stratim-Token"

// BotTokenPrefix starts every bot token.
const BotTokenPrefix = "stb_"

// The scopes of a bot token.
const (
	BotScopeRead    = "read"
	BotScopeRestart = "restart"
)

// IsBotToken tells whether a token is a bot token.
func IsBotToken(token string) bool {
	return strings.HasPrefix(token, BotTokenPrefix)
}

// NewBotTokenTransport sends the bot token with each request in
// BotTokenHeader, over base (http.DefaultTransport if nil).
func NewBotTokenTransport(token string, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &botTokenTransport{token: token, base: base}
}

type botTokenTransport struct {
	token string
	base  http.RoundTripper
}

func (t *botTokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set(BotTokenHeader, t.token)
	return t.base.RoundTrip(req)
}

// BotToken is a bot token, without its secret.
type BotToken struct {
	ID       int64    `json:"id"`
	OwnerID  int64    `json:"owner_id"`
	Name     string   `json:"name"`
	Prefix   string   `json:"prefix"`
	Scopes   []string `json:"scopes"`
	RepoIDs  []int64  `json:"repo_ids"`
	Created  int64    `json:"created"`
	Expires  int64    `json:"expires"`
	LastUsed int64    `json:"last_used"`
	Revoked  int64    `json:"revoked"`
	// Status is active, expired or revoked.
	Status string `json:"status"`
}

// BotTokenCreated is a bot token just created, with its secret, which the
// server never tells again.
type BotTokenCreated struct {
	BotToken
	Token string `json:"token"`
}

// BotTokenCreateOptions are a bot token to create.
type BotTokenCreateOptions struct {
	Name    string   `json:"name"`
	Scopes  []string `json:"scopes"`
	RepoIDs []int64  `json:"repo_ids,omitempty"`
	// ExpiresInDays is 1 to 365; 0 is 30.
	ExpiresInDays int `json:"expires_in_days,omitempty"`
}

// BotTokenCall is the audit line of a call made with a bot token.
type BotTokenCall struct {
	ID         int64  `json:"id"`
	TokenID    int64  `json:"token_id"`
	OwnerID    int64  `json:"owner_id"`
	Created    int64  `json:"created"`
	Method     string `json:"method"`
	Route      string `json:"route"`
	RepoID     int64  `json:"repo_id"`
	PipelineID int64  `json:"pipeline_id"`
	Status     int    `json:"status"`
	LatencyMS  int64  `json:"latency_ms"`
	IP         string `json:"ip"`
	Reason     string `json:"reason"`
}

// Whoami tells who the requests of a client are made by.
type Whoami struct {
	User struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
	} `json:"user"`
	// Bot is the bot token of the client, if any.
	Bot *BotToken `json:"bot,omitempty"`
}

// PipelineRestartOptions are how to restart a pipeline over /api/v2.
type PipelineRestartOptions struct {
	// FailedOnly restarts only the failed workflows and those depending on
	// them.
	FailedOnly bool `json:"failed_only,omitempty"`
	// Reason is recorded in the audit of a bot token.
	Reason string `json:"reason,omitempty"`
}

// Whoami tells who the requests of the client are made by.
func (c *client) Whoami() (*Whoami, error) {
	out := new(Whoami)
	return out, c.get(fmt.Sprintf(pathWhoami, c.addr), out)
}

// BotTokenList lists the bot tokens of the user.
func (c *client) BotTokenList() ([]*BotToken, error) {
	var out []*BotToken
	return out, c.get(fmt.Sprintf(pathBotTokens, c.addr), &out)
}

// BotTokenCreate creates a bot token of the user.
func (c *client) BotTokenCreate(opt BotTokenCreateOptions) (*BotTokenCreated, error) {
	out := new(BotTokenCreated)
	return out, c.post(fmt.Sprintf(pathBotTokens, c.addr), opt, out)
}

// BotTokenRevoke revokes a bot token.
func (c *client) BotTokenRevoke(id int64) error {
	return c.delete(fmt.Sprintf(pathBotToken, c.addr, id))
}

// BotTokenCalls lists the latest calls made with a bot token, at most
// limit (1 to 1000), newest first.
func (c *client) BotTokenCalls(id int64, limit int) ([]*BotTokenCall, error) {
	var out []*BotTokenCall
	return out, c.get(fmt.Sprintf(pathBotTokenCalls, c.addr, id, limit), &out)
}

// PipelineRestart restarts the pipeline with the id over /api/v2.
func (c *client) PipelineRestart(pipelineID int64, opt PipelineRestartOptions) (*Pipeline, error) {
	out := new(Pipeline)
	return out, c.post(fmt.Sprintf(pathPipelineRestart, c.addr, pipelineID), opt, out)
}
