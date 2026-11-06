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

// Package bottoken authenticates the bot tokens of a request, records an
// audit line for each call made with one, and purges old audit lines.
package bottoken

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// Header carries a bot token. A bot token is also accepted as
// "Authorization: Bearer stb_…".
const Header = "X-Stratim-Token"

const (
	// TouchInterval is how often at most the last use of a token is
	// written.
	TouchInterval = time.Minute
	// DefaultRetention is how long the audit lines are kept by default.
	DefaultRetention = 30 * 24 * time.Hour
	// How often the audit lines older than the retention are deleted.
	purgeInterval = time.Hour
)

// Why a bot token is refused; the reason is recorded in the audit line,
// the client gets only "invalid bot token".
const (
	ReasonInQuery   = "token in the query"
	ReasonMalformed = "malformed token"
	ReasonUnknown   = "unknown token"
	ReasonRevoked   = "revoked token"
	ReasonExpired   = "expired token"
	ReasonOwnerGone = "owner gone"
	// ReasonAdminAllRepos is the token of an admin that names no repos.
	ReasonAdminAllRepos = "admin owner without repos"
)

// RefusedError is a bot token refused, and why.
type RefusedError struct {
	Reason string
	// Token is the token found, if any.
	Token *model.BotToken
}

func (e *RefusedError) Error() string { return "invalid bot token: " + e.Reason }

// FromRequest finds the bot token of a request: in the header, or as a
// Bearer token. The header makes a request a bot request whatever it
// holds: empty, given twice or not a bot token, it is refused as
// malformed. A bot token in the query is refused, so that it is not
// written to the logs of a proxy. It does not find one, found false, when
// the request carries none: it is then authenticated otherwise.
func FromRequest(r *http.Request) (raw string, found bool, err error) {
	if values, ok := r.Header[http.CanonicalHeaderKey(Header)]; ok {
		if len(values) != 1 || !model.IsBotToken(values[0]) {
			return "", true, &RefusedError{Reason: ReasonMalformed}
		}
		return values[0], true, nil
	}
	if scheme, credentials, ok := strings.Cut(r.Header.Get("Authorization"), " "); ok &&
		strings.EqualFold(scheme, "Bearer") && model.IsBotToken(strings.TrimSpace(credentials)) {
		return strings.TrimSpace(credentials), true, nil
	}
	if model.IsBotToken(r.URL.Query().Get("access_token")) {
		return "", true, &RefusedError{Reason: ReasonInQuery}
	}
	return "", false, nil
}

// Authenticate finds the active bot token of the secret and its owner, as
// the token acts: never an admin (see AsBot). A token refused gives a
// *RefusedError; any other error is of the store.
func Authenticate(s store.Store, raw string, now time.Time) (*model.BotToken, *model.User, error) {
	if !model.IsBotToken(raw) {
		return nil, nil, &RefusedError{Reason: ReasonMalformed}
	}
	token, err := s.BotTokenFindByHash(model.HashBotToken(raw))
	switch {
	case errors.Is(err, types.ErrRecordNotExist):
		return nil, nil, &RefusedError{Reason: ReasonUnknown}
	case err != nil:
		return nil, nil, fmt.Errorf("find a bot token: %w", err)
	}
	owner, err := checkWithOwner(s, token, now)
	if err != nil {
		return nil, nil, err
	}
	if now.Sub(time.Unix(token.LastUsed, 0)) >= TouchInterval {
		if err := s.BotTokenTouch(token.ID, now.Unix()); err != nil {
			log.Error().Err(err).Int64("bot_token", token.ID).Msg("cannot record the use of a bot token")
		}
	}
	return token, AsBot(owner), nil
}

// Recheck looks a token and its owner up again, and tells whether the
// token still is active: a *RefusedError if not. It gives the owner as the
// token acts. A long request calls it now and then.
func Recheck(s store.Store, token *model.BotToken, now time.Time) (*model.User, error) {
	fresh, err := s.BotTokenGet(token.ID)
	switch {
	case errors.Is(err, types.ErrRecordNotExist):
		return nil, &RefusedError{Reason: ReasonUnknown, Token: token}
	case err != nil:
		return nil, fmt.Errorf("find bot token %d: %w", token.ID, err)
	}
	owner, err := checkWithOwner(s, fresh, now)
	if err != nil {
		return nil, err
	}
	return AsBot(owner), nil
}

// checkWithOwner tells whether a token is active at now, and gives its
// owner. The token of an admin that names no repos is refused: an admin
// who made one unrestricted before they were an admin has it refused too.
func checkWithOwner(s store.Store, token *model.BotToken, now time.Time) (*model.User, error) {
	switch token.Status(now) {
	case model.BotTokenRevoked:
		return nil, &RefusedError{Reason: ReasonRevoked, Token: token}
	case model.BotTokenExpired:
		return nil, &RefusedError{Reason: ReasonExpired, Token: token}
	}
	owner, err := s.GetUser(token.OwnerID)
	switch {
	case errors.Is(err, types.ErrRecordNotExist):
		return nil, &RefusedError{Reason: ReasonOwnerGone, Token: token}
	case err != nil:
		return nil, fmt.Errorf("find the owner of bot token %d: %w", token.ID, err)
	}
	if owner.Admin && len(token.RepoIDs) == 0 {
		return nil, &RefusedError{Reason: ReasonAdminAllRepos, Token: token}
	}
	return owner, nil
}

// AsBot is the owner of a bot token as the token acts: a copy that is not
// an admin, which the store refuses to write.
func AsBot(owner *model.User) *model.User {
	bot := *owner
	bot.Admin = false
	bot.ActsForBot = true
	return &bot
}

// Record writes the audit line of a call made with a known token: a row
// and a log line.
func Record(s store.Store, call *model.BotTokenCall) {
	logCall(log.Info(), call)
	if err := s.BotTokenCallAppend(call); err != nil {
		log.Error().Err(err).Int64("bot_token", call.TokenID).Msg("cannot record a bot token call")
	}
}

// unknownBurst is how many calls with a token that is not known are
// logged a minute: they get no row.
const unknownBurst = 10

// unknownSampler keeps the count of the calls with a token not known that
// were logged, across the loggers the server is configured with.
var unknownSampler zerolog.Sampler = &zerolog.BurstSampler{Burst: unknownBurst, Period: time.Minute}

// RecordUnknown logs a call made with a token that is not known, malformed
// or in the query: a log line, sampled, and no row, as it is no token's
// call.
func RecordUnknown(call *model.BotTokenCall) {
	sampled := log.Logger.Sample(unknownSampler)
	logCall(sampled.Info(), call)
}

func logCall(e *zerolog.Event, call *model.BotTokenCall) {
	e.Int64("bot_token", call.TokenID).
		Int64("owner", call.OwnerID).
		Str("method", call.Method).
		Str("route", call.Route).
		Int64("repo", call.RepoID).
		Int64("pipeline", call.PipelineID).
		Int("status", call.Status).
		Int64("latency_ms", call.LatencyMS).
		Str("ip", call.IP).
		Str("reason", call.Reason).
		Msg("bot token call")
}

// RunPurge deletes the audit lines older than retention now and every
// hour, until ctx is done.
func RunPurge(ctx context.Context, s store.Store, retention time.Duration) {
	ticker := time.NewTicker(purgeInterval)
	defer ticker.Stop()
	for {
		Purge(s, retention, time.Now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Purge deletes the audit lines older than retention at now.
func Purge(s store.Store, retention time.Duration, now time.Time) {
	n, err := s.BotTokenCallPurge(now.Add(-retention).Unix())
	if err != nil {
		log.Error().Err(err).Msg("cannot purge the old bot token calls")
		return
	}
	if n > 0 {
		log.Debug().Int64("calls", n).Msg("purged the old bot token calls")
	}
}
