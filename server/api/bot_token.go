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

package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"go.woodpecker-ci.org/woodpecker/v3/server/bottoken"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

const (
	// How long a bot token lives by default, and at most.
	botTokenDefaultDays = 30
	botTokenMaxDays     = 365
	// How many active bot tokens a user may have.
	botTokensPerUser = 20
	// How many calls of a token are listed by default, and at most.
	botCallsDefault = 100
	botCallsMax     = 1000
)

// BotTokenRequest creates a bot token.
type BotTokenRequest struct {
	Name   string           `json:"name"`
	Scopes []model.BotScope `json:"scopes"`
	// RepoIDs restrict the token to these repos; an admin has to name them.
	RepoIDs []int64 `json:"repo_ids"`
	// ExpiresInDays is how long the token lives: 1 to 365 days, 30 if 0.
	ExpiresInDays int `json:"expires_in_days"`
} //	@name	BotTokenRequest

// BotTokenInfo is a bot token as the API shows it: never its secret.
type BotTokenInfo struct {
	model.BotToken
	Status model.BotTokenStatus `json:"status"`
} //	@name	BotTokenInfo

// BotTokenCreated is a bot token just created, with its secret: the only
// time the server tells it.
type BotTokenCreated struct {
	BotTokenInfo
	Token string `json:"token"`
} //	@name	BotTokenCreated

// Whoami tells who a request is made by.
type Whoami struct {
	User WhoamiUser `json:"user"`
	// Bot is the bot token the request is made with, if any.
	Bot *BotTokenInfo `json:"bot,omitempty"`
} //	@name	Whoami

// WhoamiUser is the user a request is made by; with a bot token, its
// owner.
type WhoamiUser struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
} //	@name	WhoamiUser

func newBotTokenInfo(token *model.BotToken, now time.Time) BotTokenInfo {
	info := BotTokenInfo{BotToken: *token, Status: token.Status(now)}
	if info.Scopes == nil {
		info.Scopes = []model.BotScope{}
	}
	if info.RepoIDs == nil {
		info.RepoIDs = []int64{}
	}
	return info
}

// refuseBot answers 403 to a request made with a bot token: bot tokens are
// managed by their owner only.
func refuseBot(c *gin.Context) bool {
	if session.Bot(c) != nil {
		c.String(http.StatusForbidden, "bot tokens cannot manage bot tokens")
		return true
	}
	return false
}

// WhoamiV2
//
//	@Summary		Who the request is made by
//	@Description	The user of the request; with a bot token, its owner and the token, scopes and expiry included.
//	@Router			/v2/whoami [get]
//	@Produce		json
//	@Success		200	{object}	Whoami
//	@Tags			Bot tokens
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
func WhoamiV2(c *gin.Context) {
	user := session.User(c)
	who := Whoami{User: WhoamiUser{ID: user.ID, Login: user.Login}}
	if bot := session.Bot(c); bot != nil {
		info := newBotTokenInfo(bot, time.Now())
		who.Bot = &info
	}
	c.JSON(http.StatusOK, who)
}

// GetBotTokens
//
//	@Summary		List bot tokens
//	@Description	The bot tokens of the user, newest first; an admin lists those of all users with all=true.
//	@Router			/v2/bot-tokens [get]
//	@Produce		json
//	@Success		200	{array}	BotTokenInfo
//	@Tags			Bot tokens
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			all				query	bool	false	"of all users (admins only)"
//	@Param			page			query	int		false	"for response pagination, page offset number"	default(1)
//	@Param			perPage			query	int		false	"for response pagination, max items per page"	default(50)
func GetBotTokens(c *gin.Context) {
	if refuseBot(c) {
		return
	}
	user := session.User(c)
	owner := user.ID
	if c.Query("all") == "true" {
		if !user.Admin {
			c.String(http.StatusForbidden, "only admins list the bot tokens of all users")
			return
		}
		owner = 0
	}
	tokens, err := store.FromContext(c).BotTokenList(owner, &model.ListOptionsWithAll{ListOptions: session.Pagination(c)})
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	now := time.Now()
	infos := make([]BotTokenInfo, 0, len(tokens))
	for _, token := range tokens {
		infos = append(infos, newBotTokenInfo(token, now))
	}
	c.JSON(http.StatusOK, infos)
}

// PostBotToken
//
//	@Summary		Create a bot token
//	@Description	Creates a bot token of the user. The response carries the token, which the server never tells again.
//	@Description	Scopes: read, restart. A token never may more than its owner; it can be restricted to some repos, and
//	@Description	an admin's has to be.
//	@Router			/v2/bot-tokens [post]
//	@Produce		json
//	@Success		201	{object}	BotTokenCreated
//	@Tags			Bot tokens
//	@Param			Authorization	header	string			true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			request			body	BotTokenRequest	true	"the token to create"
func PostBotToken(c *gin.Context) {
	if refuseBot(c) {
		return
	}
	var req BotTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.String(http.StatusBadRequest, "invalid body: %s", err)
		return
	}
	days := req.ExpiresInDays
	if days == 0 {
		days = botTokenDefaultDays
	}
	if days < 1 || days > botTokenMaxDays {
		c.String(http.StatusBadRequest, "expires_in_days: want 1 to %d", botTokenMaxDays)
		return
	}

	user := session.User(c)
	now := time.Now()
	token := &model.BotToken{
		OwnerID: user.ID,
		Name:    req.Name,
		Scopes:  req.Scopes,
		RepoIDs: req.RepoIDs,
		Created: now.Unix(),
		Expires: now.Add(time.Duration(days) * 24 * time.Hour).Unix(),
	}
	if err := token.Validate(); err != nil {
		c.String(http.StatusBadRequest, "%s", err)
		return
	}
	if user.Admin && len(token.RepoIDs) == 0 {
		c.String(http.StatusBadRequest, "repo_ids: the bot token of an admin has to name its repos")
		return
	}

	_store := store.FromContext(c)
	// the token acts as its owner never an admin
	acting := session.Principal{User: bottoken.AsBot(user)}
	for _, id := range token.RepoIDs {
		repo, err := _store.GetRepo(id)
		if errors.Is(err, types.ErrRecordNotExist) {
			c.String(http.StatusBadRequest, "repo_ids: no repo %d", id)
			return
		}
		if err != nil {
			_ = c.AbortWithError(http.StatusInternalServerError, err)
			return
		}
		readable, err := acting.CanReadStored(_store, repo)
		if err != nil {
			_ = c.AbortWithError(http.StatusInternalServerError, err)
			return
		}
		if !readable {
			c.String(http.StatusBadRequest, "repo_ids: no repo %d", id)
			return
		}
	}

	secret, hash, shown := model.NewBotTokenSecret()
	token.Hash, token.Prefix = hash, shown
	err := _store.BotTokenCreate(token, botTokensPerUser, now.Unix())
	if errors.Is(err, types.ErrBotTokenLimit) {
		c.String(http.StatusConflict, "at most %d active bot tokens per user: revoke one first", botTokensPerUser)
		return
	}
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusCreated, BotTokenCreated{BotTokenInfo: newBotTokenInfo(token, now), Token: secret})
}

// botTokenOf finds the bot token of the path that the user may manage: their
// own, or any for an admin. It answers 404 itself if there is none.
func botTokenOf(c *gin.Context) *model.BotToken {
	id, err := strconv.ParseInt(c.Param("token_id"), 10, 64)
	if err != nil || id <= 0 {
		c.String(http.StatusBadRequest, "token_id: want a bot token id")
		return nil
	}
	token, err := store.FromContext(c).BotTokenGet(id)
	if err != nil {
		handleDBError(c, err)
		return nil
	}
	if user := session.User(c); token.OwnerID != user.ID && !user.Admin {
		c.AbortWithStatus(http.StatusNotFound)
		return nil
	}
	return token
}

// DeleteBotToken
//
//	@Summary		Revoke a bot token
//	@Description	Revokes a bot token of the user, or any for an admin: it is refused from then on, and requests running
//	@Description	with it end within a minute. A revoked token stays listed; revoking it again changes nothing.
//	@Router			/v2/bot-tokens/{token_id} [delete]
//	@Success		204
//	@Tags			Bot tokens
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			token_id		path	int		true	"the id of the bot token"
func DeleteBotToken(c *gin.Context) {
	if refuseBot(c) {
		return
	}
	token := botTokenOf(c)
	if token == nil {
		return
	}
	if _, err := store.FromContext(c).BotTokenRevoke(token.ID, time.Now().Unix()); err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// GetBotTokenCalls
//
//	@Summary		List the calls of a bot token
//	@Description	The latest calls made with a bot token of the user, or any for an admin, newest first: what was
//	@Description	called, the answer and why a call was refused. Calls are kept for a retention set on the server.
//	@Router			/v2/bot-tokens/{token_id}/calls [get]
//	@Produce		json
//	@Success		200	{array}	BotTokenCall
//	@Tags			Bot tokens
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			token_id		path	int		true	"the id of the bot token"
//	@Param			limit			query	int		false	"how many calls at most, up to 1000"	default(100)
func GetBotTokenCalls(c *gin.Context) {
	if refuseBot(c) {
		return
	}
	limit := botCallsDefault
	if s := c.Query("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > botCallsMax {
			c.String(http.StatusBadRequest, "limit: want 1 to %d", botCallsMax)
			return
		}
		limit = n
	}
	token := botTokenOf(c)
	if token == nil {
		return
	}
	calls, err := store.FromContext(c).BotTokenCallList(token.ID, limit)
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, calls)
}
