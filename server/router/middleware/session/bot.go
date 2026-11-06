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

package session

import (
	"errors"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/bottoken"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

const (
	botKey         = "bot"
	botReasonKey   = "bot_audit_reason"
	botStartKey    = "bot_audit_start"
	botV2Prefix    = "/api/v2/"
	msgInvalidBot  = "invalid bot token"
	msgBotRefused  = "bot tokens are not accepted here"
	reasonNotOpen  = "route not open to bot tokens"
	reasonNoScope  = "missing scope "
	botReasonLimit = 200
)

// Bot is the bot token the request is made with, nil if none: the user of
// the request is then its owner as the token acts.
func Bot(c *gin.Context) *model.BotToken {
	v, ok := c.Get(botKey)
	if !ok {
		return nil
	}
	t, ok := v.(*model.BotToken)
	if !ok {
		return nil
	}
	return t
}

// SetBotAuditReason records the reason a caller gave for a write, for the
// audit line of the call.
func SetBotAuditReason(c *gin.Context, reason string) {
	if len(reason) > botReasonLimit {
		reason = reason[:botReasonLimit]
	}
	c.Set(botReasonKey, reason)
}

// BotAudit is what the audit line of a route records the call was about,
// found from the route's parameters.
type BotAudit int

const (
	// BotAuditNone records no repo or pipeline.
	BotAuditNone BotAudit = iota
	// BotAuditPipeline records the pipeline of the pipeline_id parameter,
	// and its repo.
	BotAuditPipeline
	// BotAuditStep records the pipeline and the repo of the step of the
	// step_id parameter.
	BotAuditStep
)

// BotRoute is what a route open to bot tokens needs, and what its audit
// line records.
type BotRoute struct {
	// Scope is the scope the route needs; "" lets any bot token call it.
	Scope model.BotScope
	Audit BotAudit
}

// BotRoutes are the routes bot tokens may call, as they are registered
// with Handle: every other route refuses bot tokens.
type BotRoutes struct {
	rootPath string
	routes   map[string]BotRoute
}

// NewBotRoutes makes the routes open to bot tokens of a server whose root
// path is rootPath: none until they are registered.
func NewBotRoutes(rootPath string) *BotRoutes {
	return &BotRoutes{rootPath: rootPath, routes: make(map[string]BotRoute)}
}

// Handle registers a route of the group, open to bot tokens as route says.
// A route outside /api/v2 is registered closed to them, with an error
// logged: no other route is open to them.
func (b *BotRoutes) Handle(g *gin.RouterGroup, method, relativePath string, route BotRoute, handlers ...gin.HandlerFunc) {
	full := path.Join(g.BasePath(), relativePath)
	if strings.HasPrefix(full, b.rootPath+botV2Prefix) {
		b.routes[method+" "+full] = route
	} else {
		log.Error().Str("route", method+" "+full).Msg("a route outside " + botV2Prefix + " is not opened to bot tokens")
	}
	g.Handle(method, relativePath, handlers...)
}

// Lookup gives the route of a method and a full path, if it is open to bot
// tokens.
func (b *BotRoutes) Lookup(method, fullPath string) (BotRoute, bool) {
	route, ok := b.routes[method+" "+fullPath]
	return route, ok
}

// Keys are the routes open to bot tokens, "METHOD /full/path", sorted.
func (b *BotRoutes) Keys() []string {
	keys := make([]string, 0, len(b.routes))
	for key := range b.routes {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// setBotUser authenticates the request by its bot token: the owner of an
// active one becomes the user of the request, never an admin. Any other
// token aborts it with 401: recorded as a call of the token when it is
// known, only logged when it is not.
func setBotUser(c *gin.Context, raw string, found error) {
	c.Set(botStartKey, time.Now())
	_store := store.FromContext(c)
	err := found
	var token *model.BotToken
	var owner *model.User
	if err == nil {
		token, owner, err = bottoken.Authenticate(_store, raw, time.Now())
	}
	if refused, ok := errors.AsType[*bottoken.RefusedError](err); ok {
		c.String(http.StatusUnauthorized, msgInvalidBot)
		c.Abort()
		recordBotCall(c, refused.Token, nil, refused.Reason)
		return
	}
	if err != nil {
		log.Error().Err(err).Msg("cannot authenticate a bot token")
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	c.Set("user", owner)
	c.Set(botKey, token)
	c.Next()
}

// AuditBot records the audit line of each call made with a bot token
// once it is answered, refused ones included.
func AuditBot(routes *BotRoutes) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := Bot(c)
		if token == nil {
			c.Next()
			return
		}
		c.Next()
		route, _ := routes.Lookup(c.Request.Method, c.FullPath())
		recordBotCall(c, token, &route, c.GetString(botReasonKey))
	}
}

// GuardBot lets a request made with a bot token through to the routes
// open to bot tokens when the token has the scope the route needs, and
// refuses it with 403 anywhere else.
func GuardBot(routes *BotRoutes) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := Bot(c)
		if token == nil {
			c.Next()
			return
		}
		route, open := routes.Lookup(c.Request.Method, c.FullPath())
		switch {
		case c.FullPath() == "" || !open:
			c.Set(botReasonKey, reasonNotOpen)
			c.String(http.StatusForbidden, msgBotRefused)
			c.Abort()
		case route.Scope != "" && !token.HasScope(route.Scope):
			c.Set(botReasonKey, reasonNoScope+string(route.Scope))
			c.String(http.StatusForbidden, "missing scope: %s", route.Scope)
			c.Abort()
		default:
			c.Next()
		}
	}
}

// recordBotCall writes the audit line of a call made with a bot token,
// with what the route says it was about when it was answered with 2xx;
// token is nil when it is not known, and the call is then only logged.
func recordBotCall(c *gin.Context, token *model.BotToken, route *BotRoute, reason string) {
	call := &model.BotTokenCall{
		Created: time.Now().Unix(),
		Method:  c.Request.Method,
		Route:   c.FullPath(),
		Status:  c.Writer.Status(),
		IP:      c.ClientIP(),
		Reason:  reason,
	}
	if call.Route == "" {
		call.Route = c.Request.URL.Path
	}
	if start, ok := c.Get(botStartKey); ok {
		if start, ok := start.(time.Time); ok {
			call.LatencyMS = time.Since(start).Milliseconds()
		}
	}
	if token == nil {
		bottoken.RecordUnknown(call)
		return
	}
	call.TokenID, call.OwnerID = token.ID, token.OwnerID
	_store := store.FromContext(c)
	// what a refused call was about is not told: it could be of a repo the
	// token may not see
	if route != nil && call.Status >= http.StatusOK && call.Status < http.StatusMultipleChoices {
		call.RepoID, call.PipelineID = auditTarget(c, _store, route.Audit)
	}
	bottoken.Record(_store, call)
}

// auditTarget finds the repo and the pipeline a call was about from its
// parameters; none when they are not found.
func auditTarget(c *gin.Context, s store.Store, audit BotAudit) (repoID, pipelineID int64) {
	switch audit {
	case BotAuditPipeline:
		pipelineID, _ = strconv.ParseInt(c.Param("pipeline_id"), 10, 64)
	case BotAuditStep:
		stepID, err := strconv.ParseInt(c.Param("step_id"), 10, 64)
		if err != nil {
			return 0, 0
		}
		step, err := s.StepGet(stepID)
		if err != nil {
			return 0, 0
		}
		pipelineID = step.PipelineID
	default:
		return 0, 0
	}
	if pipelineID <= 0 {
		return 0, 0
	}
	pipeline, err := s.GetPipeline(pipelineID)
	if err != nil {
		return 0, pipelineID
	}
	return pipeline.RepoID, pipelineID
}
