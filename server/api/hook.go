// Copyright 2022 Woodpecker Authors
// Copyright 2021 Informatyka Boguslawski sp. z o.o. sp.k., http://www.ib.pl/
// Copyright 2018 Drone.IO Inc.
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
	"bytes"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/webhook"
	"go.woodpecker-ci.org/woodpecker/v3/shared/token"
)

// getAgentName finds an agent's name, utilizing a map as a cache.
func getAgentName(store store.Store, agentNameMap map[int64]string, agentID int64) (string, bool) {
	// 1. Check the cache first.
	name, exists := agentNameMap[agentID]
	if exists {
		return name, true
	}

	// 2. If not in cache, query the store.
	agent, err := store.AgentFind(agentID)
	if err != nil || agent == nil {
		// Agent not found or an error occurred.
		return "", false
	}

	// 3. Found the agent, update the cache and return the name.
	if agent.Name != "" {
		agentNameMap[agentID] = agent.Name
		return agent.Name, true
	}

	return "", false
}

// PostHook
//
//	@Summary		Incoming webhook from forge
//	@Description	The webhook is kept, then made into a pipeline in the background. A delivery the forge sends again, with the same delivery ID, is taken once.
//	@Router			/hook [post]
//	@Produce		plain
//	@Success		202	"kept, a pipeline follows if it makes one"
//	@Success		200	"delivered again, kept already"
//	@Success		204	"the repo is not active"
//	@Tags			System
//	@Param			hook	body	object	true	"the webhook payload; forge is automatically detected"
func PostHook(c *gin.Context) {
	_store := store.FromContext(c)

	// read first: the token may be looked for in a form, which reads it
	body, err := webhook.ReadBody(c.Request)
	if errors.Is(err, webhook.ErrTooLarge) {
		c.String(http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	if err != nil {
		c.String(http.StatusBadRequest, "cannot read the webhook")
		return
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(body))

	var repo *model.Repo

	_, err = token.ParseRequest([]token.Type{token.HookToken}, c.Request, func(t *token.Token) (string, error) {
		var err error
		repo, err = getRepoFromToken(_store, t)
		if err != nil {
			return "", err
		}

		return repo.WebhookSecret, nil
	})
	if err != nil {
		msg := "failure to parse token from hook"
		log.Error().Err(err).Msg(msg)
		c.String(http.StatusBadRequest, msg)
		return
	}

	if repo == nil {
		msg := "failure to get repo from token"
		log.Error().Msg(msg)
		c.String(http.StatusBadRequest, msg)
		return
	}

	if !repo.IsActive {
		log.Debug().Msgf("ignoring hook: repo %s is inactive", repo.FullName)
		c.Status(http.StatusNoContent)
		return
	}

	inbox, ok := webhook.FromContext(c)
	if !ok {
		log.Error().Msg("no webhook inbox")
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	// kept before it is answered, made into a pipeline after: a forge
	// waits for no forge call nor config, and what is kept is not lost
	created, err := inbox.Receive(repo, c.Request.Header, body)
	switch {
	case err != nil:
		log.Error().Err(err).Str("repo", repo.FullName).Msg("cannot keep a webhook")
		c.String(http.StatusInternalServerError, "cannot keep the webhook")
	case !created:
		c.String(http.StatusOK, "delivered again, kept already")
	default:
		c.String(http.StatusAccepted, "kept")
	}
}

func getRepoFromToken(store store.Store, t *token.Token) (*model.Repo, error) {
	if t.Get("repo-forge-remote-id") != "" {
		forgeID, err := strconv.ParseInt(t.Get("forge-id"), 10, 64)
		if err != nil {
			return nil, err
		}

		return store.GetRepoForgeID(forgeID, model.ForgeRemoteID(t.Get("repo-forge-remote-id")))
	}

	// get the repo by the repo-id
	// TODO: remove in next major
	repoID, err := strconv.ParseInt(t.Get("repo-id"), 10, 64)
	if err != nil {
		return nil, err
	}
	return store.GetRepo(repoID)
}
