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
	"context"
	"errors"
	"time"

	"github.com/gin-gonic/gin"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// Principal is who a request is made by: a user, nil for a guest, and the
// bot token the request is made with, if any. With a bot token the user is
// its owner as the token acts, never an admin (see bottoken.AsBot), and
// the repos are those the token is restricted to.
type Principal struct {
	User *model.User
	Bot  *model.BotToken
}

// PrincipalOf is who the request is made by.
func PrincipalOf(c *gin.Context) Principal {
	return Principal{User: User(c), Bot: Bot(c)}
}

// AllowsRepo tells whether a bot token, if any, is not restricted away
// from the repo.
func (p Principal) AllowsRepo(repoID int64) bool {
	return p.Bot == nil || p.Bot.AllowsRepo(repoID)
}

// EffectivePerm is what the principal may do on a repo given the stored
// permission of its user, see EffectivePerm: nothing on a repo a bot token
// is restricted away from.
func (p Principal) EffectivePerm(repo *model.Repo, stored *model.Perm) *model.Perm {
	if !p.AllowsRepo(repo.ID) {
		return new(model.Perm)
	}
	return EffectivePerm(p.User, repo, stored)
}

// Perm is what the principal may do on a repo, from the stored permission
// of its user synced from the forge as LoadPerm does.
func (p Principal) Perm(ctx context.Context, s store.Store, getForge func() (forge.Forge, error), repo *model.Repo, now time.Time) (*model.Perm, error) {
	if !p.AllowsRepo(repo.ID) {
		return new(model.Perm), nil
	}
	stored, err := LoadPerm(ctx, s, getForge, p.User, repo, now)
	if err != nil {
		return nil, err
	}
	return p.EffectivePerm(repo, stored), nil
}

// CanReadStored tells whether the principal may read a repo from the
// stored permission of its user, without asking the forge: see
// CanReadStored.
func (p Principal) CanReadStored(s store.Store, repo *model.Repo) (bool, error) {
	if !p.AllowsRepo(repo.ID) {
		return false, nil
	}
	return CanReadStored(s, p.User, repo)
}

// ErrNoForge is the forge of a repo when the server has none to ask.
var ErrNoForge = errors.New("no forge to ask")

// RepoForge gets the forge of a repo when it is called: a permission is
// synced with it only once it is old.
func RepoForge(repo *model.Repo) func() (forge.Forge, error) {
	return func() (forge.Forge, error) {
		if server.Config.Services.Manager == nil {
			return nil, ErrNoForge
		}
		return server.Config.Services.Manager.ForgeFromRepo(repo)
	}
}
