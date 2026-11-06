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

package session

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// PermSyncInterval defines how long stored repo permissions of a user are
// trusted before they are re-synced with the forge.
const PermSyncInterval = time.Hour

func Repo(c *gin.Context) *model.Repo {
	v, ok := c.Get("repo")
	if !ok {
		return nil
	}
	r, ok := v.(*model.Repo)
	if !ok {
		return nil
	}
	r.Perm = Perm(c)
	return r
}

func SetRepo() gin.HandlerFunc {
	return func(c *gin.Context) {
		var (
			_store   = store.FromContext(c)
			fullName = strings.TrimLeft(c.Param("repo_full_name"), "/")
			_repoID  = c.Param("repo_id")
			user     = User(c)
		)

		var repo *model.Repo
		var err error
		if _repoID != "" {
			var repoID int64
			repoID, err = strconv.ParseInt(_repoID, 10, 64)
			if err != nil {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
			repo, err = _store.GetRepo(repoID)
		} else {
			repo, err = _store.GetRepoName(fullName)
		}

		if repo != nil && err == nil {
			c.Set("repo", repo)
			c.Next()
			return
		}

		// debugging
		log.Debug().Err(err).Msgf("cannot find repository %s", fullName)

		if user == nil {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		if errors.Is(err, types.ErrRecordNotExist) {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}

		_ = c.AbortWithError(http.StatusInternalServerError, err)
	}
}

func Perm(c *gin.Context) *model.Perm {
	v, ok := c.Get("perm")
	if !ok {
		return nil
	}
	u, ok := v.(*model.Perm)
	if !ok {
		return nil
	}
	return u
}

func SetPerm() gin.HandlerFunc {
	return func(c *gin.Context) {
		_store := store.FromContext(c)
		user := User(c)
		repo := Repo(c)
		_forge, err := server.Config.Services.Manager.ForgeFromRepo(repo)
		if err != nil {
			log.Error().Err(err).Msg("Cannot get forge from repo")
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		perm, err := PrincipalOf(c).Perm(c, _store, func() (forge.Forge, error) { return _forge, nil }, repo, time.Now())
		if err != nil {
			_ = c.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		if user != nil {
			log.Debug().Msgf("%s granted %+v permission to %s",
				user.Login, perm, repo.FullName)
		} else {
			log.Debug().Msgf("guest granted %+v to %s", perm, repo.FullName)
		}

		c.Set("perm", perm)
		c.Next()
	}
}

func MustPull(c *gin.Context) {
	user := User(c)
	perm := Perm(c)

	if perm.Pull {
		c.Next()
		return
	}

	// debugging
	if user != nil {
		c.AbortWithStatus(http.StatusNotFound)
		log.Debug().Msgf("user %s denied read access to %s",
			user.Login, c.Request.URL.Path)
	} else {
		c.AbortWithStatus(http.StatusUnauthorized)
		log.Debug().Msgf(
			"guest denied read access to %s %s",
			c.Request.Method,
			c.Request.URL.Path,
		)
	}
}

func MustPush(c *gin.Context) {
	if CheckPush(c, Perm(c)) {
		c.Next()
	}
}

// CheckPush tells whether the user of the request may push by the
// permission; if not, it answers the request: 404 to a user, 401 to a
// guest.
func CheckPush(c *gin.Context, perm *model.Perm) bool {
	if perm.Push {
		return true
	}

	// debugging
	if user := User(c); user != nil {
		c.AbortWithStatus(http.StatusNotFound)
		log.Debug().Msgf("user %s denied write access to %s",
			user.Login, c.Request.URL.Path)
	} else {
		c.AbortWithStatus(http.StatusUnauthorized)
		log.Debug().Msgf(
			"guest denied write access to %s %s",
			c.Request.Method,
			c.Request.URL.Path,
		)
	}
	return false
}
