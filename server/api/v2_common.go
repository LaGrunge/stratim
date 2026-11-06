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

// What the handlers of /api/v2 share: their errors and the access of their
// user to repos by the stored permissions.

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

var (
	// The user of a request and the repos they may read are looked up again
	// after v2AccessTTL.
	v2AccessTTL = time.Minute
	// A repo whose access could not be looked up stays denied for
	// v2AccessBackoff.
	v2AccessBackoff = 30 * time.Second
)

type httpError struct {
	code int
	msg  string
}

func (e *httpError) Error() string { return e.msg }

func badRequest(format string, args ...any) error {
	return &httpError{code: http.StatusBadRequest, msg: fmt.Sprintf(format, args...)}
}

var errNotFound = &httpError{code: http.StatusNotFound, msg: "not found"}

func notFoundOr(err error) error {
	if errors.Is(err, types.ErrRecordNotExist) {
		return errNotFound
	}
	return err
}

// errUserGone ends a request or stream whose user does not exist any more,
// or whose tokens were reset.
var errUserGone = &httpError{code: http.StatusUnauthorized, msg: "user not authorized"}

// repoAccess tells the repos the user of a request or stream may read, by
// session.CanReadStored, from the repos and permissions as they are now.
// What it found is trusted for v2AccessTTL; then the user and the repos
// are looked up again.
type repoAccess struct {
	store store.Store
	user  *model.User
	// hash is the user's hash when the request started; a new one, as a
	// reset of the user's tokens makes, ends it.
	hash string

	repos   map[int64]repoAccessEntry
	checked time.Time
}

type repoAccessEntry struct {
	allowed bool
	until   time.Time
}

func newRepoAccess(s store.Store, user *model.User) *repoAccess {
	return &repoAccess{store: s, user: user, hash: user.Hash, repos: make(map[int64]repoAccessEntry), checked: time.Now()}
}

// refresh looks the user up again once v2AccessTTL passed, and forgets
// the repos looked up. It returns errUserGone once the user is gone or has
// another hash.
func (a *repoAccess) refresh() error {
	if time.Since(a.checked) < v2AccessTTL {
		return nil
	}
	user, err := a.store.GetUser(a.user.ID)
	switch {
	case errors.Is(err, types.ErrRecordNotExist):
		return errUserGone
	case err == nil && user.Hash != a.hash:
		return errUserGone
	case err != nil:
		log.Error().Err(err).Str("user", a.user.Login).Msg("api v2: cannot look up the user")
		a.checked = time.Now().Add(v2AccessBackoff - v2AccessTTL) // again after the backoff
		return nil
	}
	a.user = user
	a.checked = time.Now()
	clear(a.repos)
	return nil
}

// allowed tells whether the user may read the repo with the id.
func (a *repoAccess) allowed(repoID int64) (bool, error) {
	if err := a.refresh(); err != nil {
		return false, err
	}
	if entry, ok := a.repos[repoID]; ok && time.Now().Before(entry.until) {
		return entry.allowed, nil
	}
	repo, err := a.store.GetRepo(repoID)
	if err != nil {
		if !errors.Is(err, types.ErrRecordNotExist) {
			log.Error().Err(err).Int64("repo", repoID).Msg("api v2: cannot look up a repo")
		}
		a.deny(repoID)
		return false, nil
	}
	return a.canRead(repo), nil
}

// canRead tells whether the user may read the repo, from the stored
// permission: a stream never asks the forge.
func (a *repoAccess) canRead(repo *model.Repo) bool {
	allowed, err := session.CanReadStored(a.store, a.user, repo)
	if err != nil {
		log.Error().Err(err).Int64("repo", repo.ID).Msg("api v2: cannot look up a permission")
		a.deny(repo.ID)
		return false
	}
	a.repos[repo.ID] = repoAccessEntry{allowed: allowed, until: a.checked.Add(v2AccessTTL)}
	return allowed
}

// cachedAllowed tells whether the user may read the repo by what was looked
// up and is still trusted, without a look up: false when nothing is.
func (a *repoAccess) cachedAllowed(repoID int64) bool {
	if time.Since(a.checked) >= v2AccessTTL {
		return false
	}
	entry, ok := a.repos[repoID]
	return ok && entry.allowed && time.Now().Before(entry.until)
}

// deny denies the repo for v2AccessBackoff.
func (a *repoAccess) deny(repoID int64) {
	a.repos[repoID] = repoAccessEntry{until: time.Now().Add(v2AccessBackoff)}
}

// findRepo finds a repo by id or full name the user may read.
func (a *repoAccess) findRepo(ref string) (*model.Repo, error) {
	if err := a.refresh(); err != nil {
		return nil, err
	}
	var repo *model.Repo
	var err error
	if id, parseErr := strconv.ParseInt(ref, 10, 64); parseErr == nil {
		repo, err = a.store.GetRepo(id)
	} else {
		repo, err = a.store.GetRepoName(ref)
	}
	if err != nil {
		return nil, notFoundOr(err)
	}
	if !a.canRead(repo) {
		return nil, errNotFound
	}
	return repo, nil
}
