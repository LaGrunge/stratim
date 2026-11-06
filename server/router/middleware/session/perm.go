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
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// PermNeedsSync tells whether the stored permission of a user on a repo is
// to be synced from the repo's forge before it is used: it is older than
// PermSyncInterval. Admins, guests and users of another forge are never
// synced: the token of a user of another forge must not be sent to it.
func PermNeedsSync(user *model.User, repo *model.Repo, perm *model.Perm, now time.Time) bool {
	return user != nil && !user.Admin && user.ForgeID == repo.ForgeID &&
		time.Unix(perm.Synced, 0).Add(PermSyncInterval).Before(now)
}

// EffectivePerm is what a user may do on a repo given their stored
// permission: an admin everything; anyone pull a public repo, and a logged
// in user an internal one. The perm given may be nil; it is not changed.
func EffectivePerm(user *model.User, repo *model.Repo, perm *model.Perm) *model.Perm {
	effective := new(model.Perm)
	if perm != nil {
		*effective = *perm
	}
	if user != nil && user.Admin {
		effective.Pull = true
		effective.Push = true
		effective.Admin = true
	}
	if repo.Visibility == model.VisibilityPublic || (repo.Visibility == model.VisibilityInternal && user != nil) {
		effective.Pull = true
	}
	return effective
}

// CanRead tells whether a user may read a repo, its pipelines and their
// events, given their stored permission.
func CanRead(user *model.User, repo *model.Repo, perm *model.Perm) bool {
	return EffectivePerm(user, repo, perm).Pull
}

// CanReadStored tells whether a user may read a repo from the permission
// stored, without asking the forge: a permission older than
// PermSyncInterval counts as LoadPerm counts it when the forge cannot
// confirm it, which keeps pull. No stored permission is none.
func CanReadStored(s store.Store, user *model.User, repo *model.Repo) (bool, error) {
	if CanRead(user, repo, nil) { // an admin, or a repo anyone may read
		return true, nil
	}
	if user == nil {
		return false, nil
	}
	perm, err := s.PermFind(user, repo)
	switch {
	case errors.Is(err, types.ErrRecordNotExist):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("find the permission of %s on %s: %w", user.Login, repo.FullName, err)
	}
	return CanRead(user, repo, perm), nil
}

// LoadPerm gives the stored permission of a user on a repo, synced from the
// repo's forge first when PermNeedsSync says so. When the forge cannot
// confirm it, a permission to push or administer is cut to read only, and
// it is asked again next time. The forge is got only to sync. Of an admin
// or a guest, it gives an empty permission: EffectivePerm completes it.
func LoadPerm(ctx context.Context, s store.Store, getForge func() (forge.Forge, error), user *model.User, repo *model.Repo, now time.Time) (*model.Perm, error) {
	if user == nil || user.Admin {
		return new(model.Perm), nil
	}

	perm, err := s.PermFind(user, repo)
	if err != nil {
		log.Error().Err(err).Msgf("error fetching permission for %s %s", user.Login, repo.FullName)
	}
	if perm == nil {
		perm = new(model.Perm)
	}
	if !PermNeedsSync(user, repo, perm, now) {
		return perm, nil
	}

	var synced *model.Repo
	_forge, err := getForge()
	if err == nil {
		synced, err = _forge.Repo(ctx, user, repo.ForgeRemoteID, repo.Owner, repo.Name)
	}
	switch {
	case err == nil:
		log.Debug().Msgf("synced user permission for %s %s", user.Login, repo.FullName)
		perm = synced.Perm
		if perm == nil {
			perm = new(model.Perm)
		}
		perm.RepoID = repo.ID
		perm.UserID = user.ID
		perm.Synced = now.Unix()
		if err := s.PermUpsert(perm); err != nil {
			return nil, fmt.Errorf("store the permission of %s on %s: %w", user.Login, repo.FullName, err)
		}
	case perm.Push || perm.Admin:
		// Synced stays old, so the next request asks the forge again
		log.Warn().Err(err).Msgf("could not sync user permission for %s %s, falling back to read only",
			user.Login, repo.FullName)
		perm.Push = false
		perm.Admin = false
		perm.RepoID = repo.ID
		perm.UserID = user.ID
		if err := s.PermUpsert(perm); err != nil {
			return nil, fmt.Errorf("store the permission of %s on %s: %w", user.Login, repo.FullName, err)
		}
	}
	return perm, nil
}
