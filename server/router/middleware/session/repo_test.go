// Copyright 2026 Woodpecker Authors
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

//go:build test

package session

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

func TestSetPerm(t *testing.T) {
	s := datastore.NewTestStore(t)

	newCtx := func(user *model.User, repo *model.Repo) (*gin.Context, *httptest.ResponseRecorder) {
		gin.SetMode(gin.TestMode)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
		c.Set("store", s)
		if user != nil {
			c.Set("user", user)
		}
		c.Set("repo", repo)
		return c, rec
	}

	t.Run("stale perm of user on another forge is not synced via the repo's forge", func(t *testing.T) {
		mgr := manager_mocks.NewMockManager(t)
		// forge of the repo; no Repo() expectation: the mock fails the test
		// if the middleware sends the foreign user's token to this forge
		_forge := forge_mocks.NewMockForge(t)
		mgr.On("ForgeFromRepo", mock.Anything).Return(_forge, nil)
		server.Config.Services.Manager = mgr

		user := &model.User{ID: 10, Login: "alice", ForgeID: 1}
		repo := &model.Repo{ID: 20, FullName: "acme/spanner", ForgeID: 2, ForgeRemoteID: "7"}
		c, _ := newCtx(user, repo)

		SetPerm()(c)

		perm := Perm(c)
		require.NotNil(t, perm)
		assert.False(t, perm.Pull)
		assert.False(t, perm.Push)
		assert.False(t, perm.Admin)
	})

	t.Run("stale perm of user on the repo's forge is synced", func(t *testing.T) {
		mgr := manager_mocks.NewMockManager(t)
		_forge := forge_mocks.NewMockForge(t)
		mgr.On("ForgeFromRepo", mock.Anything).Return(_forge, nil)
		server.Config.Services.Manager = mgr

		user := &model.User{ID: 11, Login: "bob", ForgeID: 2}
		repo := &model.Repo{ID: 21, FullName: "acme/spanner", ForgeID: 2, ForgeRemoteID: "7"}

		_forge.On("Repo", mock.Anything, user, repo.ForgeRemoteID, repo.Owner, repo.Name).
			Return(&model.Repo{
				ForgeRemoteID: repo.ForgeRemoteID,
				ForgeID:       repo.ForgeID,
				Perm:          &model.Perm{Pull: true, Push: true},
			}, nil).Once()

		c, _ := newCtx(user, repo)

		SetPerm()(c)

		perm := Perm(c)
		require.NotNil(t, perm)
		assert.True(t, perm.Pull)
		assert.True(t, perm.Push)
		assert.EqualValues(t, repo.ID, perm.RepoID)
		assert.EqualValues(t, user.ID, perm.UserID)
		assert.InDelta(t, time.Now().Unix(), perm.Synced, 5)
	})

	t.Run("stale perm falls back to read only if the forge sync fails", func(t *testing.T) {
		mgr := manager_mocks.NewMockManager(t)
		_forge := forge_mocks.NewMockForge(t)
		mgr.On("ForgeFromRepo", mock.Anything).Return(_forge, nil)
		server.Config.Services.Manager = mgr

		user := &model.User{ID: 12, Login: "carol", ForgeID: 2}
		repo := &model.Repo{ID: 22, FullName: "acme/spanner", ForgeID: 2, ForgeRemoteID: "7"}
		synced := time.Now().Add(-2 * time.Hour).Unix()
		require.NoError(t, s.PermUpsert(&model.Perm{
			UserID: user.ID, RepoID: repo.ID, Pull: true, Push: true, Admin: true, Synced: synced,
		}))

		_forge.On("Repo", mock.Anything, user, repo.ForgeRemoteID, repo.Owner, repo.Name).
			Return(nil, errors.New("forge not reachable")).Once()

		c, _ := newCtx(user, repo)

		SetPerm()(c)

		perm := Perm(c)
		require.NotNil(t, perm)
		assert.True(t, perm.Pull)
		assert.False(t, perm.Push)
		assert.False(t, perm.Admin)

		stored, err := s.PermFind(user, repo)
		require.NoError(t, err)
		assert.True(t, stored.Pull)
		assert.False(t, stored.Push)
		assert.False(t, stored.Admin)
		// not marked as synced, so the next request asks the forge again
		assert.Equal(t, synced, stored.Synced)
	})

	t.Run("an admin may do everything without asking the forge", func(t *testing.T) {
		mgr := manager_mocks.NewMockManager(t)
		// no Repo() expectation: the mock fails the test if the forge is asked
		mgr.On("ForgeFromRepo", mock.Anything).Return(forge_mocks.NewMockForge(t), nil)
		server.Config.Services.Manager = mgr

		admin := &model.User{ID: 13, Login: "root", ForgeID: 2, Admin: true}
		repo := &model.Repo{ID: 23, FullName: "acme/spanner", ForgeID: 2, Visibility: model.VisibilityPrivate}
		c, _ := newCtx(admin, repo)

		SetPerm()(c)

		perm := Perm(c)
		require.NotNil(t, perm)
		assert.True(t, perm.Pull)
		assert.True(t, perm.Push)
		assert.True(t, perm.Admin)
	})

	t.Run("a stale admin perm falls back to read only if the forge sync fails", func(t *testing.T) {
		mgr := manager_mocks.NewMockManager(t)
		_forge := forge_mocks.NewMockForge(t)
		mgr.On("ForgeFromRepo", mock.Anything).Return(_forge, nil)
		server.Config.Services.Manager = mgr

		user := &model.User{ID: 14, Login: "dave", ForgeID: 2}
		repo := &model.Repo{ID: 24, FullName: "acme/spanner", ForgeID: 2, ForgeRemoteID: "7"}
		require.NoError(t, s.PermUpsert(&model.Perm{
			UserID: user.ID, RepoID: repo.ID, Pull: true, Admin: true, Synced: time.Now().Add(-2 * time.Hour).Unix(),
		}))
		_forge.On("Repo", mock.Anything, user, repo.ForgeRemoteID, repo.Owner, repo.Name).
			Return(nil, errors.New("forge not reachable")).Once()

		c, _ := newCtx(user, repo)
		SetPerm()(c)

		perm := Perm(c)
		require.NotNil(t, perm)
		assert.True(t, perm.Pull)
		assert.False(t, perm.Admin)
		stored, err := s.PermFind(user, repo)
		require.NoError(t, err)
		assert.False(t, stored.Admin, "the admin is dropped in the store too")
	})

	t.Run("a perm that cannot be stored fails the request", func(t *testing.T) {
		mgr := manager_mocks.NewMockManager(t)
		_forge := forge_mocks.NewMockForge(t)
		mgr.On("ForgeFromRepo", mock.Anything).Return(_forge, nil)
		server.Config.Services.Manager = mgr

		user := &model.User{ID: 15, Login: "erin", ForgeID: 2}
		repo := &model.Repo{ID: 25, FullName: "acme/spanner", ForgeID: 2, ForgeRemoteID: "7"}
		failing := store_mocks.NewMockStore(t)
		failing.On("PermFind", user, repo).Return(&model.Perm{Pull: true}, nil).Once()
		failing.On("PermUpsert", mock.Anything).Return(errors.New("database is down")).Once()
		_forge.On("Repo", mock.Anything, user, repo.ForgeRemoteID, repo.Owner, repo.Name).
			Return(&model.Repo{Perm: &model.Perm{Pull: true}}, nil).Once()

		c, rec := newCtx(user, repo)
		c.Set("store", failing)
		SetPerm()(c)

		assert.True(t, c.IsAborted())
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}

// TestLoadPerm checks the sync of a stale permission on its own.
func TestLoadPerm(t *testing.T) {
	s := datastore.NewTestStore(t)
	now := time.Now()
	stale := now.Add(-2 * PermSyncInterval).Unix()
	user := &model.User{ID: 1, Login: "alice", ForgeID: 1}
	noForge := func() (forge.Forge, error) { return nil, errors.New("no forge") }

	t.Run("a forge that cannot be got keeps pull, drops push and stays unsynced", func(t *testing.T) {
		repo := &model.Repo{ID: 1, FullName: "acme/a", ForgeID: 1}
		require.NoError(t, s.PermUpsert(&model.Perm{UserID: user.ID, RepoID: repo.ID, Pull: true, Push: true, Synced: stale}))

		perm, err := LoadPerm(t.Context(), s, noForge, user, repo, now)
		require.NoError(t, err)
		assert.True(t, perm.Pull)
		assert.False(t, perm.Push)

		stored, err := s.PermFind(user, repo)
		require.NoError(t, err)
		assert.True(t, stored.Pull)
		assert.False(t, stored.Push)
		assert.Equal(t, stale, stored.Synced, "asked again next time")
	})

	t.Run("a forge that gives no perm is none, synced now", func(t *testing.T) {
		repo := &model.Repo{ID: 2, FullName: "acme/b", ForgeID: 1, ForgeRemoteID: "2"}
		require.NoError(t, s.PermUpsert(&model.Perm{UserID: user.ID, RepoID: repo.ID, Pull: true, Synced: stale}))
		_forge := forge_mocks.NewMockForge(t)
		_forge.On("Repo", mock.Anything, user, repo.ForgeRemoteID, repo.Owner, repo.Name).Return(&model.Repo{}, nil).Once()

		perm, err := LoadPerm(t.Context(), s, func() (forge.Forge, error) { return _forge, nil }, user, repo, now)
		require.NoError(t, err)
		assert.False(t, CanRead(user, repo, perm))

		stored, err := s.PermFind(user, repo)
		require.NoError(t, err)
		assert.False(t, stored.Pull)
		assert.Equal(t, now.Unix(), stored.Synced)
	})

	t.Run("of an admin, nothing is looked up", func(t *testing.T) {
		perm, err := LoadPerm(t.Context(), store_mocks.NewMockStore(t), noForge, &model.User{ID: 2, Admin: true}, &model.Repo{ID: 4}, now)
		require.NoError(t, err)
		assert.Equal(t, &model.Perm{}, perm)
	})

	t.Run("a perm that cannot be found is none", func(t *testing.T) {
		repo := &model.Repo{ID: 5, FullName: "acme/e", ForgeID: 1}
		failing := store_mocks.NewMockStore(t)
		failing.On("PermFind", user, repo).Return(nil, errors.New("database is down")).Once()

		perm, err := LoadPerm(t.Context(), failing, noForge, user, repo, now)
		require.NoError(t, err)
		assert.False(t, CanRead(user, repo, perm))
	})

	t.Run("a fallback that cannot be stored is an error", func(t *testing.T) {
		repo := &model.Repo{ID: 3, FullName: "acme/c", ForgeID: 1}
		failing := store_mocks.NewMockStore(t)
		failing.On("PermFind", user, repo).Return(&model.Perm{Pull: true, Push: true, Synced: stale}, nil).Once()
		failing.On("PermUpsert", mock.Anything).Return(errors.New("database is down")).Once()

		_, err := LoadPerm(t.Context(), failing, noForge, user, repo, now)
		assert.Error(t, err)
	})
}

func TestCanReadStored(t *testing.T) {
	s := datastore.NewTestStore(t)
	user := &model.User{ID: 1, Login: "alice", ForgeID: 1}
	private := &model.Repo{ID: 1, FullName: "acme/a", Visibility: model.VisibilityPrivate}
	pulled := &model.Repo{ID: 2, FullName: "acme/b", Visibility: model.VisibilityPrivate}
	require.NoError(t, s.PermUpsert(&model.Perm{UserID: user.ID, RepoID: pulled.ID, Pull: true, Synced: 1}))

	for _, tt := range []struct {
		name string
		user *model.User
		repo *model.Repo
		want bool
	}{
		{"no stored perm", user, private, false},
		{"a stale perm with pull keeps it", user, pulled, true},
		{"a guest a private repo", nil, pulled, false},
		{"an admin", &model.User{ID: 2, Admin: true}, private, true},
		{"anyone a public repo", nil, &model.Repo{ID: 3, Visibility: model.VisibilityPublic}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CanReadStored(s, tt.user, tt.repo)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	failing := store_mocks.NewMockStore(t)
	failing.On("PermFind", user, private).Return(nil, errors.New("database is down")).Once()
	_, err := CanReadStored(failing, user, private)
	assert.Error(t, err)
}
