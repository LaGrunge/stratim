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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func TestCanRead(t *testing.T) {
	user := &model.User{ID: 1}
	admin := &model.User{ID: 2, Admin: true}
	private := &model.Repo{Visibility: model.VisibilityPrivate}
	internal := &model.Repo{Visibility: model.VisibilityInternal}
	public := &model.Repo{Visibility: model.VisibilityPublic}
	pull := &model.Perm{Pull: true}

	for _, tt := range []struct {
		name string
		user *model.User
		repo *model.Repo
		perm *model.Perm
		want bool
	}{
		{"a guest a public repo", nil, public, nil, true},
		{"a guest an internal repo", nil, internal, nil, false},
		{"a user an internal repo", user, internal, nil, true},
		{"a user a private repo without a permission", user, private, nil, false},
		{"a user a private repo with pull", user, private, pull, true},
		{"a user a private repo with a permission without pull", user, private, &model.Perm{Push: false}, false},
		{"an admin a private repo", admin, private, nil, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, CanRead(tt.user, tt.repo, tt.perm))
		})
	}

	perm := &model.Perm{}
	EffectivePerm(admin, private, perm)
	assert.Equal(t, &model.Perm{}, perm, "the permission given is not changed")
}

func TestPermNeedsSync(t *testing.T) {
	now := time.Now()
	user := &model.User{ForgeID: 1}
	repo := &model.Repo{ForgeID: 1}
	fresh := &model.Perm{Synced: now.Add(-PermSyncInterval / 2).Unix()}
	stale := &model.Perm{Synced: now.Add(-2 * PermSyncInterval).Unix()}

	assert.False(t, PermNeedsSync(user, repo, fresh, now))
	assert.True(t, PermNeedsSync(user, repo, stale, now))
	assert.False(t, PermNeedsSync(nil, repo, stale, now), "a guest")
	assert.False(t, PermNeedsSync(&model.User{ForgeID: 1, Admin: true}, repo, stale, now), "an admin")
	assert.False(t, PermNeedsSync(&model.User{ForgeID: 2}, repo, stale, now), "a user of another forge")
}
