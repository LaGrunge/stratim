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

//go:build test

package webhook

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
)

// redeliveringForge is a fake forge that keeps the deliveries of its
// webhooks.
type redeliveringForge struct {
	*forge_mocks.MockForge
	*forge_mocks.MockHookRedeliverer
}

func TestReconcile(t *testing.T) {
	s := datastore.NewTestStore(t)
	user := &model.User{ForgeID: 1, ForgeRemoteID: "1", Login: "owner", Hash: "hash"}
	require.NoError(t, s.CreateUser(user))
	newRepo := func(remote string, active bool) *model.Repo {
		r := &model.Repo{
			ForgeID: 1, ForgeRemoteID: model.ForgeRemoteID(remote), Owner: "owner", Name: remote, FullName: "owner/" + remote,
			IsActive: active, UserID: user.ID, WebhookSecret: "secret",
		}
		require.NoError(t, s.CreateRepo(r))
		return r
	}
	keeping, plain := newRepo("keeping", true), newRepo("plain", true)
	// not reconciled: the manager fails a call for its forge
	newRepo("inactive", false)

	redeliverer := forge_mocks.NewMockHookRedeliverer(t)
	keeper := &redeliveringForge{MockForge: forge_mocks.NewMockForge(t), MockHookRedeliverer: redeliverer}
	m := manager_mocks.NewMockManager(t)
	m.On("ForgeFromRepo", mock.MatchedBy(func(r *model.Repo) bool { return r.ID == keeping.ID })).Return(keeper, nil)
	// a forge that does not keep them is not asked
	m.On("ForgeFromRepo", mock.MatchedBy(func(r *model.Repo) bool { return r.ID == plain.ID })).Return(forge_mocks.NewMockForge(t), nil)
	orig := server.Config.Services.Manager
	server.Config.Services.Manager = m
	t.Cleanup(func() { server.Config.Services.Manager = orig })

	// one the inbox took
	_, err := s.WebhookDeliveryCreate(&model.WebhookDelivery{ForgeID: 1, GUID: "known", RepoID: keeping.ID, State: model.DeliveryDone})
	require.NoError(t, err)

	now := time.Unix(1_800_000_000, 0)
	at := func(minutes int) time.Time { return now.Add(time.Duration(minutes) * time.Minute) }
	attempt := func(id, guid string, minutes, status int) *types.HookDelivery {
		return &types.HookDelivery{ID: id, GUID: guid, Event: "push", DeliveredAt: at(minutes), StatusCode: status}
	}
	redeliverer.EXPECT().HookDeliveries(mock.Anything, mock.Anything, mock.Anything, "https://ci.example.com", now.Add(-reconcileWindow)).
		Return([]*types.HookDelivery{
			// newest first, as GitHub lists them
			attempt("8", "stubborn", -2, 502),
			attempt("7", "missed", -3, 0),
			attempt("6", "known", -4, 0),
			attempt("5", "reached", -5, 202),
			attempt("4", "missed", -6, 502),
			attempt("3", "stubborn", -7, 502),
			attempt("2", "stubborn", -8, 502),
			attempt("1", "stubborn", -9, 502),
			attempt("0", "first", -10, 0),
			attempt("-1", "reached", -11, 502),
			attempt("-2", "refused", -12, 0),
		}, nil).Once()
	var asked []string
	redeliverer.EXPECT().RedeliverHook(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, u *model.User, r *model.Repo, d *types.HookDelivery) error {
			assert.Equal(t, user.ID, u.ID)
			assert.Equal(t, keeping.ID, r.ID)
			asked = append(asked, d.ID)
			if d.GUID == "refused" {
				return errors.New("forbidden")
			}
			return nil
		})

	r := NewReconciler(s, ReconcilerOptions{Link: "https://ci.example.com", Now: func() time.Time { return now }})
	assert.Equal(t, 2, r.Once(t.Context()), "the refused one is not counted")
	assert.Equal(t, []string{"-2", "0", "7"}, asked, "the latest attempt of each, oldest first")
}

func TestUnreached(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	got := unreached([]*types.HookDelivery{
		{ID: "1", GUID: "a", DeliveredAt: now, StatusCode: 500},
		{ID: "2", GUID: "", DeliveredAt: now, StatusCode: 0},
		{ID: "3", GUID: "a", DeliveredAt: now.Add(time.Second), StatusCode: 0},
		{ID: "4", GUID: "b", DeliveredAt: now, StatusCode: 204},
	})
	require.Len(t, got, 1)
	assert.Equal(t, "3", got[0].ID)
	assert.Empty(t, unreached(nil))
}
