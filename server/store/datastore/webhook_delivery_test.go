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

package datastore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func newDelivery(forgeID int64, guid string, nextTry int64) *model.WebhookDelivery {
	return &model.WebhookDelivery{
		ForgeID:  forgeID,
		GUID:     guid,
		RepoID:   7,
		Event:    "push",
		Header:   map[string][]string{"X-Github-Event": {"push"}},
		Body:     []byte("gzipped"),
		State:    model.DeliveryPending,
		NextTry:  nextTry,
		Received: nextTry,
		Updated:  nextTry,
	}
}

func TestWebhookDeliveryCreateOnce(t *testing.T) {
	store, closer := newTestStore(t, new(model.WebhookDelivery))
	defer closer()

	first := newDelivery(1, "guid-1", 100)
	created, err := store.WebhookDeliveryCreate(first)
	require.NoError(t, err)
	assert.True(t, created)

	// the forge delivering it again
	created, err = store.WebhookDeliveryCreate(newDelivery(1, "guid-1", 200))
	require.NoError(t, err)
	assert.False(t, created, "a delivery again is taken once")

	// another forge's delivery of the same GUID is another
	created, err = store.WebhookDeliveryCreate(newDelivery(2, "guid-1", 100))
	require.NoError(t, err)
	assert.True(t, created)

	n, err := store.engine.Count(new(model.WebhookDelivery))
	require.NoError(t, err)
	assert.EqualValues(t, 2, n, "one row per delivery")

	got, err := store.WebhookDeliveryGet(first.ID)
	require.NoError(t, err)
	assert.Equal(t, first.Header, got.Header)
	assert.Equal(t, first.Body, got.Body)
	assert.EqualValues(t, 100, got.NextTry, "the first one stays")
}

func TestWebhookDeliveriesDueAndClaim(t *testing.T) {
	store, closer := newTestStore(t, new(model.WebhookDelivery))
	defer closer()

	due := newDelivery(1, "due", 100)
	later := newDelivery(1, "later", 300)
	expired := newDelivery(1, "expired", 50)
	expired.State = model.DeliveryProcessing
	claimed := newDelivery(1, "claimed", 500)
	claimed.State = model.DeliveryProcessing
	done := newDelivery(1, "done", 10)
	done.State = model.DeliveryDone
	for _, d := range []*model.WebhookDelivery{due, later, expired, claimed, done} {
		_, err := store.WebhookDeliveryCreate(d)
		require.NoError(t, err)
	}

	list, err := store.WebhookDeliveriesDue(200, 10)
	require.NoError(t, err)
	require.Len(t, list, 2)
	assert.Equal(t, []string{"due", "expired"}, []string{list[0].GUID, list[1].GUID}, "by ID")
	assert.Nil(t, list[0].Body, "listed without the body")
	assert.Nil(t, list[0].Header)

	list, err = store.WebhookDeliveriesDue(200, 1)
	require.NoError(t, err)
	assert.Len(t, list, 1, "at most limit")

	listed := *list[0]
	ok, err := store.WebhookDeliveryClaim(list[0], 260, 200)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, model.DeliveryProcessing, list[0].State)
	assert.Equal(t, 1, list[0].Attempts)

	// a claim on what was listed before fails: another took it
	ok, err = store.WebhookDeliveryClaim(&listed, 260, 200)
	require.NoError(t, err)
	assert.False(t, ok)

	got, err := store.WebhookDeliveryGet(due.ID)
	require.NoError(t, err)
	assert.Equal(t, model.DeliveryProcessing, got.State)
	assert.EqualValues(t, 260, got.NextTry)
	assert.Equal(t, 1, got.Attempts)

	got.State = model.DeliveryDone
	got.PipelineID = 42
	got.Error = "none"
	got.Updated = 210
	require.NoError(t, store.WebhookDeliveryUpdate(got))
	got, err = store.WebhookDeliveryGet(due.ID)
	require.NoError(t, err)
	assert.Equal(t, model.DeliveryDone, got.State)
	assert.EqualValues(t, 42, got.PipelineID)
	assert.Equal(t, "none", got.Error)
	assert.Equal(t, []byte("gzipped"), got.Body, "the body is not rewritten")
}

func TestWebhookDeliveriesKnownAndPurge(t *testing.T) {
	store, closer := newTestStore(t, new(model.WebhookDelivery))
	defer closer()

	for _, d := range []*model.WebhookDelivery{
		newDelivery(1, "a", 100), newDelivery(1, "b", 100), newDelivery(2, "c", 100),
	} {
		_, err := store.WebhookDeliveryCreate(d)
		require.NoError(t, err)
	}
	known, err := store.WebhookDeliveriesKnown(1, []string{"a", "c", "x"})
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, known)
	known, err = store.WebhookDeliveriesKnown(1, nil)
	require.NoError(t, err)
	assert.Empty(t, known)

	_, err = store.engine.Exec("UPDATE webhook_deliveries SET state = ?, updated = ? WHERE guid = ?", model.DeliveryDone, 50, "a")
	require.NoError(t, err)
	_, err = store.engine.Exec("UPDATE webhook_deliveries SET state = ?, updated = ? WHERE guid = ?", model.DeliveryFailed, 150, "c")
	require.NoError(t, err)

	// "b" is pending, "c" settled after
	n, err := store.WebhookDeliveriesPurge(100, 10)
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
	known, err = store.WebhookDeliveriesKnown(1, []string{"a", "b"})
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, known)
	n, err = store.WebhookDeliveriesPurge(100, 10)
	require.NoError(t, err)
	assert.Zero(t, n)
}
