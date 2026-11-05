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

package datastore

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/cronconfig"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// TestCronApplyConfig: the crons of a config are created, updated and
// deleted on the store in one transaction, only the columns the config owns,
// and only for a pipeline newer (by ID) than the one applied; crons made in
// the UI are left alone.
func TestCronApplyConfig(t *testing.T) {
	store, closer := newTestStore(t, new(model.Cron), new(model.Repo))
	defer closer()

	repo := &model.Repo{ID: 1, UserID: 7, ForgeRemoteID: "1", FullName: "o/r", Owner: "o", Name: "r"}
	require.NoError(t, store.CreateRepo(repo))
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	managed := func(name, schedule string) *model.Cron {
		next, err := model.NextAfter(schedule, "UTC", now)
		require.NoError(t, err)
		return &model.Cron{Name: name, Schedule: schedule, Timezone: "UTC", Workflow: "test", Managed: true, Enabled: true, NextExec: next.Unix()}
	}
	byName := func() map[string]*model.Cron {
		crons, err := store.CronList(repo, &model.ListOptionsWithAll{All: true})
		require.NoError(t, err)
		out := make(map[string]*model.Cron)
		for _, c := range crons {
			out[c.Name] = c
		}
		return out
	}
	reconcile := func(pipelineID int64, desired ...*model.Cron) ([]cronconfig.Change, bool) {
		changes, applied, err := cronconfig.Reconcile(store, repo, pipelineID, desired, now)
		require.NoError(t, err)
		return changes, applied
	}

	// a cron of the settings made before / was kept for the config's
	_, err := store.engine.Insert(&model.Cron{RepoID: 1, Name: "test/taken", Schedule: "@daily", Timezone: "UTC", Enabled: true})
	require.NoError(t, err)

	changes, applied := reconcile(10, managed("test/nightly", "0 3 * * *"), managed("test/taken", "@hourly"))
	require.True(t, applied)
	assert.ElementsMatch(t, []cronconfig.Change{{Name: "test/nightly", Action: cronconfig.Created}, {Name: "test/taken", Action: cronconfig.Skipped}}, changes)
	nightly := byName()["test/nightly"]
	require.NotNil(t, nightly)
	assert.True(t, nightly.Managed)
	assert.Equal(t, time.Date(2026, 11, 6, 3, 0, 0, 0, time.UTC).Unix(), nightly.NextExec)
	assert.False(t, byName()["test/taken"].Managed, "the UI's cron stays")

	// an older pipeline's config, finishing late, is not applied
	_, applied = reconcile(9)
	assert.False(t, applied)
	assert.Contains(t, byName(), "test/nightly")

	// a repo loaded before the apply and saved after (a hook, the settings)
	// does not write back the marker
	stale := *repo
	stale.CronsFrom = 0
	require.NoError(t, store.UpdateRepo(&stale))
	_, applied = reconcile(9)
	assert.False(t, applied, "the marker survives UpdateRepo")

	// the scheduler moves next_exec and a user disables it meanwhile: a new
	// variable in the config keeps both
	nightly.Enabled, nightly.NextExec = false, 12345
	require.NoError(t, store.CronUpdate(repo, nightly))
	changed := managed("test/nightly", "0 3 * * *")
	changed.Variables = map[string]string{"TIER": "full"}
	changes, applied = reconcile(11, changed)
	require.True(t, applied)
	assert.Equal(t, []cronconfig.Change{{Name: "test/nightly", Action: cronconfig.Updated}}, changes)
	nightly = byName()["test/nightly"]
	assert.Equal(t, map[string]string{"TIER": "full"}, nightly.Variables)
	assert.False(t, nightly.Enabled, "the user's disable stays")
	assert.EqualValues(t, 12345, nightly.NextExec, "the scheduler's next_exec stays")

	// enabling a managed cron saves enabled and next_exec only: what the
	// config owns stays as the store has it, even from an older copy
	toggled := *nightly
	toggled.Enabled, toggled.NextExec, toggled.Variables = true, 777, nil
	require.NoError(t, store.CronSetEnabled(repo, &toggled))
	got := byName()["test/nightly"]
	assert.True(t, got.Enabled)
	assert.EqualValues(t, 777, got.NextExec)
	assert.Equal(t, map[string]string{"TIER": "full"}, got.Variables)
	require.NoError(t, store.CronSetEnabled(repo, &toggled), "saving the same values is no error")
	assert.ErrorIs(t, store.CronSetEnabled(repo, byName()["test/taken"]), types.ErrRecordNotExist, "not for the UI's crons")

	// a next run worked out from a schedule the config changed meanwhile
	staleCron := *got
	staleCron.Schedule, staleCron.NextExec = "@hourly", 1
	assert.ErrorIs(t, store.CronSetEnabled(repo, &staleCron), types.ErrRecordNotExist)
	assert.EqualValues(t, 777, byName()["test/nightly"].NextExec)
	staleZone := *got
	staleZone.Timezone, staleZone.NextExec = "Europe/Berlin", 1
	assert.ErrorIs(t, store.CronSetEnabled(repo, &staleZone), types.ErrRecordNotExist, "nor of a timezone")
	assert.EqualValues(t, 777, byName()["test/nightly"].NextExec)

	// disabling writes no next run, whatever schedule it was read with
	staleCron.Enabled = false
	require.NoError(t, store.CronSetEnabled(repo, &staleCron))
	got = byName()["test/nightly"]
	assert.False(t, got.Enabled)
	assert.EqualValues(t, 777, got.NextExec)
	assert.Equal(t, "0 3 * * *", got.Schedule)

	// rescheduled: next_exec moves, enabled stays
	changes, applied = reconcile(12, managed("test/nightly", "0 4 * * *"))
	require.True(t, applied)
	require.Len(t, changes, 1)
	nightly = byName()["test/nightly"]
	assert.Equal(t, time.Date(2026, 11, 6, 4, 0, 0, 0, time.UTC).Unix(), nightly.NextExec)
	assert.False(t, nightly.Enabled)

	// gone from the config: deleted; the UI's crons stay
	changes, applied = reconcile(13)
	require.True(t, applied)
	assert.Equal(t, []cronconfig.Change{{Name: "test/nightly", Action: cronconfig.Deleted}}, changes)
	assert.NotContains(t, byName(), "test/nightly")
	assert.Contains(t, byName(), "test/taken")

	// a plan that fails changes nothing, the marker included
	_, err = store.CronApplyConfig(repo, 14, func([]*model.Cron) (*model.CronPlan, error) {
		return nil, errors.New("no plan")
	})
	require.Error(t, err)
	_, applied = reconcile(14, managed("test/again", "@daily"))
	assert.True(t, applied, "14 was not marked applied")
}
