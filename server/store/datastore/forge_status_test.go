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

func candidateIDs(t *testing.T, s *storage, since, before int64, limit int) []int64 {
	t.Helper()
	candidates, err := s.ForgeStatusCandidates(since, before, limit)
	require.NoError(t, err)
	ids := make([]int64, 0, len(candidates))
	for _, c := range candidates {
		ids = append(ids, c.WorkflowID)
	}
	return ids
}

func TestForgeStatusCandidates(t *testing.T) {
	store, closer := newTestStore(t, new(model.Pipeline), new(model.Workflow), new(model.Step), new(model.ForgeStatus))
	defer closer()

	old := &model.Pipeline{RepoID: 1, Number: 1, Status: model.StatusSuccess}
	recent := &model.Pipeline{RepoID: 1, Number: 2, Status: model.StatusFailure, Event: model.EventDeploy}
	for _, p := range []*model.Pipeline{old, recent} {
		_, err := store.engine.Insert(p)
		require.NoError(t, err)
	}
	// created is set by xorm on insert
	const since = 1_700_000_000
	_, err := store.engine.Exec("UPDATE pipelines SET created = ? WHERE id = ?", since-1, old.ID)
	require.NoError(t, err)
	_, err = store.engine.Exec("UPDATE pipelines SET created = ? WHERE id = ?", since, recent.ID)
	require.NoError(t, err)

	workflows := []*model.Workflow{
		{PipelineID: old.ID, PID: 1, Name: "old", State: model.StatusSuccess},
		{PipelineID: recent.ID, PID: 1, Name: "never-taken", State: model.StatusFailure},
		{PipelineID: recent.ID, PID: 2, Name: "taken", State: model.StatusSuccess},
	}
	require.NoError(t, store.WorkflowsCreate(workflows))
	never, taken := workflows[1], workflows[2]

	set := func(w *model.Workflow, value string) {
		require.NoError(t, store.ForgeStatusSet(&model.ForgeStatus{WorkflowID: w.ID, PipelineID: w.PipelineID, Value: value}))
	}
	set(taken, "running running")
	set(taken, "success failure") // replaces the one before
	set(taken, "success failure") // the same again

	assert.Equal(t, []int64{taken.ID, never.ID}, candidateIDs(t, store, since, 0, 100), "newest first; created at since is in, before it out")
	assert.Equal(t, []int64{taken.ID}, candidateIDs(t, store, since, 0, 1))
	assert.Equal(t, []int64{never.ID}, candidateIDs(t, store, since, taken.ID, 100), "below before")
	assert.Len(t, candidateIDs(t, store, since-1, 0, 100), 3, "old pipeline in")
	assert.Empty(t, candidateIDs(t, store, since+1, 0, 100), "no pipeline created since")

	got, err := store.ForgeStatusCandidates(since, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, []*model.ForgeStatusCandidate{
		{WorkflowID: taken.ID, PipelineID: recent.ID, State: model.StatusSuccess, PipelineEvent: model.EventDeploy, PipelineStatus: model.StatusFailure, Value: "success failure"},
		{WorkflowID: never.ID, PipelineID: recent.ID, State: model.StatusFailure, PipelineEvent: model.EventDeploy, PipelineStatus: model.StatusFailure},
	}, got)
	assert.False(t, got[0].Unsent())
	assert.True(t, got[1].Unsent())

	// not for a workflow that is not there
	require.NoError(t, store.ForgeStatusSet(&model.ForgeStatus{WorkflowID: 999999, PipelineID: recent.ID, Value: "success success"}))
	exist, err := store.engine.ID(int64(999999)).Exist(new(model.ForgeStatus))
	require.NoError(t, err)
	assert.False(t, exist)

	// deleting the workflows of a pipeline forgets their statuses
	require.NoError(t, store.WorkflowsReplace(recent, nil))
	count, err := store.engine.Count(new(model.ForgeStatus))
	require.NoError(t, err)
	assert.Zero(t, count)
}
