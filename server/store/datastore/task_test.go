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

package datastore

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func TestTaskList(t *testing.T) {
	store, closer := newTestStore(t, new(model.Task))
	defer closer()

	assert.NoError(t, store.TaskInsert(&model.Task{
		ID:        "some_random_id",
		Data:      []byte("foo"),
		Labels:    map[string]string{"foo": "bar"},
		DepStatus: map[string]model.StatusValue{"test": "dep"},
	}))

	list, err := store.TaskList()
	assert.NoError(t, err)
	assert.Len(t, list, 1, "Expected one task in list")
	assert.Equal(t, "some_random_id", list[0].ID)
	assert.Equal(t, "foo", string(list[0].Data))
	assert.EqualValues(t, map[string]model.StatusValue{"test": "dep"}, list[0].DepStatus)

	assert.NoError(t, store.TaskDelete("some_random_id"))

	list, err = store.TaskList()
	assert.NoError(t, err)
	assert.Len(t, list, 0, "Want empty task list after delete")
}

// TaskSetBoost writes the boost of the stored tasks only, and only a higher
// one: a deleted task stays deleted, a later move is not undone.
func TestTaskSetBoost(t *testing.T) {
	store, closer := newTestStore(t, new(model.Task))
	defer closer()

	for _, task := range []*model.Task{
		{ID: "a", Priority: 3},
		{ID: "b"},
		{ID: "c", Priority: -2},
	} {
		assert.NoError(t, store.TaskInsert(task))
	}
	boosts := func() map[string][2]int64 {
		list, err := store.TaskList()
		assert.NoError(t, err)
		got := make(map[string][2]int64, len(list))
		for _, task := range list {
			got[task.ID] = [2]int64{int64(task.Priority), task.Boost}
		}
		return got
	}

	assert.NoError(t, store.TaskSetBoost(nil, 1))
	assert.NoError(t, store.TaskSetBoost([]string{"a", "b", "missing"}, 5))
	assert.Equal(t, map[string][2]int64{"a": {3, 5}, "b": {0, 5}, "c": {-2, 0}}, boosts())

	assert.NoError(t, store.TaskSetBoost([]string{"a"}, 4), "an earlier move written late")
	assert.Equal(t, map[string][2]int64{"a": {3, 5}, "b": {0, 5}, "c": {-2, 0}}, boosts())

	assert.NoError(t, store.TaskDelete("b"))
	assert.NoError(t, store.TaskSetBoost([]string{"b", "c"}, 6))
	assert.Equal(t, map[string][2]int64{"a": {3, 5}, "c": {-2, 6}}, boosts())
}

// TaskSetQueued sets the stamp of a stored task that has none, only.
func TestTaskSetQueued(t *testing.T) {
	store, closer := newTestStore(t, new(model.Task))
	defer closer()

	assert.NoError(t, store.TaskInsert(&model.Task{ID: "legacy"}))
	assert.NoError(t, store.TaskInsert(&model.Task{ID: "stamped", Queued: 7}))
	assert.NoError(t, store.TaskSetQueued("legacy", 3))
	assert.NoError(t, store.TaskSetQueued("stamped", 3))
	assert.NoError(t, store.TaskSetQueued("legacy", 4), "set already")
	assert.NoError(t, store.TaskSetQueued("missing", 5))

	list, err := store.TaskList()
	assert.NoError(t, err)
	got := make(map[string]int64, len(list))
	for _, task := range list {
		got[task.ID] = task.Queued
	}
	assert.Equal(t, map[string]int64{"legacy": 3, "stamped": 7}, got)
}

// TaskSetBoost and TaskDelete at once, as a move and a cancel or a poll: the
// deleted tasks stay deleted, the others get the highest boost.
func TestTaskSetBoostConcurrentDelete(t *testing.T) {
	store, closer := newTestStore(t, new(model.Task))
	defer closer()

	const n = 20
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
		assert.NoError(t, store.TaskInsert(&model.Task{ID: ids[i]}))
	}

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			if i%2 == 0 {
				assert.NoError(t, store.TaskDelete(ids[i]))
				return
			}
			assert.NoError(t, store.TaskSetBoost(ids, int64(i)))
		})
	}
	wg.Wait()

	list, err := store.TaskList()
	assert.NoError(t, err)
	assert.Len(t, list, n/2)
	for _, task := range list {
		var id int
		_, err := fmt.Sscan(task.ID, &id)
		assert.NoError(t, err)
		assert.Equal(t, 1, id%2, "task %s was deleted", task.ID)
		assert.Equal(t, int64(n-1), task.Boost, "the highest boost stays")
	}
}
