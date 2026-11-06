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

package queue

import (
	"cmp"
	"slices"
	"strconv"
	"sync"
	"time"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// stamps hands out unique, increasing stamps: unix nanoseconds, one more
// than the last if the clock did not move on. They stay increasing over a
// restart as long as the clock does.
var stamps struct {
	sync.Mutex
	last int64
}

func nextStamp() int64 {
	stamps.Lock()
	defer stamps.Unlock()
	stamps.last = max(time.Now().UnixNano(), stamps.last+1)
	return stamps.last
}

// seedStamps makes the stamps handed out from now on higher than last, a
// stamp handed out before, as by another server or a clock that went back.
func seedStamps(last int64) {
	stamps.Lock()
	defer stamps.Unlock()
	stamps.last = max(stamps.last, last)
}

// NextBoost returns the boost of a move to the top of the queue: higher than
// that of any move before it.
func NextBoost() int64 {
	return nextStamp()
}

// stampQueued sets when the tasks not stamped yet were queued, in their
// order.
func stampQueued(tasks []*model.Task) {
	for _, task := range tasks {
		if task.Queued == 0 {
			task.Queued = nextStamp()
		}
	}
}

// compareQueued orders tasks as they were queued: by their stamp, then, for
// tasks stored before it was set, by their pipeline's creation, the pipeline
// and the workflow's id.
func compareQueued(a, b *model.Task) int {
	if c := cmp.Compare(a.Queued, b.Queued); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Created, b.Created); c != 0 {
		return c
	}
	if c := cmp.Compare(a.PipelineID, b.PipelineID); c != 0 {
		return c
	}
	aID, aErr := strconv.ParseInt(a.ID, 10, 64)
	bID, bErr := strconv.ParseInt(b.ID, 10, 64)
	if aErr != nil || bErr != nil {
		return cmp.Compare(a.ID, b.ID)
	}
	return cmp.Compare(aID, bID)
}

// orderPending puts pending tasks in the queue's order and reports whether
// that changed their order:
//
//  1. a task an admin moved to the top first, the latest moved first (Boost),
//     then the others; within each, in the order they were queued
//     (compareQueued). This is a plain sort on a total order.
//  2. then, within each of these, the tasks of one repository are reordered
//     by their priority, the higher first, ties in the order queued: they
//     take the places the repository's tasks hold, among them, so the
//     places of other repositories' tasks do not change. A priority orders
//     the tasks of its repository, not of others.
//
// The second step is not a comparison of two tasks (which of two tasks of
// different repositories comes first depends on the other tasks), so it is
// not a sort; it is a permutation within each repository, computed from the
// order of step 1 only, so the result does not depend on the order the tasks
// came in. Each task moved takes the stamp of the place it takes (the
// repository's tasks exchange their stamps), so the order of step 1 is the
// final order: ordering them again changes nothing, and a task handed out
// takes its place with it. A repository has as many places ahead of another
// repository's task as it had tasks queued before it, not more.
func orderPending(tasks []*model.Task) bool {
	ordered := slices.Clone(tasks)
	slices.SortStableFunc(ordered, func(a, b *model.Task) int {
		if c := cmp.Compare(b.Boost, a.Boost); c != 0 {
			return c
		}
		return compareQueued(a, b)
	})

	type group struct {
		boost int64
		repo  int64
	}
	places := make(map[group][]int)
	prioritized := make(map[group]bool)
	for i, task := range ordered {
		g := group{task.Boost, task.RepoID}
		places[g] = append(places[g], i)
		if task.Priority != 0 {
			prioritized[g] = true
		}
	}
	for g := range prioritized {
		members := make([]*model.Task, 0, len(places[g]))
		queued := make([]int64, 0, len(places[g]))
		for _, i := range places[g] {
			members = append(members, ordered[i])
			queued = append(queued, ordered[i].Queued)
		}
		slices.SortStableFunc(members, func(a, b *model.Task) int {
			return cmp.Compare(b.Priority, a.Priority)
		})
		for j, i := range places[g] {
			ordered[i] = members[j]
			// the task takes the place's stamp: once handed out, it takes its
			// place away with it, and the repository's others keep theirs, so
			// that one low task cannot keep a place ahead of other
			// repositories for ever more tasks of a higher priority
			members[j].Queued = queued[j]
		}
	}

	if slices.Equal(ordered, tasks) {
		return false
	}
	copy(tasks, ordered)
	return true
}
