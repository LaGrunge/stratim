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

package summary

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

var testRepo = &model.Repo{ID: 7, FullName: "LaGrunge/stratim"}

// succeeded is a pipeline of the branch and event that succeeded after the
// minutes given.
func succeeded(number int64, branch string, event model.WebhookEvent, minutes int64) *model.Pipeline {
	return &model.Pipeline{
		ID: 100 + number, Number: number, Branch: branch, Event: event, Status: model.StatusSuccess,
		Started: t0, Finished: t0 + minutes*60,
	}
}

func running(branch string, event model.WebhookEvent) *model.Pipeline {
	return &model.Pipeline{ID: 999, Number: 99, Branch: branch, Event: event, Status: model.StatusRunning, Started: t0}
}

// expectLookup expects one look up of the latest successful push of main,
// which gives found.
func expectLookup(s *store_mocks.MockStore, found ...*model.Pipeline) {
	const branch, event = "main", model.EventPush
	s.On("GetPipelineList", testRepo,
		mock.MatchedBy(func(o *model.ListOptionsWithAll) bool { return !o.All && o.Page == 1 && o.PerPage == 1 }),
		mock.MatchedBy(func(f *model.PipelineFilter) bool {
			return f.Branch == branch && slices.Equal(f.Events, []model.WebhookEvent{event}) && f.Status == model.StatusSuccess
		}),
	).Return(found, nil).Once()
}

func TestDurationsLooksUpOnce(t *testing.T) {
	s := store_mocks.NewMockStore(t)
	d := NewDurations(0)
	expectLookup(s, succeeded(5, "main", model.EventPush, 35))

	assert.Equal(t, 35*time.Minute, d.Typical(s, testRepo, running("main", model.EventPush)))
	assert.Equal(t, 35*time.Minute, d.Typical(s, testRepo, running("main", model.EventPush)), "cached")
}

func TestDurationsNoneFoundIsKept(t *testing.T) {
	s := store_mocks.NewMockStore(t)
	d := NewDurations(0)
	expectLookup(s)

	assert.Zero(t, d.Typical(s, testRepo, running("main", model.EventPush)))
	assert.Zero(t, d.Typical(s, testRepo, running("main", model.EventPush)), "cached")
}

func TestDurationsWithoutALookUp(t *testing.T) {
	s := store_mocks.NewMockStore(t) // fails on any call
	d := NewDurations(0)
	assert.Zero(t, d.Typical(s, testRepo, running("", model.EventPush)), "no branch")
	finished := succeeded(9, "main", model.EventPush, 3)
	assert.Zero(t, d.Typical(s, testRepo, finished), "finished")

	var none *Durations
	assert.Zero(t, none.Typical(s, testRepo, running("main", model.EventPush)))
	none.Observe(testRepo, finished)
	typical, known := none.Cached(testRepo, running("main", model.EventPush))
	assert.True(t, known)
	assert.Zero(t, typical)
}

func TestDurationsLearnsFromObserved(t *testing.T) {
	s := store_mocks.NewMockStore(t)
	d := NewDurations(0)
	expectLookup(s, succeeded(5, "main", model.EventPush, 35))
	assert.Equal(t, 35*time.Minute, d.Typical(s, testRepo, running("main", model.EventPush)))

	d.Observe(testRepo, succeeded(6, "main", model.EventPush, 20))
	assert.Equal(t, 20*time.Minute, d.Typical(s, testRepo, running("main", model.EventPush)), "a newer one")
	d.Observe(testRepo, succeeded(4, "main", model.EventPush, 50))
	assert.Equal(t, 20*time.Minute, d.Typical(s, testRepo, running("main", model.EventPush)), "not an older one")

	failed := succeeded(7, "main", model.EventPush, 1)
	failed.Status = model.StatusFailure
	d.Observe(testRepo, failed)
	assert.Equal(t, 20*time.Minute, d.Typical(s, testRepo, running("main", model.EventPush)), "not a failed one")
}

func TestDurationsOtherBranchOrEventIsNoBaseline(t *testing.T) {
	s := store_mocks.NewMockStore(t)
	d := NewDurations(0)
	d.Observe(testRepo, succeeded(5, "dev", model.EventPush, 35))
	d.Observe(testRepo, succeeded(6, "main", model.EventPull, 35))

	expectLookup(s)
	assert.Zero(t, d.Typical(s, testRepo, running("main", model.EventPush)))
	assert.Equal(t, 35*time.Minute, d.Typical(s, testRepo, running("dev", model.EventPush)))
	assert.Equal(t, 35*time.Minute, d.Typical(s, testRepo, running("main", model.EventPull)))
}

func TestDurationsExpire(t *testing.T) {
	s := store_mocks.NewMockStore(t)
	d := NewDurations(0)
	now := time.Unix(t0, 0)
	d.now = func() time.Time { return now }
	expectLookup(s, succeeded(5, "main", model.EventPush, 35))
	assert.Equal(t, 35*time.Minute, d.Typical(s, testRepo, running("main", model.EventPush)))

	now = now.Add(durationTTL + time.Second)
	_, known := d.Cached(testRepo, running("main", model.EventPush))
	assert.False(t, known)
	expectLookup(s, succeeded(8, "main", model.EventPush, 10))
	assert.Equal(t, 10*time.Minute, d.Typical(s, testRepo, running("main", model.EventPush)), "looked up again")
}

func TestDurationsAreBounded(t *testing.T) {
	d := NewDurations(2)
	d.Observe(testRepo, succeeded(1, "a", model.EventPush, 1))
	d.Observe(testRepo, succeeded(2, "b", model.EventPush, 2))
	d.Observe(testRepo, succeeded(3, "c", model.EventPush, 3))
	assert.LessOrEqual(t, len(d.entries), 2)
	typical, known := d.Cached(testRepo, running("c", model.EventPush))
	assert.True(t, known)
	assert.Equal(t, 3*time.Minute, typical)
}

func TestDurationsLookUpError(t *testing.T) {
	s := store_mocks.NewMockStore(t)
	d := NewDurations(0)
	s.On("GetPipelineList", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("database is down")).Twice()
	assert.Zero(t, d.Typical(s, testRepo, running("main", model.EventPush)))
	assert.Zero(t, d.Typical(s, testRepo, running("main", model.EventPush)), "not kept: looked up again")
}
