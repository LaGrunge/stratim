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

package events

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

var hubStart = time.Unix(t0, 0)

func newTestHub(bufferSize int) *Hub {
	return NewHub(Options{BufferSize: bufferSize, Now: func() time.Time { return hubStart }})
}

// publishPipelines makes n pipelines that each give one event at Debug and
// above: pipeline.created at info.
func publishPipelines(h *Hub, n int) {
	for i := range int64(n) {
		p := pl(model.StatusPending, -1, -1)
		p.ID = 1 + i
		h.Observe(testRepo, p)
	}
}

func ids(records []Record) []int64 {
	out := make([]int64, 0, len(records))
	for _, r := range records {
		out = append(out, r.Event.ID)
	}
	return out
}

func idRange(from, to int64) []int64 {
	var out []int64
	for id := from; id <= to; id++ {
		out = append(out, id)
	}
	return out
}

func TestHubIDsStartAtTheMicrosecondOfStart(t *testing.T) {
	h := newTestHub(10)
	first := hubStart.UnixMicro()
	publishPipelines(h, 3)
	_, start := h.Subscribe(Filter{}, Cursor{ID: 0, Set: true})
	assert.Equal(t, idRange(first, first+2), ids(start.Replay))
	assert.Equal(t, first+2, start.Head)
	assert.Equal(t, first, start.Oldest)

	var e Event
	require.NoError(t, json.Unmarshal(start.Replay[0].Data, &e))
	assert.Equal(t, first, e.ID, "the id is in the JSON too")
}

func TestHubReplay(t *testing.T) {
	h := newTestHub(5)
	first := hubStart.UnixMicro()

	_, start := h.Subscribe(Filter{}, Cursor{ID: first - 1, Set: true})
	assert.Empty(t, start.Replay, "nothing yet")
	assert.Empty(t, start.Reset, "the head of an empty hub is a cursor to replay from")

	publishPipelines(h, 8) // the buffer keeps the last 5: first+3 .. first+7

	tests := []struct {
		name   string
		cursor Cursor
		replay []int64
		reset  ResetReason
	}{
		{"no cursor is live only", Cursor{}, nil, ""},
		{"zero is every event kept", Cursor{ID: 0, Set: true}, idRange(first+3, first+7), ""},
		{"after a kept one", Cursor{ID: first + 5, Set: true}, idRange(first+6, first+7), ""},
		{"right before the oldest", Cursor{ID: first + 2, Set: true}, idRange(first+3, first+7), ""},
		{"at the head", Cursor{ID: first + 7, Set: true}, nil, ""},
		{"older than the oldest kept", Cursor{ID: first + 1, Set: true}, nil, ResetExpired},
		{"from before the start", Cursor{ID: first - 2, Set: true}, nil, ResetRestart},
		{"from after the head", Cursor{ID: first + 8, Set: true}, nil, ResetInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub, start := h.Subscribe(Filter{}, tt.cursor)
			defer sub.Close()
			assert.Equal(t, tt.replay, nilIfEmpty(ids(start.Replay)))
			assert.Equal(t, tt.reset, start.Reset)
			assert.Equal(t, first+7, start.Head)
			assert.Equal(t, first+3, start.Oldest)
		})
	}
}

func nilIfEmpty(s []int64) []int64 {
	if len(s) == 0 {
		return nil
	}
	return s
}

func TestHubFilters(t *testing.T) {
	h := newTestHub(100)
	other := &model.Repo{ID: 8, FullName: "LaGrunge/other", Visibility: model.VisibilityPrivate}
	steps := []*model.Pipeline{
		pl(model.StatusPending, -1, -1, wf(1, "build", model.StatusPending, -1, -1, st(11, "test", model.StatusPending))),
		pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1, st(11, "test", model.StatusRunning, times(6, -1)))),
		pl(model.StatusFailure, 5, 9, wf(1, "build", model.StatusFailure, 5, 9, st(11, "test", model.StatusFailure, times(6, 8), exit(1)))),
	}
	for _, p := range steps {
		h.Observe(testRepo, p)
		q := *p
		q.ID, q.Branch = 900, "dev"
		h.Observe(other, &q)
	}

	types := func(f Filter) []Type {
		sub, start := h.Subscribe(f, Cursor{ID: 0, Set: true})
		sub.Close()
		var out []Type
		for _, r := range start.Replay {
			out = append(out, r.Event.Type)
		}
		return out
	}

	assert.Equal(t, []Type{PipelineStarted, StepFailed, WorkflowFinished, PipelineFinished},
		types(Filter{MinSeverity: Notice, RepoIDs: []int64{testRepo.ID}}))
	assert.Equal(t, []Type{StepFailed, WorkflowFinished, PipelineFinished},
		types(Filter{MinSeverity: Error, Branch: "dev"}))
	assert.Equal(t, []Type{StepFailed},
		types(Filter{PipelineID: 900, Types: []Type{StepFailed, StepStarted}, MinSeverity: Info}))
	assert.Len(t, types(Filter{}), 14, "two pipelines of 7 events")
}

// TestHubSubscribeWhilePublishing checks that the events published while a
// subscription starts are each either replayed or delivered, once.
func TestHubSubscribeWhilePublishing(t *testing.T) {
	h := NewHub(Options{BufferSize: 100000, QueueLimit: 100000})
	const n = 2000

	var wg sync.WaitGroup
	wg.Go(func() { publishPipelines(h, n) })

	sub, start := h.Subscribe(Filter{}, Cursor{ID: 0, Set: true})
	defer sub.Close()
	got := ids(start.Replay)
	wg.Wait()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for len(got) < n {
		r, err := sub.Next(ctx)
		require.NoError(t, err, "got %d of %d", len(got), n)
		got = append(got, r.Event.ID)
	}
	first := got[0]
	assert.Equal(t, idRange(first, first+n-1), got, "every id once, in order")
	r, ok, err := sub.TryNext()
	assert.False(t, ok, "nothing more: %v", r)
	assert.NoError(t, err)
}

func TestHubDropsTheTooSlow(t *testing.T) {
	h := NewHub(Options{QueueLimit: 3})
	sub, _ := h.Subscribe(Filter{}, Cursor{})
	defer sub.Close()
	publishPipelines(h, 4)

	_, err := sub.Next(t.Context())
	assert.ErrorIs(t, err, ErrTooSlow)

	// it subscribes again from where it was and misses nothing
	again, start := h.Subscribe(Filter{}, Cursor{ID: 0, Set: true})
	defer again.Close()
	assert.Len(t, start.Replay, 4)
}

func TestHubClose(t *testing.T) {
	h := newTestHub(10)
	sub, _ := h.Subscribe(Filter{}, Cursor{})
	sub.Close()
	publishPipelines(h, 1)
	_, ok, err := sub.TryNext()
	assert.False(t, ok, "a closed subscription gets nothing")
	assert.NoError(t, err)
	assert.Empty(t, h.subs)
}

func TestRingAfter(t *testing.T) {
	r := newRing(3)
	collect := func(after int64) []int64 {
		var out []int64
		r.after(after, func(rec *Record) { out = append(out, rec.Event.ID) })
		return out
	}
	assert.Empty(t, collect(0))
	for id := int64(10); id < 15; id++ {
		r.push(Record{Event: Event{ID: id}})
	}
	assert.Equal(t, []int64{12, 13, 14}, collect(0))
	assert.Equal(t, []int64{14}, collect(13))
	assert.Empty(t, collect(14))
	assert.Empty(t, collect(99))
}

func TestHubResume(t *testing.T) {
	h := newTestHub(5)
	first := hubStart.UnixMicro()
	publishPipelines(h, 8)

	for _, tt := range []struct {
		name   string
		cursor Cursor
		resume int64
	}{
		{"live goes on from the head", Cursor{}, first + 7},
		{"a replay from its cursor", Cursor{ID: first + 5, Set: true}, first + 5},
		{"every event kept from before the oldest", Cursor{ID: 0, Set: true}, first + 2},
		{"a reset from the head", Cursor{ID: first + 1, Set: true}, first + 7},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sub, start := h.Subscribe(Filter{}, tt.cursor)
			defer sub.Close()
			assert.Equal(t, tt.resume, start.Resume)
		})
	}
}

func TestHubEnrichesBeforeTheID(t *testing.T) {
	var seen []int64
	h := NewHub(Options{Enrich: func(e *Event) {
		seen = append(seen, e.ID)
		e.Summary += " enriched"
	}})
	publishPipelines(h, 2)

	sub, start := h.Subscribe(Filter{}, Cursor{ID: 0, Set: true})
	defer sub.Close()
	require.Len(t, start.Replay, 2)
	assert.Equal(t, []int64{0, 0}, seen, "an event is enriched before it has an id")
	for _, r := range start.Replay {
		assert.Contains(t, r.Event.Summary, " enriched", "the typed event")
		assert.Contains(t, string(r.Data), " enriched", "and its JSON")
	}
}

func TestBufferSize(t *testing.T) {
	require.NoError(t, CheckBufferSize(1))
	require.NoError(t, CheckBufferSize(MaxBufferSize))
	assert.Error(t, CheckBufferSize(0))
	assert.Error(t, CheckBufferSize(-1))
	assert.Error(t, CheckBufferSize(MaxBufferSize+1))

	h := NewHub(Options{BufferSize: 10 * MaxBufferSize})
	assert.Equal(t, MaxBufferSize, h.ring.size, "cut to the most")
	assert.Empty(t, h.ring.records, "nothing is allocated before the events come")
}

func TestSubscriptionPosition(t *testing.T) {
	h := newTestHub(3)
	first := hubStart.UnixMicro()
	sub, start := h.Subscribe(Filter{RepoIDs: []int64{99}}, Cursor{})
	defer sub.Close()
	assert.Equal(t, start.Resume, sub.Position(start.Resume), "nothing dropped yet")

	publishPipelines(h, 5) // none for the subscription; the buffer keeps first+2..first+4
	assert.Equal(t, first+1, sub.Position(start.Resume), "past the events dropped")
	assert.Equal(t, first+3, sub.Position(first+3), "never back")

	queued, _ := h.Subscribe(Filter{}, Cursor{})
	defer queued.Close()
	for id := int64(100); id < 104; id++ { // first+5..first+8; the buffer keeps first+6..
		p := pl(model.StatusPending, -1, -1)
		p.ID = id
		h.Observe(testRepo, p)
	}
	assert.Equal(t, first+3, queued.Position(first+3), "not past an event queued for it")
	_, ok, err := queued.TryNext()
	require.True(t, ok)
	require.NoError(t, err)
	assert.Equal(t, first+5, sub.Position(first+3), "the other subscription has none queued")
}
