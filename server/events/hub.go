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
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

const (
	// DefaultBufferSize is how many of the latest events are kept for replay.
	DefaultBufferSize = 10000
	// MaxBufferSize is the most events that can be kept for replay.
	MaxBufferSize = 100000
	// DefaultQueueLimit bounds the events queued for one subscriber.
	DefaultQueueLimit = 1000
)

// ErrTooSlow ends a subscription that fell DefaultQueueLimit events behind.
// The subscriber can subscribe again from the last event it got.
var ErrTooSlow = errors.New("subscriber does not keep up with the events")

// CheckBufferSize tells whether n events can be kept for replay.
func CheckBufferSize(n int) error {
	if n < 1 || n > MaxBufferSize {
		return fmt.Errorf("the events kept for replay must be between 1 and %d, not %d", MaxBufferSize, n)
	}
	return nil
}

// Record is an event as the hub keeps it: the event and its JSON.
type Record struct {
	Event Event
	// Data is the event's JSON.
	Data []byte
}

// Filter selects the events a subscriber gets. A zero field selects all.
type Filter struct {
	RepoIDs     []int64
	Branch      string
	PipelineID  int64
	MinSeverity Severity
	Types       []Type
}

// Match tells whether the filter selects the event.
func (f *Filter) Match(e *Event) bool {
	return e.Severity >= f.MinSeverity &&
		(len(f.RepoIDs) == 0 || slices.Contains(f.RepoIDs, e.RepoID)) &&
		(f.Branch == "" || f.Branch == e.Branch) &&
		(f.PipelineID == 0 || f.PipelineID == e.PipelineID) &&
		(len(f.Types) == 0 || slices.Contains(f.Types, e.Type))
}

// Cursor is where a subscription starts: live only when not Set, after the
// event with ID when set, or with every event kept when ID is 0.
type Cursor struct {
	ID  int64
	Set bool
}

// Start is how a subscription starts.
type Start struct {
	// Head is the id of the newest event, Oldest of the oldest kept: Head+1
	// when none is.
	Head, Oldest int64
	// Resume is where the subscription goes on from: the cursor when its
	// events are replayed, else Head. A subscriber that subscribes again from
	// it misses nothing in between.
	Resume int64
	// Replay are the events after the cursor the filter selects.
	Replay []Record
	// Reset, if set, says the events after the cursor are not all kept any
	// more: none is replayed, and the subscriber has to load the current
	// state.
	Reset ResetReason
}

// Options configure a hub; zero fields take the defaults.
type Options struct {
	// BufferSize is how many events are kept for replay, at most
	// MaxBufferSize.
	BufferSize   int
	MaxPipelines int
	QueueLimit   int
	// Enrich, if set, completes each event before it gets its id, e.g. with
	// what made a step fail. It is called in the order of the events.
	Enrich func(*Event)
	// Now is the clock, time.Now by default.
	Now func() time.Time
}

// Hub makes the events of the pipeline snapshots it observes, keeps the
// latest of them and hands them to its subscribers.
//
// Event ids follow each other from the microsecond the hub was made, so they
// keep growing across restarts of the server, and a cursor from before one is
// told by being older than the first id.
type Hub struct {
	// observing orders Observe: diffing, enriching and appending the events
	// of one snapshot before those of the next.
	observing sync.Mutex
	differ    *Differ
	enrich    func(*Event)

	mu         sync.Mutex
	ring       *ring
	subs       map[*Subscription]struct{}
	firstID    int64
	nextID     int64
	queueLimit int
	now        func() time.Time
}

// NewHub makes a hub. A buffer size beyond MaxBufferSize is cut to it.
func NewHub(o Options) *Hub {
	if o.BufferSize <= 0 {
		o.BufferSize = DefaultBufferSize
	}
	o.BufferSize = min(o.BufferSize, MaxBufferSize)
	if o.QueueLimit <= 0 {
		o.QueueLimit = DefaultQueueLimit
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	started := o.Now()
	first := started.UnixMicro()
	return &Hub{
		differ:     NewDiffer(o.MaxPipelines, started),
		enrich:     o.Enrich,
		ring:       newRing(o.BufferSize),
		subs:       make(map[*Subscription]struct{}),
		firstID:    first,
		nextID:     first,
		queueLimit: o.QueueLimit,
		now:        o.Now,
	}
}

// Observe takes the next snapshot of a pipeline: the pipeline with its
// workflows and their steps. Its events are enriched and appended. It never
// waits for a subscriber.
func (h *Hub) Observe(repo *model.Repo, pipeline *model.Pipeline) {
	h.observing.Lock()
	defer h.observing.Unlock()

	events := h.differ.Observe(repo, pipeline, h.now())
	if h.enrich != nil {
		for i := range events {
			h.enrich(&events[i])
		}
	}
	h.Append(events...)
}

// Append gives the events their ids, keeps them and hands them to the
// subscribers whose filter selects them.
func (h *Hub) Append(events ...Event) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, e := range events {
		e.V = SchemaVersion
		e.ID = h.nextID
		data, err := Encode(e)
		if err != nil {
			log.Error().Err(err).Str("type", string(e.Type)).Msg("events: cannot encode an event")
			continue
		}
		h.nextID++
		rec := Record{Event: e, Data: data}
		h.ring.push(rec)
		for s := range h.subs {
			if s.filter.Match(&rec.Event) {
				s.enqueue(rec)
			}
		}
	}
}

// Subscribe starts a subscription at the cursor. The events after the cursor
// are in the start, the ones after them come from the subscription; none is
// missed or repeated between them. The subscription ends with Close.
func (h *Hub) Subscribe(f Filter, c Cursor) (*Subscription, Start) {
	h.mu.Lock()
	defer h.mu.Unlock()

	start := Start{Head: h.nextID - 1, Oldest: h.nextID}
	if oldest, ok := h.ring.oldest(); ok {
		start.Oldest = oldest
	}
	start.Resume = start.Head

	replayAfter := int64(-1) // none
	switch {
	case !c.Set:
	case c.ID == 0:
		replayAfter = start.Oldest - 1
	case c.ID < h.firstID-1:
		start.Reset = ResetRestart
	case c.ID > start.Head:
		start.Reset = ResetInvalid
	case c.ID < start.Oldest-1:
		start.Reset = ResetExpired
	default:
		replayAfter = c.ID
	}
	if replayAfter >= 0 {
		start.Resume = replayAfter
		h.ring.after(replayAfter, func(r *Record) {
			if f.Match(&r.Event) {
				start.Replay = append(start.Replay, *r)
			}
		})
	}

	s := &Subscription{hub: h, filter: f, limit: h.queueLimit, wake: make(chan struct{}, 1)}
	h.subs[s] = struct{}{}
	return s, start
}

// Subscribers is how many subscriptions are open.
func (h *Hub) Subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// Subscription gets the events its filter selects, in order.
type Subscription struct {
	hub    *Hub
	filter Filter
	limit  int

	mu        sync.Mutex
	queue     []Record
	overflown bool
	wake      chan struct{}
}

func (s *Subscription) enqueue(r Record) {
	s.mu.Lock()
	switch {
	case s.overflown:
	case len(s.queue) >= s.limit:
		s.overflown = true
		s.queue = nil
	default:
		s.queue = append(s.queue, r)
	}
	s.mu.Unlock()

	select {
	case s.wake <- struct{}{}:
	default: // a wake-up is pending
	}
}

// Next waits for the next event. It returns ErrTooSlow once the subscriber
// fell too far behind, and the context's error once it is done.
func (s *Subscription) Next(ctx context.Context) (Record, error) {
	for {
		if r, ok, err := s.TryNext(); ok || err != nil {
			return r, err
		}
		select {
		case <-ctx.Done():
			return Record{}, ctx.Err()
		case <-s.wake:
		}
	}
}

// Wake returns a channel that is ready when an event may be queued; Next
// after it does not block if one is. Callers that wait for other things too
// use it with TryNext.
func (s *Subscription) Wake() <-chan struct{} {
	return s.wake
}

// TryNext takes the next queued event without waiting: false if there is
// none, ErrTooSlow once the subscriber fell too far behind.
func (s *Subscription) TryNext() (Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.overflown {
		return Record{}, false, ErrTooSlow
	}
	if len(s.queue) == 0 {
		return Record{}, false, nil
	}
	r := s.queue[0]
	s.queue[0] = Record{}
	s.queue = s.queue[1:]
	return r, true, nil
}

// Position is where a cursor at the id given can be moved without missing an
// event of the subscription: past the events the buffer no longer keeps, if
// no event is queued for it. A subscriber that was handed all its events up
// to the id given lost none of the ones dropped.
func (s *Subscription) Position(id int64) int64 {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	oldest, ok := s.hub.ring.oldest()
	if !ok || len(s.queue) > 0 || s.overflown {
		return id
	}
	return max(id, oldest-1)
}

// Close ends the subscription.
func (s *Subscription) Close() {
	s.hub.mu.Lock()
	delete(s.hub.subs, s)
	s.hub.mu.Unlock()
}
