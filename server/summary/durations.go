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
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

const (
	// DefaultMaxDurations bounds the typical runs kept; beyond it they are
	// all forgotten and looked up again.
	DefaultMaxDurations = 10000
	// A typical run is looked up again durationTTL after it was.
	durationTTL = time.Hour
)

// Durations keeps how long a typical run of the pipelines of a branch took:
// the latest successful pipeline of the same repo, branch and event. It
// looks one up in the store once and then learns the newer ones from the
// pipelines it observes, as the scheduler publishes them. It is safe for
// concurrent use.
type Durations struct {
	mu      sync.Mutex
	entries map[durationKey]durationEntry
	max     int
	now     func() time.Time
}

type durationKey struct {
	repoID int64
	branch string
	event  model.WebhookEvent
}

type durationEntry struct {
	// number is the number of the pipeline it is of, 0 if there is none
	number   int64
	duration time.Duration
	until    time.Time
}

// NewDurations makes a Durations keeping at most max typical runs, at most
// DefaultMaxDurations when max is 0.
func NewDurations(maxEntries int) *Durations {
	if maxEntries <= 0 {
		maxEntries = DefaultMaxDurations
	}
	return &Durations{entries: make(map[durationKey]durationEntry), max: maxEntries, now: time.Now}
}

func keyOf(repo *model.Repo, p *model.Pipeline) (durationKey, bool) {
	if p.Branch == "" {
		return durationKey{}, false
	}
	return durationKey{repoID: repo.ID, branch: p.Branch, event: p.Event}, true
}

// Observe learns the duration of a pipeline that succeeded, if it is the
// newest of its branch and event.
func (d *Durations) Observe(repo *model.Repo, p *model.Pipeline) {
	if d == nil || p.Status != model.StatusSuccess || p.Started <= 0 || p.Finished <= p.Started {
		return
	}
	key, ok := keyOf(repo, p)
	if !ok {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if entry, known := d.entries[key]; known && entry.number > p.Number {
		return
	}
	d.put(key, durationEntry{number: p.Number, duration: time.Duration(p.Finished-p.Started) * time.Second})
}

func (d *Durations) put(key durationKey, entry durationEntry) {
	if _, known := d.entries[key]; !known && len(d.entries) >= d.max {
		clear(d.entries)
	}
	entry.until = d.now().Add(durationTTL)
	d.entries[key] = entry
}

// Cached is the typical run of the pipeline as kept, without looking it up:
// false when it is not kept. A nil Durations keeps none, and knows there is
// none.
func (d *Durations) Cached(repo *model.Repo, p *model.Pipeline) (time.Duration, bool) {
	key, ok := keyOf(repo, p)
	if !ok || d == nil {
		return 0, true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	entry, known := d.entries[key]
	if !known || d.now().After(entry.until) {
		return 0, false
	}
	return entry.duration, true
}

// Typical is how long a typical run of the pipeline took: the latest
// successful pipeline of the same repo, branch and event. It is 0 when there
// is none, the pipeline has no branch, or it ended, and of a nil Durations.
func (d *Durations) Typical(s store.Store, repo *model.Repo, p *model.Pipeline) time.Duration {
	if d == nil || p.Status.IsDone() {
		return 0
	}
	if typical, ok := d.Cached(repo, p); ok {
		return typical
	}
	key, _ := keyOf(repo, p)
	list, err := s.GetPipelineList(repo,
		&model.ListOptionsWithAll{ListOptions: &model.ListOptions{Page: 1, PerPage: 1}},
		&model.PipelineFilter{Branch: p.Branch, Events: []model.WebhookEvent{p.Event}, Status: model.StatusSuccess})
	if err != nil {
		log.Error().Err(err).Int64("pipeline", p.ID).Msg("summary: cannot look up the pipelines before")
		return 0
	}
	var entry durationEntry
	if len(list) > 0 && list[0].Started > 0 && list[0].Finished > list[0].Started {
		entry = durationEntry{number: list[0].Number, duration: time.Duration(list[0].Finished-list[0].Started) * time.Second}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if known, ok := d.entries[key]; !ok || known.number <= entry.number || d.now().After(known.until) {
		d.put(key, entry)
	} else {
		entry = known
	}
	return entry.duration
}
