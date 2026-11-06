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

package explain

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	service_log "go.woodpecker-ci.org/woodpecker/v3/server/services/log"
	"go.woodpecker-ci.org/woodpecker/v3/shared/ansi"
)

const (
	// TailLines are the most lines of the end of a log the rules look at.
	TailLines = 4000
	// TailBytes are the most bytes of the end of a log the rules look at.
	TailBytes = 2 << 20
	// MaxLineBytes is the most bytes a line is kept with; the rest is cut.
	MaxLineBytes = 2048

	// DefaultCacheSize is how many steps' results are kept.
	DefaultCacheSize = 1024

	// Of a store that reads a log only whole, a result is kept as final
	// once it was found wholeLogGrace after the step finished: the lines an
	// agent sends late are stored by then.
	wholeLogGrace = time.Minute
)

// Logs reads the logs of steps from their end, as log.Service does.
type Logs interface {
	// LogLastLine gives the line of a step's last entry, -1 when none.
	LogLastLine(ctx context.Context, step *model.Step) (int, error)
	// LogStreamFrom calls fn on the entries of a step from the line from
	// on, reading only those.
	LogStreamFrom(ctx context.Context, step *model.Step, from int, fn func(*model.LogEntry) error) error
}

// Result is what explaining a step gives.
type Result struct {
	StepID   int64  `json:"step_id"`
	Status   string `json:"status"`
	ExitCode int    `json:"exit_code"`
	// Causes are the causes found, the first cause first; none when the
	// step did not fail.
	Causes []Cause `json:"causes"`
	// Lines are the lines of the log; From is the first of them the rules
	// looked at.
	Lines int `json:"lines"`
	From  int `json:"from,omitempty"`
	// Untrusted is always true: the titles and excerpts are text of the
	// log.
	Untrusted bool `json:"untrusted"`
} //	@name	StepExplanation

// Explainer explains steps from their logs and keeps the results of the
// steps that failed and ended.
type Explainer struct {
	logs  Logs
	whole bool
	cache *cache
	now   func() time.Time
}

// Options configure an explainer; zero fields take the defaults.
type Options struct {
	CacheSize int
	// Now is the clock, time.Now by default.
	Now func() time.Time
}

// New makes an explainer of the steps' logs.
func New(logs Logs, o Options) *Explainer {
	if o.CacheSize <= 0 {
		o.CacheSize = DefaultCacheSize
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Explainer{logs: logs, whole: service_log.ReadsWhole(logs), cache: newCache(o.CacheSize), now: o.Now}
}

// errPanicked is the error of an explanation that panicked.
var errPanicked = errors.New("explaining the step failed")

// Explain finds why the step failed. The result of a step that failed and
// ended is kept with the last line of the log it was found from: it is found
// again when the log has another last line since, e.g. when lines that came
// late were stored, and after a restart of the server. Of a store that reads
// a log only whole, the last line is not asked: a result found
// wholeLogGrace after the step finished is final, an earlier one is found
// again.
func (x *Explainer) Explain(ctx context.Context, step *model.Step) (r *Result, err error) {
	defer func() {
		if p := recover(); p != nil {
			log.Error().Int64("step", step.ID).Str("recovered", fmt.Sprint(p)).Str("stack", string(debug.Stack())).
				Msg("explain: a rule broke")
			r, err = nil, errPanicked
		}
	}()

	final := !step.Running() && step.Failing()
	if final {
		if cached, last, at, ok := x.cache.get(step.ID); ok {
			if x.whole {
				if at.Unix() >= step.Finished+int64(wholeLogGrace/time.Second) {
					return cached, nil
				}
			} else {
				current, err := x.logs.LogLastLine(ctx, step)
				if err != nil {
					return nil, err
				}
				if current == last {
					return cached, nil
				}
			}
		}
	}

	found := x.now()
	tail, last, err := ReadTail(ctx, x.logs, step)
	if err != nil {
		return nil, err
	}
	in := &Input{Lines: tail, ExitCode: step.ExitCode, State: step.State, Error: step.Error}
	causes, err := Analyze(ctx, in)
	if err != nil {
		return nil, err
	}
	r = &Result{
		StepID:    step.ID,
		Status:    string(step.State),
		ExitCode:  step.ExitCode,
		Causes:    causes,
		Lines:     last + 1,
		Untrusted: true,
	}
	if len(tail) > 0 {
		r.From = tail[0].N
	}
	if final {
		x.cache.put(step.ID, r, last, found)
	}
	return r, nil
}

// Forget drops what is kept of the step, as when its log is deleted.
func (x *Explainer) Forget(stepID int64) {
	x.cache.remove(stepID)
}

// errLogTooSlow ends reading a log whose context is done.
var errLogTooSlow = errors.New("reading the log took too long")

// ReadTail reads the end of a step's log, its last TailLines lines and at
// most TailBytes bytes of them, without their escape sequences and each cut
// to MaxLineBytes. Only those lines are read; of a store that reads a log
// only whole, the log is read once. It also gives the line of the log's last
// entry, -1 when it has none.
func ReadTail(ctx context.Context, logs Logs, step *model.Step) ([]Line, int, error) {
	from, last := 0, math.MaxInt
	if !service_log.ReadsWhole(logs) {
		var err error
		if last, err = logs.LogLastLine(ctx, step); err != nil || last < 0 {
			return nil, -1, err
		}
		from = max(last-TailLines+1, 0)
	}
	seen := -1
	var tail []Line
	var size int
	var buf []byte
	err := logs.LogStreamFrom(ctx, step, from, func(entry *model.LogEntry) error {
		if ctx.Err() != nil {
			return errLogTooSlow
		}
		if entry.Line > last {
			return nil // written since
		}
		seen = max(seen, entry.Line)
		buf = ansi.StripLine(buf[:0], entry.Data)
		text := truncateBytes(string(buf), MaxLineBytes)
		tail = append(tail, Line{N: entry.Line + 1, Text: text})
		size += len(text)
		for len(tail) > TailLines || size > TailBytes {
			size -= len(tail[0].Text)
			tail[0] = Line{}
			tail = tail[1:]
		}
		return nil
	})
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return nil, 0, err
	}
	return tail, seen, nil
}

// cache keeps the latest results by step, the least recently used dropped
// first.
type cache struct {
	mu    sync.Mutex
	size  int
	order *list.List // of *cacheEntry, the most recently used first
	byID  map[int64]*list.Element
}

type cacheEntry struct {
	stepID int64
	result *Result
	// last is the last line of the log the result was found from, at when
	// it was found.
	last int
	at   time.Time
}

func newCache(size int) *cache {
	return &cache{size: size, order: list.New(), byID: make(map[int64]*list.Element)}
}

func (c *cache) get(stepID int64) (*Result, int, time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.byID[stepID]
	if !ok {
		return nil, 0, time.Time{}, false
	}
	c.order.MoveToFront(el)
	entry := entryOf(el)
	return entry.result, entry.last, entry.at, true
}

func (c *cache) put(stepID int64, r *Result, last int, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byID[stepID]; ok {
		entry := entryOf(el)
		entry.result, entry.last, entry.at = r, last, at
		c.order.MoveToFront(el)
		return
	}
	c.byID[stepID] = c.order.PushFront(&cacheEntry{stepID: stepID, result: r, last: last, at: at})
	for c.order.Len() > c.size {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.byID, entryOf(oldest).stepID)
	}
}

func (c *cache) remove(stepID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.byID[stepID]; ok {
		c.order.Remove(el)
		delete(c.byID, stepID)
	}
}

// entryOf is the entry of an element of the cache's order.
func entryOf(el *list.Element) *cacheEntry {
	entry, _ := el.Value.(*cacheEntry) // all are
	return entry
}
