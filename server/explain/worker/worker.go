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

// Package worker explains the steps that failed in the background: it takes
// the step.failed events of the hub, finds their causes with an explainer
// and appends a step.explained event for each cause found. The step.failed
// itself goes out at once, without waiting for the log to be read.
package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	"go.woodpecker-ci.org/woodpecker/v3/server/explain"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

const (
	// DefaultWorkers explain steps at once by default.
	DefaultWorkers = 2
	// MaxWorkers are the most workers.
	MaxWorkers = 16
	// DefaultTimeout bounds explaining one step by default.
	DefaultTimeout = 10 * time.Second
	// DefaultQueueSize are the steps a worker keeps waiting; beyond them the
	// oldest is dropped.
	DefaultQueueSize = 256

	// An excerpt in an event has at most maxEventExcerptLines lines, before
	// the event's budget cuts them.
	maxEventExcerptLines = 6
)

// The results of a job, the label of jobsTotal.
const (
	resultExplained = "explained"
	resultNone      = "none"
	resultError     = "error"
	resultDropped   = "dropped"
)

// jobsTotal counts the failed steps the workers took, by what came of them.
var jobsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "woodpecker",
	Name:      "explain_jobs_total",
	Help:      "Failed steps taken to be explained in the background, by result: explained, none, error or dropped.",
}, []string{"result"})

// resets counts the times the pool fell so far behind the events that some
// step.failed events were gone before it took them.
var resets = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "woodpecker",
	Name:      "explain_resets_total",
	Help:      "Times the explaining of failed steps fell so far behind that some step.failed events were no longer kept.",
})

// Steps loads a step.
type Steps interface {
	StepLoad(pipelineID, stepID int64) (*model.Step, error)
}

// Options configure the workers; zero fields take the defaults.
type Options struct {
	Workers   int
	Timeout   time.Duration
	QueueSize int
	// Now is the clock, time.Now by default.
	Now func() time.Time
}

// Pool explains failed steps with a few workers. The steps of a pipeline go
// to one worker, so the step.explained events of a pipeline follow each
// other as its step.failed events do.
type Pool struct {
	hub       *events.Hub
	explainer *explain.Explainer
	steps     Steps
	timeout   time.Duration
	queueSize int
	now       func() time.Time
	shards    []*shard
}

// shard is the queue of one worker.
type shard struct {
	mu   sync.Mutex
	jobs []events.Event
	wake chan struct{}
}

// New makes a pool; Start starts it.
func New(hub *events.Hub, x *explain.Explainer, steps Steps, o Options) *Pool {
	if o.Workers <= 0 {
		o.Workers = DefaultWorkers
	}
	o.Workers = min(o.Workers, MaxWorkers)
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.QueueSize <= 0 {
		o.QueueSize = DefaultQueueSize
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	p := &Pool{hub: hub, explainer: x, steps: steps, timeout: o.Timeout, queueSize: o.QueueSize, now: o.Now}
	for range o.Workers {
		p.shards = append(p.shards, &shard{wake: make(chan struct{}, 1)})
	}
	return p
}

// filter selects what the pool explains.
var filter = events.Filter{Types: []events.Type{events.StepFailed}, MinSeverity: events.Debug}

// Start subscribes to the step.failed events, so none after it returns is
// missed, and runs the workers until the context ends.
func (p *Pool) Start(ctx context.Context) {
	sub, start := p.hub.Subscribe(filter, events.Cursor{})
	go p.take(ctx, sub, start.Resume)
	for _, s := range p.shards {
		go p.work(ctx, s)
	}
}

// take queues the step.failed events of the subscription. One that falls
// behind subscribes again from the last event it took and gets the rest.
func (p *Pool) take(ctx context.Context, sub *events.Subscription, last int64) {
	defer func() { sub.Close() }()
	for {
		rec, err := sub.Next(ctx)
		switch {
		case errors.Is(err, events.ErrTooSlow):
			sub.Close()
			var start events.Start
			sub, start = p.hub.Subscribe(filter, events.Cursor{ID: last, Set: true})
			if start.Reset != "" {
				// the events after the last taken are not all kept: all those
				// kept are after it, none was taken
				resets.Inc()
				log.Warn().Str("reason", string(start.Reset)).
					Msg("explain: some failed steps were not taken in time and are not explained")
				sub.Close()
				sub, start = p.hub.Subscribe(filter, events.Cursor{ID: 0, Set: true})
			}
			for _, r := range start.Replay {
				p.enqueue(r.Event)
			}
			last = start.Resume
			if n := len(start.Replay); n > 0 {
				last = start.Replay[n-1].Event.ID
			}
			continue
		case err != nil:
			return
		}
		last = rec.Event.ID
		p.enqueue(rec.Event)
	}
}

// enqueue queues a failed step with the worker of its pipeline, dropping the
// oldest one waiting there when its queue is full.
func (p *Pool) enqueue(e events.Event) {
	s := p.shards[uint64(e.PipelineID)%uint64(len(p.shards))]
	s.mu.Lock()
	if len(s.jobs) >= p.queueSize {
		s.jobs[0] = events.Event{}
		s.jobs = s.jobs[1:]
		jobsTotal.WithLabelValues(resultDropped).Inc()
	}
	s.jobs = append(s.jobs, e)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default: // a wake-up is pending
	}
}

func (s *shard) next() (events.Event, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.jobs) == 0 {
		return events.Event{}, false
	}
	e := s.jobs[0]
	s.jobs[0] = events.Event{}
	s.jobs = s.jobs[1:]
	return e, true
}

func (p *Pool) work(ctx context.Context, s *shard) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		}
		for ctx.Err() == nil {
			e, ok := s.next()
			if !ok {
				break
			}
			jobsTotal.WithLabelValues(p.explain(ctx, &e)).Inc()
		}
	}
}

// explain finds the cause of the step of a step.failed and appends its
// step.explained, telling what came of it.
func (p *Pool) explain(ctx context.Context, failed *events.Event) (result string) {
	defer func() {
		if r := recover(); r != nil {
			log.Error().Int64("step", failed.StepID).Str("recovered", fmt.Sprint(r)).Str("stack", string(debug.Stack())).
				Msg("explain: a worker broke")
			result = resultError
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	step, err := p.steps.StepLoad(failed.PipelineID, failed.StepID)
	if err != nil {
		log.Debug().Err(err).Int64("step", failed.StepID).Msg("explain: cannot load the step that failed")
		return resultError
	}
	r, err := p.explainer.Explain(ctx, step)
	if err != nil {
		log.Debug().Err(err).Int64("step", failed.StepID).Msg("explain: no cause in time")
		return resultError
	}
	if len(r.Causes) == 0 {
		return resultNone
	}
	p.hub.Append(events.Explained(failed, EventCause(&r.Causes[0]), p.now()))
	return resultExplained
}

// EventCause is a cause as an event carries it.
func EventCause(c *explain.Cause) events.Cause {
	lines := c.Excerpt
	if len(lines) > maxEventExcerptLines {
		lines = lines[:maxEventExcerptLines]
	}
	texts := make([]string, len(lines))
	for i, l := range lines {
		texts[i] = l.Text
	}
	return events.Cause{
		Kind:       c.Kind,
		Title:      c.Title,
		Line:       c.Line,
		Confidence: string(c.Confidence),
		Excerpt:    strings.Join(texts, "\n"),
		Untrusted:  true,
	}
}
