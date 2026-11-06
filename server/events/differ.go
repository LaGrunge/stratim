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
	"cmp"
	"fmt"
	"slices"
	"time"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

const (
	// DefaultMaxPipelines bounds the pipelines the differ remembers; beyond
	// it the one seen longest ago is forgotten.
	DefaultMaxPipelines = 10000
	// A finished pipeline is remembered for finishedTTL after it was last
	// seen, so that a late snapshot of it changes nothing.
	finishedTTL = 10 * time.Minute
	// An unfinished pipeline nothing is heard of is remembered for idleTTL.
	idleTTL = 24 * time.Hour
	// The forgotten pipelines are swept every sweepInterval.
	sweepInterval = time.Minute
)

// pipelineState is what the differ remembers of a pipeline: the states of it,
// its workflows and its steps, nothing else.
type pipelineState struct {
	status    model.StatusValue
	workflows map[int64]model.StatusValue
	steps     map[int64]model.StatusValue
	lastSeen  time.Time
}

// Differ turns the snapshots of pipelines into the events between them. It
// is not safe for concurrent use.
//
// The snapshots of one pipeline may arrive out of order: two goroutines
// each build one from the store and publish it. So states only move forward
// (created, blocked, pending, running, done): a snapshot that shows something
// as less far than the differ has seen it changes nothing.
type Differ struct {
	pipelines    map[int64]*pipelineState
	maxPipelines int
	lastSweep    time.Time
	// started is when the differ started, in unix seconds.
	started int64
}

// NewDiffer makes a differ that remembers at most maxPipelines pipelines and
// started at the time given.
func NewDiffer(maxPipelines int, started time.Time) *Differ {
	if maxPipelines <= 0 {
		maxPipelines = DefaultMaxPipelines
	}
	return &Differ{pipelines: make(map[int64]*pipelineState), maxPipelines: maxPipelines, started: started.Unix()}
}

// The ranks of the states, in the order they move.
const (
	rankNone = iota - 1
	rankCreated
	rankBlocked
	rankPending
	rankRunning
	rankDone
)

func rank(s model.StatusValue) int {
	switch {
	case s == model.StatusCreated:
		return rankCreated
	case s == model.StatusBlocked:
		return rankBlocked
	case s == model.StatusPending:
		return rankPending
	case s == model.StatusRunning:
		return rankRunning
	case s.IsDone():
		return rankDone
	default:
		return rankNone
	}
}

// Observe takes the next snapshot of a pipeline, with its workflows and their
// steps if it has them loaded, and gives the events since the last one, in
// the order they happened: the pipeline's start, then per workflow its start,
// its steps and its end, then the pipeline's end. The events have no ID yet.
//
// The first snapshot of a pipeline that already ran (seen first after a
// restart of the server, or after it was forgotten) only becomes what the
// next ones are compared with: its past is not made into events. Only if it
// finished lately, since the differ started and within finishedTTL, its end
// is: pipeline.finished.
func (d *Differ) Observe(repo *model.Repo, p *model.Pipeline, now time.Time) []Event {
	defer d.sweep(now)

	prev, known := d.pipelines[p.ID]
	if !known {
		prev = &pipelineState{workflows: make(map[int64]model.StatusValue), steps: make(map[int64]model.StatusValue), lastSeen: now}
		d.evict()
		d.pipelines[p.ID] = prev
	}
	prev.lastSeen = now

	b := builder{repo: repo, p: p, now: now}
	baseline := !known && alreadyRan(p)

	prevRank, curRank := rank(prev.status), rank(p.Status)
	if curRank > prevRank {
		switch {
		case p.Status == model.StatusBlocked:
			b.pipeline(PipelineBlocked, Warning, p.Created)
		case prevRank == rankNone && curRank < rankRunning:
			b.pipeline(PipelineCreated, Info, p.Created)
		case p.Status == model.StatusRunning:
			b.pipeline(PipelineStarted, Notice, p.Started)
		}
		prev.status = p.Status
	}

	workflows := slices.Clone(p.Workflows)
	slices.SortFunc(workflows, func(a, b *model.Workflow) int { return cmp.Compare(a.PID, b.PID) })
	for _, w := range workflows {
		d.workflow(&b, prev, w)
	}

	if curRank == rankDone && prevRank < rankDone {
		severity := finishedSeverity(p.Status, Notice, Warning)
		b.pipelineFinished(severity)
	}

	if baseline {
		return d.lateEnd(p, now, b.events)
	}
	return b.events
}

func (d *Differ) workflow(b *builder, prev *pipelineState, w *model.Workflow) {
	prevRank, curRank := rank(prev.workflows[w.ID]), rank(w.State)
	if curRank > prevRank {
		prev.workflows[w.ID] = w.State
		if w.State == model.StatusRunning {
			b.workflow(w, WorkflowStarted, Info)
		}
	}

	steps := slices.Clone(w.Children)
	slices.SortFunc(steps, func(a, b *model.Step) int { return cmp.Compare(a.PID, b.PID) })
	for _, s := range steps {
		stepPrevRank, stepCurRank := rank(prev.steps[s.ID]), rank(s.State)
		if stepCurRank <= stepPrevRank {
			continue
		}
		prev.steps[s.ID] = s.State
		switch {
		case s.State == model.StatusRunning:
			b.step(w, s, StepStarted, Debug)
		case stepCurRank == rankDone:
			b.stepEnded(w, s)
		}
	}

	if curRank == rankDone && prevRank < rankDone {
		b.workflowFinished(w, finishedSeverity(w.State, Notice, Info))
	}
}

// lateEnd keeps of the events of a pipeline seen first after it ran its
// pipeline.finished, if it finished lately.
func (d *Differ) lateEnd(p *model.Pipeline, now time.Time, events []Event) []Event {
	if p.Finished < d.started || now.Unix()-p.Finished >= int64(finishedTTL/time.Second) {
		return nil
	}
	for _, e := range events {
		if e.Type == PipelineFinished {
			return []Event{e}
		}
	}
	return nil
}

// alreadyRan tells a pipeline that is running or ran: one of its workflows
// started.
func alreadyRan(p *model.Pipeline) bool {
	if p.Status == model.StatusRunning {
		return true
	}
	if !p.Status.IsDone() {
		return false
	}
	for _, w := range p.Workflows {
		if w.Started > 0 || rank(w.State) == rankRunning {
			return true
		}
	}
	return false
}

// finishedSeverity is the severity of the end of a pipeline or workflow:
// success is a notice, a failure an error. Killed and declined are a warning;
// canceled is the one given (a pipeline canceled is a warning, a workflow
// that never started because of it just info).
func finishedSeverity(s model.StatusValue, success, canceled Severity) Severity {
	switch s {
	case model.StatusSuccess:
		return success
	case model.StatusFailure, model.StatusError, model.StatusTimeout:
		return Error
	case model.StatusKilled:
		return Warning
	case model.StatusCanceled, model.StatusDeclined:
		return canceled
	default: // skipped
		return Info
	}
}

// sweep forgets the pipelines finished or idle for long, once a minute.
func (d *Differ) sweep(now time.Time) {
	if now.Sub(d.lastSweep) < sweepInterval {
		return
	}
	d.lastSweep = now
	for id, state := range d.pipelines {
		idle := now.Sub(state.lastSeen)
		if idle > idleTTL || (state.status.IsDone() && idle > finishedTTL) {
			delete(d.pipelines, id)
		}
	}
}

// evict forgets the pipelines seen longest ago to make room for one more.
func (d *Differ) evict() {
	for len(d.pipelines) >= d.maxPipelines {
		var oldestID int64
		var oldest time.Time
		first := true
		for id, state := range d.pipelines {
			if first || state.lastSeen.Before(oldest) {
				oldestID, oldest, first = id, state.lastSeen, false
			}
		}
		delete(d.pipelines, oldestID)
	}
}

// builder makes the events of one snapshot.
type builder struct {
	repo   *model.Repo
	p      *model.Pipeline
	now    time.Time
	events []Event
}

func (b *builder) base(t Type, severity Severity, at int64) Event {
	ts := b.now
	if at > 0 {
		ts = time.Unix(at, 0)
	}
	return Event{
		V:          SchemaVersion,
		Type:       t,
		Severity:   severity,
		TS:         ts.UTC().Format(time.RFC3339),
		RepoID:     b.repo.ID,
		Repo:       b.repo.FullName,
		PipelineID: b.p.ID,
		Number:     b.p.Number,
		Branch:     b.p.Branch,
	}
}

func (b *builder) add(e Event, subject, what string) {
	e.Summary = truncateRunes(fmt.Sprintf("#%d %s%s", b.p.Number, subject, what), maxSummaryRunes)
	b.events = append(b.events, e)
}

func (b *builder) pipeline(t Type, severity Severity, at int64) {
	e := b.base(t, severity, at)
	e.Status = string(b.p.Status)
	var what string
	switch t {
	case PipelineCreated:
		what = "created"
		if e.Branch != "" {
			what += " on " + short(e.Branch)
		}
	case PipelineBlocked:
		what = "waits for approval"
	default:
		what = "started"
	}
	b.add(e, "", what)
}

func (b *builder) pipelineFinished(severity Severity) {
	e := b.base(PipelineFinished, severity, b.p.Finished)
	e.Status = string(b.p.Status)
	e.DurationS = duration(b.p.Started, b.p.Finished)
	b.add(e, "", ended(e.Status, e.DurationS))
}

func (b *builder) withWorkflow(e Event, w *model.Workflow) Event {
	e.Workflow = w.Name
	e.WorkflowID = w.ID
	e.Status = string(w.State)
	return e
}

func (b *builder) workflow(w *model.Workflow, t Type, severity Severity) {
	e := b.withWorkflow(b.base(t, severity, w.Started), w)
	b.add(e, short(e.Workflow)+" ", "started")
}

func (b *builder) workflowFinished(w *model.Workflow, severity Severity) {
	e := b.withWorkflow(b.base(WorkflowFinished, severity, w.Finished), w)
	e.DurationS = duration(w.Started, w.Finished)
	b.add(e, short(e.Workflow)+" ", ended(e.Status, e.DurationS))
}

func (b *builder) withStep(e Event, w *model.Workflow, s *model.Step) Event {
	e = b.withWorkflow(e, w)
	e.Step = s.Name
	e.StepID = s.ID
	e.Status = string(s.State)
	return e
}

func (b *builder) step(w *model.Workflow, s *model.Step, t Type, severity Severity) {
	e := b.withStep(b.base(t, severity, s.Started), w, s)
	b.add(e, short(e.Workflow)+"/"+short(e.Step)+" ", "started")
}

// stepEnded makes the event of a step that ended: none for one skipped.
func (b *builder) stepEnded(w *model.Workflow, s *model.Step) {
	t, severity := StepFinished, Info
	switch s.State {
	case model.StatusSkipped:
		return
	case model.StatusCanceled, model.StatusDeclined:
		severity = Debug
	case model.StatusKilled:
		severity = Warning
	case model.StatusFailure, model.StatusTimeout, model.StatusError:
		t, severity = StepFailed, Error
	}

	e := b.withStep(b.base(t, severity, s.Finished), w, s)
	if t == StepFailed && s.Failure == model.FailureIgnore {
		e.Severity, e.Ignored = Notice, true
	}
	exitCode := s.ExitCode
	if s.Started > 0 { // one that never started has no exit code
		e.ExitCode = &exitCode
	}
	e.DurationS = duration(s.Started, s.Finished)

	what := ended(e.Status, e.DurationS)
	if t == StepFailed {
		what += fmt.Sprintf(", exit %d", exitCode)
	}
	if e.Ignored {
		what += " (ignored)"
	}
	b.add(e, short(e.Workflow)+"/"+short(e.Step)+" ", what)
}

func duration(started, finished int64) *int64 {
	if started <= 0 || finished < started {
		return nil
	}
	d := finished - started
	return &d
}

// short is a name as an event carries it.
func short(name string) string {
	return truncateBytes(name, maxNameBytes)
}

func ended(status string, durationS *int64) string {
	if durationS == nil {
		return status
	}
	return fmt.Sprintf("%s after %s", status, time.Duration(*durationS)*time.Second)
}
