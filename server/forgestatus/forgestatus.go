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

// Package forgestatus keeps the statuses a forge shows of each workflow in
// line with the store.
//
// A forge keeps only the last status per commit and context, so a lost
// final status leaves a check pending for good, and an older status landing
// last shows a wrong result. The server therefore does not send statuses
// where they change: it marks the workflow, and a sender of its pipeline
// reads the workflow from the store and sends what is there at that moment.
// A status that failed is sent again, with growing waits, for up to a
// window; by then the store may hold a newer one, which goes instead, with
// a window of its own. So:
//
//   - the newest status always goes last: a pipeline has one sender at a
//     time, and it sends what the store holds when it sends;
//   - changes made while a forge was failing collapse into the last one;
//   - nobody waits for the forge: marking costs a map write.
//
// A status the forge took is not sent again: the server marks a workflow
// several times while its state stays the same.
//
// Marks live in memory. The statuses a forge took, or refused for good, are
// kept in the store too: on start, Resend marks the recent workflows whose
// forge has neither, so a status not yet through when the server stopped,
// or given up on, is sent after all. Those go one at a time, slowly, never
// in the way of the statuses of the moment.
package forgestatus

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v7"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	store_types "go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// Policy is how hard a status is pushed through.
type Policy struct {
	Attempt   time.Duration // bounds each request to the forge
	FirstWait time.Duration // after the first failure; grows up to MaxWait
	MaxWait   time.Duration
	Window    time.Duration // how long a failing status is tried
	Senders   int           // pipelines synced at once
	// Resend sends the statuses not taken of the workflows of the pipelines
	// created ResendSince ago at most (MaxResendSince at most), ResendMax of
	// them at most; its requests go one every ResendEvery at most, retries
	// included. With ResendSince or ResendMax zero it sends none.
	ResendSince time.Duration
	ResendMax   int
	ResendEvery time.Duration
}

// DefaultPolicy is the policy of the server.
var DefaultPolicy = Policy{
	Attempt:   10 * time.Second, //nolint:mnd
	FirstWait: time.Second,
	MaxWait:   time.Minute,
	Window:    10 * time.Minute, //nolint:mnd
	Senders:   8,                //nolint:mnd

	ResendSince: 3 * 24 * time.Hour,
	ResendMax:   5000, //nolint:mnd
	// well below the content creation limit of GitHub, 80 a minute
	ResendEvery: time.Second,
}

// MaxResendSince is as far back as Resend goes: the statuses of before were
// taken as taken when the store began recording them, by migration 031.
const MaxResendSince = 30 * 24 * time.Hour

// withDefaults fills the fields left zero from DefaultPolicy, but for
// ResendSince and ResendMax: zero turns Resend off.
func (p Policy) withDefaults() Policy {
	d := DefaultPolicy
	for _, f := range []struct {
		v   *time.Duration
		def time.Duration
	}{{&p.Attempt, d.Attempt}, {&p.FirstWait, d.FirstWait}, {&p.MaxWait, d.MaxWait}, {&p.Window, d.Window}, {&p.ResendEvery, d.ResendEvery}} {
		if *f.v <= 0 {
			*f.v = f.def
		}
	}
	if p.Senders <= 0 {
		p.Senders = d.Senders
	}
	if p.ResendSince > MaxResendSince {
		log.Warn().Msgf("statuses not taken by the forges: sent again from %s back at most, not %s", MaxResendSince, p.ResendSince)
		p.ResendSince = MaxResendSince
	}
	return p
}

// maxSent is how many sent statuses are remembered; forgetting them costs
// at most one repeated request each.
const maxSent = 10000

// Store is what the sender reads, and where it keeps the statuses taken.
type Store interface {
	GetPipeline(id int64) (*model.Pipeline, error)
	GetRepo(id int64) (*model.Repo, error)
	GetUser(id int64) (*model.User, error)
	WorkflowList(pipeline *model.Pipeline) ([]*model.Workflow, error)
	ForgeStatusSet(status *model.ForgeStatus) error
	ForgeStatusCandidates(since, before int64, limit int) ([]*model.ForgeStatusCandidate, error)
}

// Sender sends the statuses of marked workflows to their forges.
type Sender struct {
	ctx        context.Context
	store      Store
	forgeOf    func(*model.Repo) (forge.Forge, error)
	refresh    func(context.Context, forge.Forge, *model.User)
	policy     Policy
	slots      chan struct{}
	resendSlot chan struct{} // the one slot of resent statuses, apart

	mu        sync.Mutex
	marked    map[int64]*marks // pipeline ID -> workflows to sync
	busy      map[int64]bool   // pipelines with a sender
	sent      map[int64]string // workflow ID -> the status its forge took
	resending map[int64]int64  // workflow ID -> pipeline ID, resent and not done with
	resent    resendCounts
	resentAt  time.Time      // when the last resent status was marked
	resendAt  time.Time      // when the next request of a resent status may go
	page      int            // candidates read at once
	wg        sync.WaitGroup // senders, for tests

	// paced, for tests, learns the turn of each request of a resent status
	// as the request goes; it is set before the first mark.
	paced func(turn time.Time)
}

// marks are the workflows of one pipeline to sync.
type marks struct {
	all       bool
	workflows map[int64]bool
	resend    bool // all of them are resent
}

func (m *marks) add(o *marks) {
	m.resend = (m.empty() || m.resend) && o.resend
	m.all = m.all || o.all
	for id := range o.workflows {
		m.workflows[id] = true
	}
}

func (m *marks) empty() bool { return !m.all && len(m.workflows) == 0 }

func newMarks() *marks { return &marks{workflows: make(map[int64]bool)} }

// New returns a sender that sends statuses until ctx is done; with refresh
// it renews the token of a user if its forge needs it.
func New(ctx context.Context, s Store, forgeOf func(*model.Repo) (forge.Forge, error), refresh func(context.Context, forge.Forge, *model.User), policy Policy) *Sender {
	policy = policy.withDefaults()
	return &Sender{
		ctx:        ctx,
		store:      s,
		forgeOf:    forgeOf,
		refresh:    refresh,
		policy:     policy,
		slots:      make(chan struct{}, policy.Senders),
		resendSlot: make(chan struct{}, 1),
		marked:     make(map[int64]*marks),
		busy:       make(map[int64]bool),
		sent:       make(map[int64]string),
		resending:  make(map[int64]int64),
		page:       resendPage,
	}
}

// Wait waits for the senders to end, once ctx is done.
func (s *Sender) Wait() { s.wg.Wait() }

// Pipeline marks every workflow of the pipeline. A nil sender does nothing.
func (s *Sender) Pipeline(pipelineID int64) {
	if s == nil {
		return
	}
	m := newMarks()
	m.all = true
	s.mark(pipelineID, m)
}

// Workflow marks one workflow of the pipeline. A nil sender does nothing.
func (s *Sender) Workflow(pipelineID, workflowID int64) {
	if s == nil {
		return
	}
	m := newMarks()
	m.workflows[workflowID] = true
	s.mark(pipelineID, m)
}

func (s *Sender) mark(pipelineID int64, m *marks) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return // the server stops: nobody sends it
	}
	if prev, ok := s.marked[pipelineID]; ok {
		prev.add(m)
	} else {
		s.marked[pipelineID] = m
	}
	if !s.busy[pipelineID] {
		s.busy[pipelineID] = true
		s.wg.Add(1)
		go s.run(pipelineID)
	}
}

// take returns the marks of the pipeline added to todo, and clears them.
// With nothing to do it lets the pipeline go, in the same step, so a mark
// added meanwhile always finds a sender.
func (s *Sender) take(pipelineID int64, todo *marks) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.marked[pipelineID]; ok {
		todo.add(m)
		delete(s.marked, pipelineID)
	}
	if todo.empty() || s.ctx.Err() != nil {
		delete(s.busy, pipelineID)
		return false
	}
	return true
}

// takeLive adds to todo the marks of the pipeline that came meanwhile if
// one of them is live, which makes todo live.
func (s *Sender) takeLive(pipelineID int64, todo *marks) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.marked[pipelineID]; ok && !m.resend {
		todo.add(m)
		delete(s.marked, pipelineID)
	}
}

// run sends the statuses of the pipeline until none is marked or failing.
func (s *Sender) run(pipelineID int64) {
	defer s.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Error().Msgf("statuses of pipeline %d: sender panicked: %v", pipelineID, r)
			s.donePipeline(pipelineID, gaveUp)
			s.restart(pipelineID)
		}
	}()

	waits := backoff.NewExponentialBackOff()
	waits.InitialInterval, waits.MaxInterval = s.policy.FirstWait, s.policy.MaxWait
	p := &pipelineSync{id: pipelineID, failing: make(map[int64]*failure)}
	todo := newMarks()
	for s.take(pipelineID, todo) {
		retry, after, ok := s.syncInSlot(p, todo)
		if !ok {
			continue // the server stops; take lets go
		}
		todo = retry
		if todo.empty() {
			waits.Reset()
			continue
		}
		if todo.resend {
			if s.takeLive(pipelineID, todo); !todo.resend {
				continue // a newer status came: it goes now
			}
		}
		sleep(s.ctx, s.retryWait(waits.NextBackOff(), after))
	}
}

// retryWait is how long to wait before the next attempt: the backoff, or as
// long as the forge asked, plus a little, so that senders stalled by one
// rate limit do not all retry the same instant; never past the window,
// which would hold back newer statuses that long.
func (s *Sender) retryWait(backoffWait, after time.Duration) time.Duration {
	if after <= 0 {
		return backoffWait
	}
	jitter := time.Duration(rand.Int64N(int64(s.policy.FirstWait) + 1)) //nolint:gosec
	return min(min(after, s.policy.Window)+jitter, s.policy.Window)
}

// restart gives the pipeline a new sender if marks came while the one
// before panicked; the marks it was sending are dropped, so that a status
// that panics is not sent forever.
func (s *Sender) restart(pipelineID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.marked[pipelineID]; ok && !m.empty() && s.ctx.Err() == nil {
		s.wg.Add(1)
		go s.run(pipelineID)
		return
	}
	delete(s.busy, pipelineID)
}

// pipelineSync is what the sender of one pipeline keeps between attempts.
type pipelineSync struct {
	id      int64
	loading *failure           // failures to read the pipeline
	failing map[int64]*failure // workflow ID -> failures of its status
}

// failure is how a status, or reading the pipeline, has failed so far.
type failure struct {
	value    string // the status that failed
	since    time.Time
	attempts int
}

// fail records a failed attempt at value. A status other than the one
// failing so far gets a window of its own.
func fail(f *failure, value string) *failure {
	if f == nil || f.value != value {
		f = &failure{value: value, since: time.Now()}
	}
	f.attempts++
	return f
}

func (f *failure) expired(window time.Duration) bool { return time.Since(f.since) > window }

// syncInSlot is sync within one of the slots; ok is false when the server
// stopped before a slot was free. Resent statuses have a slot of their own
// and give way to the statuses of the moment.
func (s *Sender) syncInSlot(p *pipelineSync, todo *marks) (retry *marks, after time.Duration, ok bool) {
	release, turn, ok := s.acquire(p.id, todo)
	if !ok {
		return newMarks(), 0, false
	}
	defer release()
	retry, after = s.sync(p, todo, turn)
	return retry, after, true
}

// acquire takes a slot for todo; for resent statuses it also waits for the
// turn of their first request, and returns it.
func (s *Sender) acquire(pipelineID int64, todo *marks) (release func(), turn time.Time, ok bool) {
	for todo.resend {
		select {
		case s.resendSlot <- struct{}{}:
			turn, ok = s.paceResend()
			if !ok {
				<-s.resendSlot
				return nil, time.Time{}, false
			}
			s.takeLive(pipelineID, todo)
			if todo.resend {
				return func() { <-s.resendSlot }, turn, true
			}
			<-s.resendSlot // live now: it goes in a slot of the senders
		case <-s.ctx.Done():
			return nil, time.Time{}, false
		case <-time.After(s.policy.FirstWait):
			s.takeLive(pipelineID, todo) // a live mark does not wait for the resent ones
		}
	}
	select {
	case s.slots <- struct{}{}:
		return func() { <-s.slots }, time.Time{}, true
	case <-s.ctx.Done():
		return nil, time.Time{}, false
	}
}

// outcome is what a status attempt leads to.
type outcome int

const (
	taken   outcome = iota // the forge has the status
	refused                // the forge will not take it; do not send it again
	failed                 // it may pass if sent again
	gaveUp                 // it failed for the whole window
)

// classify tells what the error of a status attempt leads to, and how long
// the forge asked to wait before the next.
func classify(err error) (outcome, time.Duration) {
	var statusErr *types.StatusError
	switch {
	case err == nil, errors.Is(err, types.ErrNotImplemented):
		return taken, 0
	case errors.As(err, &statusErr) && statusErr.Final:
		return refused, 0
	case errors.As(err, &statusErr):
		return failed, statusErr.RetryAfter
	default:
		return failed, 0
	}
}

// refusedForAuth tells whether the forge refused a status for the user's
// credentials, which a login renews: then a start sends it again.
func refusedForAuth(err error) bool {
	statusErr, ok := errors.AsType[*types.StatusError](err)
	return ok && statusErr.Auth
}

// sync sends the statuses of the marked workflows of the pipeline. It
// returns the workflows to try again, and how long a forge asked to wait.
// For resent statuses, turn is the turn of the first request.
func (s *Sender) sync(p *pipelineSync, todo *marks, turn time.Time) (*marks, time.Duration) {
	retry := newMarks()
	retry.resend = todo.resend
	l, err := s.load(p.id)
	if errors.Is(err, errGone) {
		// recorded so that a start does not send them again
		for _, w := range l.marked(todo) {
			s.storeValue(l.pipeline, w)
		}
		s.donePipeline(p.id, refused)
		return newMarks(), 0
	}
	if err != nil {
		// nothing could be sent; try it all again, within the window
		p.loading = fail(p.loading, "")
		if p.loading.expired(s.policy.Window) {
			log.Error().Err(err).Msgf("statuses of pipeline %d: gave up after %d attempts", p.id, p.loading.attempts)
			p.loading = nil
			s.donePipeline(p.id, gaveUp)
			return newMarks(), 0
		}
		if p.loading.attempts == 1 {
			log.Warn().Err(err).Msgf("statuses of pipeline %d: trying again", p.id)
		}
		return todo, 0
	}
	p.loading = nil

	var after time.Duration
	seen := make(map[int64]bool)
	requested := false
	for _, w := range l.marked(todo) {
		seen[w.ID] = true
		value := model.ForgeStatusValue(l.pipeline.Event, w.State, l.pipeline.Status)
		if s.sentValue(w.ID) == value {
			s.done(w.ID, taken)
			continue
		}

		// each request of resent statuses waits its turn; acquire waited
		// for the first
		if todo.resend && requested {
			var ok bool
			if turn, ok = s.paceResend(); !ok {
				return newMarks(), 0 // the server stops
			}
		}
		requested = true
		if todo.resend && s.paced != nil {
			s.paced(turn)
		}
		ctx, cancel := context.WithTimeout(s.ctx, s.policy.Attempt)
		err := l.forge.Status(ctx, l.user, l.repo, l.pipeline, w)
		cancel()
		what := fmt.Sprintf("status %s of %s#%d %s", w.State, l.repo.FullName, l.pipeline.Number, w.Name)

		result, wait := classify(err)
		if result == failed && s.ctx.Err() != nil {
			return newMarks(), 0 // the server stops
		}
		switch result {
		case taken:
			s.setSent(w.ID, value)
			s.storeValue(l.pipeline, w)
			if fl := p.failing[w.ID]; fl != nil && fl.value == value {
				log.Info().Msgf("%s: sent after %d failed attempts in %s", what, fl.attempts, time.Since(fl.since).Round(time.Second))
			}
			delete(p.failing, w.ID)
		case refused:
			s.setSent(w.ID, "")
			if !refusedForAuth(err) {
				s.storeValue(l.pipeline, w) // not sent again on start
			}
			delete(p.failing, w.ID)
			log.Error().Err(err).Msgf("%s: refused by the forge", what)
		default:
			s.setSent(w.ID, "") // the forge may have stored it anyway
			fl := fail(p.failing[w.ID], value)
			p.failing[w.ID] = fl
			if fl.expired(s.policy.Window) {
				// not recorded: the next start sends it
				log.Error().Err(err).Msgf("%s: gave up after %d attempts in %s", what, fl.attempts, time.Since(fl.since).Round(time.Second))
				delete(p.failing, w.ID)
				result = gaveUp
				break
			}
			if fl.attempts == 1 {
				log.Warn().Err(err).Msgf("%s: failed, trying again", what)
			}
			after = max(after, wait)
			retry.workflows[w.ID] = true
		}
		s.done(w.ID, result)
		countStatus(result)
	}
	for id := range todo.workflows {
		if !seen[id] {
			s.done(id, refused) // the workflow is no more
		}
	}
	return retry, after
}

// errGone: what the statuses of a pipeline need is not in the store, so
// they cannot be sent; load logged why.
var errGone = errors.New("gone")

// loaded is what the statuses of a pipeline need, as the store holds it.
type loaded struct {
	pipeline  *model.Pipeline
	workflows []*model.Workflow
	repo      *model.Repo
	user      *model.User
	forge     forge.Forge
}

// marked returns the marked workflows; none of a nil loaded.
func (l *loaded) marked(todo *marks) []*model.Workflow {
	if l == nil {
		return nil
	}
	var out []*model.Workflow
	for _, w := range l.workflows {
		if todo.all || todo.workflows[w.ID] {
			out = append(out, w)
		}
	}
	return out
}

// load reads what the statuses of the pipeline need. With errGone it
// returns the pipeline and its workflows if they are there.
func (s *Sender) load(pipelineID int64) (*loaded, error) {
	pipeline, err := s.store.GetPipeline(pipelineID)
	if errors.Is(err, store_types.ErrRecordNotExist) {
		log.Debug().Msgf("statuses of pipeline %d: the pipeline was deleted", pipelineID)
		return nil, errGone
	}
	if err != nil {
		return nil, err
	}
	workflows, err := s.store.WorkflowList(pipeline)
	if err != nil {
		return nil, err
	}
	l := &loaded{pipeline: pipeline, workflows: workflows}
	l.repo, err = s.store.GetRepo(pipeline.RepoID)
	if errors.Is(err, store_types.ErrRecordNotExist) {
		log.Error().Msgf("statuses of pipeline %d: its repository %d is not in the store", pipelineID, pipeline.RepoID)
		return l, errGone
	}
	if err != nil {
		return nil, err
	}
	l.user, err = s.store.GetUser(l.repo.UserID)
	if errors.Is(err, store_types.ErrRecordNotExist) {
		log.Error().Msgf("statuses of pipeline %d: user %d of repository %s is not in the store", pipelineID, l.repo.UserID, l.repo.FullName)
		return l, errGone
	}
	if err != nil {
		return nil, err
	}
	l.forge, err = s.forgeOf(l.repo)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.policy.Attempt)
	defer cancel()
	s.refresh(ctx, l.forge, l.user)
	return l, nil
}

// storeValue records in the store the status the forge took, or refused for
// good, so that a start after this one does not send it again; failing that,
// it does.
func (s *Sender) storeValue(pipeline *model.Pipeline, w *model.Workflow) {
	err := s.store.ForgeStatusSet(&model.ForgeStatus{
		WorkflowID: w.ID, PipelineID: pipeline.ID, Value: model.ForgeStatusValue(pipeline.Event, w.State, pipeline.Status),
	})
	if err != nil {
		log.Warn().Err(err).Msgf("statuses of pipeline %d: cannot record the one of workflow %d", pipeline.ID, w.ID)
	}
}

func (s *Sender) sentValue(workflowID int64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent[workflowID]
}

// setSent records the status the forge took for the workflow; "" forgets it.
func (s *Sender) setSent(workflowID int64, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if value == "" {
		delete(s.sent, workflowID)
		return
	}
	if len(s.sent) >= maxSent {
		clear(s.sent)
	}
	s.sent[workflowID] = value
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(max(d, 0))
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
