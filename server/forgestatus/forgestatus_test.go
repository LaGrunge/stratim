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

package forgestatus

import (
	"cmp"
	"context"
	"errors"
	"math"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	store_types "go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// fastPolicy is the default in milliseconds.
var fastPolicy = Policy{
	Attempt:   time.Second,
	FirstWait: 5 * time.Millisecond,
	MaxWait:   20 * time.Millisecond,
	Window:    time.Second,
	Senders:   2,

	ResendSince: time.Hour,
	ResendMax:   100,
	ResendEvery: time.Millisecond,
}

// fakeStore holds one repository, its user and its pipelines.
type fakeStore struct {
	mu        sync.Mutex
	pipelines map[int64]*model.Pipeline
	workflows map[int64][]*model.Workflow // pipeline ID -> workflows
	fail      int                         // fail this many next reads of a pipeline
	noRepo    bool                        // the repository is not in the store
	taken     map[int64]string            // workflow ID -> the status recorded
	befores   []int64                     // the cursors candidates were read from
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		pipelines: make(map[int64]*model.Pipeline),
		workflows: make(map[int64][]*model.Workflow),
		taken:     make(map[int64]string),
	}
}

func (s *fakeStore) ForgeStatusSet(status *model.ForgeStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.taken[status.WorkflowID] = status.Value
	return nil
}

func (s *fakeStore) recorded(workflowID int64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.taken[workflowID]
}

func (s *fakeStore) ForgeStatusCandidates(since, before int64, limit int) ([]*model.ForgeStatusCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.befores = append(s.befores, before)
	var out []*model.ForgeStatusCandidate
	for id, p := range s.pipelines {
		if p.Created < since {
			continue
		}
		for _, w := range s.workflows[id] {
			if before == 0 || w.ID < before {
				out = append(out, &model.ForgeStatusCandidate{
					WorkflowID: w.ID, PipelineID: id, State: w.State, PipelineEvent: p.Event, PipelineStatus: p.Status, Value: s.taken[w.ID],
				})
			}
		}
	}
	slices.SortFunc(out, func(a, b *model.ForgeStatusCandidate) int { return cmp.Compare(b.WorkflowID, a.WorkflowID) })
	return out[:min(limit, len(out))], nil
}

func (s *fakeStore) GetPipeline(id int64) (*model.Pipeline, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail > 0 {
		s.fail--
		return nil, errors.New("database unavailable")
	}
	p, ok := s.pipelines[id]
	if !ok {
		return nil, store_types.ErrRecordNotExist
	}
	c := *p
	return &c, nil
}

func (s *fakeStore) GetRepo(id int64) (*model.Repo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.noRepo {
		return nil, store_types.ErrRecordNotExist
	}
	return &model.Repo{ID: id, UserID: 1, FullName: "octocat/hello"}, nil
}

func (*fakeStore) GetUser(id int64) (*model.User, error) { return &model.User{ID: id}, nil }

func (s *fakeStore) WorkflowList(p *model.Pipeline) ([]*model.Workflow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*model.Workflow
	for _, w := range s.workflows[p.ID] {
		c := *w
		out = append(out, &c)
	}
	return out, nil
}

// set puts the pipeline in the store with the workflows in the states given.
func (s *fakeStore) set(pipelineID int64, status model.StatusValue, states ...model.StatusValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pipelines[pipelineID] = &model.Pipeline{ID: pipelineID, RepoID: 1, Number: pipelineID, Status: status, Created: time.Now().Unix()}
	s.workflows[pipelineID] = nil
	for i, state := range states {
		s.workflows[pipelineID] = append(s.workflows[pipelineID], &model.Workflow{
			ID: pipelineID*100 + int64(i), PipelineID: pipelineID, Name: "w", State: state,
		})
	}
}

// fakeForge records the statuses it took, per workflow, and answers with
// the errors queued.
type fakeForge struct {
	mu      sync.Mutex
	taken   map[int64][]model.StatusValue
	calls   int
	errs    []error
	block   chan struct{} // when set, every request waits for it to close
	reqTime []time.Time
	order   []int64
}

func (f *fakeForge) status(ctx context.Context, _ *model.User, _ *model.Repo, _ *model.Pipeline, w *model.Workflow) error {
	f.mu.Lock()
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.reqTime = append(f.reqTime, time.Now())
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return err
		}
	}
	if f.taken == nil {
		f.taken = make(map[int64][]model.StatusValue)
	}
	f.taken[w.ID] = append(f.taken[w.ID], w.State)
	f.order = append(f.order, w.ID)
	return nil
}

// Order returns the workflows whose statuses the forge took, in order.
func (f *fakeForge) Order() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.order...)
}

func (f *fakeForge) Taken(workflowID int64) []model.StatusValue {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.StatusValue(nil), f.taken[workflowID]...)
}

func (f *fakeForge) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func newTestSender(t *testing.T, policy Policy, errs ...error) (*Sender, *fakeStore, *fakeForge) {
	t.Helper()
	st := newFakeStore()
	ff := &fakeForge{errs: errs}
	return startSender(t, st, ff, policy), st, ff
}

// startSender starts a sender on the store and the forge, as a server does.
func startSender(t *testing.T, st *fakeStore, ff *fakeForge, policy Policy) *Sender {
	t.Helper()
	m := forge_mocks.NewMockForge(t)
	m.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(ff.status).Maybe()
	ctx, cancel := context.WithCancelCause(t.Context())
	s := New(ctx, st, func(*model.Repo) (forge.Forge, error) { return m, nil },
		func(context.Context, forge.Forge, *model.User) {}, policy)
	t.Cleanup(func() {
		cancel(nil)
		s.Wait()
	})
	return s
}

// idle waits for every sender to finish.
func idle(t *testing.T, s *Sender) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("senders still running")
	}
}

var errDown = errors.New("502 bad gateway")

func TestPipelineSendsEveryWorkflow(t *testing.T) {
	s, st, ff := newTestSender(t, fastPolicy)
	st.set(1, model.StatusPending, model.StatusPending, model.StatusPending)
	s.Pipeline(1)
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusPending}, ff.Taken(100))
	assert.Equal(t, []model.StatusValue{model.StatusPending}, ff.Taken(101))
}

func TestWorkflowSendsOnlyThatWorkflow(t *testing.T) {
	s, st, ff := newTestSender(t, fastPolicy)
	st.set(1, model.StatusRunning, model.StatusRunning, model.StatusPending)
	s.Workflow(1, 100)
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusRunning}, ff.Taken(100))
	assert.Empty(t, ff.Taken(101))
}

// TestSameStatusNotSentTwice: the server marks a workflow several times
// while its state stays the same.
func TestSameStatusNotSentTwice(t *testing.T) {
	s, st, ff := newTestSender(t, fastPolicy)
	st.set(1, model.StatusPending, model.StatusPending)
	for range 3 {
		s.Workflow(1, 100)
		idle(t, s)
	}
	s.Pipeline(1)
	idle(t, s)
	assert.Equal(t, 1, ff.Calls())

	st.set(1, model.StatusRunning, model.StatusRunning)
	s.Workflow(1, 100)
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusPending, model.StatusRunning}, ff.Taken(100))
}

// TestRetriedUntilTaken: a status that fails is sent again.
func TestRetriedUntilTaken(t *testing.T) {
	s, st, ff := newTestSender(t, fastPolicy, errDown, errDown, errDown)
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(1, 100)
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, ff.Taken(100))
	assert.Equal(t, 4, ff.Calls())
}

// TestNewestSentAfterOutage: changes made while the forge fails collapse
// into the newest, and that is what the forge ends with.
func TestNewestSentAfterOutage(t *testing.T) {
	policy := fastPolicy
	policy.FirstWait, policy.MaxWait = 50*time.Millisecond, 50*time.Millisecond
	s, st, ff := newTestSender(t, policy, errDown)
	st.set(1, model.StatusRunning, model.StatusRunning)
	s.Workflow(1, 100) // fails, waits
	require.Eventually(t, func() bool { return ff.Calls() == 1 }, time.Second, time.Millisecond)

	st.set(1, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(1, 100)
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, ff.Taken(100), "running is stale by then")
}

// TestNewestLastWhileSending: a change during a slow request is sent after
// it, never before.
func TestNewestLastWhileSending(t *testing.T) {
	s, st, ff := newTestSender(t, fastPolicy)
	ff.block = make(chan struct{})
	st.set(1, model.StatusRunning, model.StatusRunning)
	s.Workflow(1, 100)
	time.Sleep(20 * time.Millisecond) // the sender is in the request

	st.set(1, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(1, 100) // does not wait for the forge
	ff.mu.Lock()
	close(ff.block)
	ff.block = nil
	ff.mu.Unlock()
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusRunning, model.StatusSuccess}, ff.Taken(100))
}

// TestFinalErrorNotRetried: a status the forge refused is not sent again.
func TestFinalErrorNotRetried(t *testing.T) {
	s, st, ff := newTestSender(t, fastPolicy, &types.StatusError{Err: errors.New("422"), Final: true})
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(1, 100)
	idle(t, s)
	assert.Equal(t, 1, ff.Calls())
	assert.Empty(t, ff.Taken(100))

	// nor remembered as taken: marking it again sends it
	s.Workflow(1, 100)
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, ff.Taken(100))
}

func TestNotImplementedNotRetried(t *testing.T) {
	s, st, ff := newTestSender(t, fastPolicy, types.ErrNotImplemented)
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(1, 100)
	idle(t, s)
	assert.Equal(t, 1, ff.Calls())
}

// TestRetryAfterHonoured: the next attempt waits as long as the forge asked.
func TestRetryAfterHonoured(t *testing.T) {
	const after = 150 * time.Millisecond
	s, st, ff := newTestSender(t, fastPolicy, &types.StatusError{Err: errors.New("rate limited"), RetryAfter: after})
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(1, 100)
	idle(t, s)
	require.Len(t, ff.reqTime, 2)
	assert.GreaterOrEqual(t, ff.reqTime[1].Sub(ff.reqTime[0]), after)
}

// TestGivesUpAfterWindow: a forge that keeps failing is not tried forever.
func TestGivesUpAfterWindow(t *testing.T) {
	policy := fastPolicy
	policy.Window = 100 * time.Millisecond
	errs := make([]error, 1000)
	for i := range errs {
		errs[i] = errDown
	}
	s, st, ff := newTestSender(t, policy, errs...)
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	start := time.Now()
	s.Workflow(1, 100)
	idle(t, s)
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.Greater(t, ff.Calls(), 1)
	assert.Empty(t, ff.Taken(100))
}

// TestStoreFailureRetried: a pipeline that could not be read is read again.
func TestStoreFailureRetried(t *testing.T) {
	s, st, ff := newTestSender(t, fastPolicy)
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	st.fail = 2
	s.Pipeline(1)
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, ff.Taken(100))
}

// TestStoreFailureGivesUp: a store that keeps failing is not tried forever.
func TestStoreFailureGivesUp(t *testing.T) {
	policy := fastPolicy
	policy.Window = 100 * time.Millisecond
	s, st, ff := newTestSender(t, policy)
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	st.fail = 1 << 30
	s.Pipeline(1)
	idle(t, s)
	assert.Zero(t, ff.Calls())
}

func TestDeletedPipelineDropped(t *testing.T) {
	s, _, ff := newTestSender(t, fastPolicy)
	s.Pipeline(42)
	idle(t, s)
	assert.Zero(t, ff.Calls())
}

// TestStopsWithTheServer: senders end when the server stops, even with a
// status failing.
func TestStopsWithTheServer(t *testing.T) {
	st := newFakeStore()
	ff := &fakeForge{block: make(chan struct{})}
	m := forge_mocks.NewMockForge(t)
	m.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(ff.status).Maybe()
	ctx, cancel := context.WithCancelCause(t.Context())
	s := New(ctx, st, func(*model.Repo) (forge.Forge, error) { return m, nil },
		func(context.Context, forge.Forge, *model.User) {}, fastPolicy)
	st.set(1, model.StatusRunning, model.StatusRunning)
	st.set(2, model.StatusRunning, model.StatusRunning)
	st.set(3, model.StatusRunning, model.StatusRunning)
	s.Pipeline(1)
	s.Pipeline(2)
	s.Pipeline(3) // waits for a slot: Senders is 2
	time.Sleep(20 * time.Millisecond)
	cancel(nil)
	idle(t, s)
	assert.Zero(t, ff.Calls())
}

// TestSendersBounded: no more pipelines are synced at once than Senders.
func TestSendersBounded(t *testing.T) {
	s, st, ff := newTestSender(t, fastPolicy)
	ff.block = make(chan struct{})
	for id := int64(1); id <= 5; id++ {
		st.set(id, model.StatusRunning, model.StatusRunning)
		s.Pipeline(id)
	}
	time.Sleep(20 * time.Millisecond)
	assert.Len(t, s.slots, fastPolicy.Senders)
	ff.mu.Lock()
	close(ff.block)
	ff.block = nil
	ff.mu.Unlock()
	idle(t, s)
	assert.Equal(t, 5, ff.Calls())
}

// TestPanicFreesThePipeline: a sender that panics does not leave its
// pipeline without one.
func TestPanicFreesThePipeline(t *testing.T) {
	st := newFakeStore()
	ff := &fakeForge{}
	m := forge_mocks.NewMockForge(t)
	first := true
	m.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return( //nolint:forbidigo // tests a panic
		func(ctx context.Context, u *model.User, r *model.Repo, p *model.Pipeline, w *model.Workflow) error {
			if first {
				first = false
				panic("boom") //nolint:forbidigo // tests a panic
			}
			return ff.status(ctx, u, r, p, w)
		},
	).Maybe()
	s := New(t.Context(), st, func(*model.Repo) (forge.Forge, error) { return m, nil },
		func(context.Context, forge.Forge, *model.User) {}, fastPolicy)
	st.set(1, model.StatusRunning, model.StatusRunning)
	s.Pipeline(1)
	idle(t, s)
	s.Pipeline(1)
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusRunning}, ff.Taken(100))
	assert.Empty(t, s.slots, "the slot was given back")
}

// TestMarkDuringPanicSent: a mark that came while the sender panicked is
// sent without another mark.
func TestMarkDuringPanicSent(t *testing.T) {
	st := newFakeStore()
	ff := &fakeForge{}
	m := forge_mocks.NewMockForge(t)
	inside, release := make(chan struct{}), make(chan struct{})
	first := true
	m.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return( //nolint:forbidigo // tests a panic
		func(ctx context.Context, u *model.User, r *model.Repo, p *model.Pipeline, w *model.Workflow) error {
			if first {
				first = false
				close(inside)
				<-release
				panic("boom") //nolint:forbidigo // tests a panic
			}
			return ff.status(ctx, u, r, p, w)
		},
	).Maybe()
	s := New(t.Context(), st, func(*model.Repo) (forge.Forge, error) { return m, nil },
		func(context.Context, forge.Forge, *model.User) {}, fastPolicy)
	st.set(1, model.StatusRunning, model.StatusRunning)
	s.Workflow(1, 100)
	<-inside
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(1, 100) // the agent's done, while the sender is in the request
	close(release)
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, ff.Taken(100))
}

// TestNewStatusGetsItsOwnWindow: a status that replaces one that failed for
// most of the window is still tried for a whole window.
func TestNewStatusGetsItsOwnWindow(t *testing.T) {
	policy := fastPolicy
	policy.Window = 200 * time.Millisecond
	errs := make([]error, 0, 1000)
	for range cap(errs) {
		errs = append(errs, errDown)
	}
	s, st, ff := newTestSender(t, policy, errs...)
	st.set(1, model.StatusRunning, model.StatusRunning)
	s.Workflow(1, 100)
	time.Sleep(150 * time.Millisecond) // running has failed for most of its window

	st.set(1, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(1, 100)
	time.Sleep(100 * time.Millisecond) // past the window of running
	ff.mu.Lock()
	ff.errs = nil // the forge is back
	ff.mu.Unlock()
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, ff.Taken(100))
}

// TestRetryAfterCapped: a forge asking to wait longer than the window does
// not hold the pipeline's statuses that long.
func TestRetryAfterCapped(t *testing.T) {
	policy := fastPolicy
	policy.Window = 100 * time.Millisecond
	s, st, ff := newTestSender(t, policy, &types.StatusError{Err: errors.New("rate limited"), RetryAfter: time.Hour})
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	start := time.Now()
	s.Workflow(1, 100)
	idle(t, s)
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.Equal(t, 2, ff.Calls())
}

// TestZeroPolicy: fields left zero take the defaults, but resend is off.
func TestZeroPolicy(t *testing.T) {
	s, st, ff := newTestSender(t, Policy{})
	want := DefaultPolicy
	want.ResendSince, want.ResendMax = 0, 0
	assert.Equal(t, want, s.policy)
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(1, 100)
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, ff.Taken(100))
}

// TestRepoGone: statuses of a pipeline whose repository is not in the store
// are dropped, not tried for the window.
func TestRepoGone(t *testing.T) {
	s, st, ff := newTestSender(t, fastPolicy)
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	st.noRepo = true
	s.Pipeline(1)
	idle(t, s)
	assert.Zero(t, ff.Calls())
}

func TestNilSender(t *testing.T) {
	var s *Sender
	s.Pipeline(1)
	s.Workflow(1, 2)
}

// TestRetryAfterHuge: a wait asked for past any reason does not overflow
// into no wait at all.
func TestRetryAfterHuge(t *testing.T) {
	policy := fastPolicy
	policy.Window = 100 * time.Millisecond
	s, st, ff := newTestSender(t, policy, &types.StatusError{Err: errors.New("rate limited"), RetryAfter: math.MaxInt64})
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(1, 100)
	idle(t, s)
	assert.Equal(t, 2, ff.Calls(), "waited, then sent")
	require.Len(t, ff.reqTime, 2)
	assert.GreaterOrEqual(t, ff.reqTime[1].Sub(ff.reqTime[0]), policy.Window/2, "waited")
	for _, after := range []time.Duration{math.MaxInt64, math.MaxInt64 - 1, time.Hour, time.Nanosecond} {
		wait := s.retryWait(time.Millisecond, after)
		assert.Positive(t, wait)
		assert.LessOrEqual(t, wait, policy.Window)
	}
	assert.Equal(t, time.Millisecond, s.retryWait(time.Millisecond, 0), "the backoff when the forge asks nothing")
}

// TestOutcomesRecorded: statuses taken, refused, and not implemented by the
// forge are recorded, so that a start does not send them again.
func TestOutcomesRecorded(t *testing.T) {
	s, st, ff := newTestSender(t, fastPolicy, nil, &types.StatusError{Err: errors.New("422"), Final: true}, types.ErrNotImplemented)
	st.set(1, model.StatusRunning, model.StatusRunning, model.StatusRunning, model.StatusRunning)
	s.Pipeline(1)
	idle(t, s)
	for _, id := range []int64{100, 101, 102} {
		assert.Equal(t, "running", st.recorded(id), "workflow %d", id)
	}
	assert.Equal(t, 3, ff.Calls())
}

// TestGivenUpNotRecorded: a status given up on is not recorded, and the
// next start delivers it.
func TestGivenUpNotRecorded(t *testing.T) {
	policy := fastPolicy
	policy.Window = 50 * time.Millisecond
	errs := make([]error, 0, 1000)
	for range cap(errs) {
		errs = append(errs, errDown)
	}
	s, st, ff := newTestSender(t, policy, errs...)
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(1, 100)
	idle(t, s)
	assert.Empty(t, ff.Taken(100))
	assert.Empty(t, st.recorded(100))

	up := &fakeForge{}
	s = startSender(t, st, up, policy)
	s.Resend(t.Context())
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, up.Taken(100))
	assert.Equal(t, resendCounts{queued: 1, taken: 1}, s.resent)
}

// TestRepoGoneRecorded: statuses that cannot be sent, their repository
// gone, are recorded as done with.
func TestRepoGoneRecorded(t *testing.T) {
	s, st, _ := newTestSender(t, fastPolicy)
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	st.noRepo = true
	s.Resend(t.Context())
	idle(t, s)
	assert.Equal(t, "success", st.recorded(100))
	assert.Equal(t, resendCounts{queued: 1, refused: 1}, s.resent)
}

// TestUnsentSentAfterRestart: statuses not through when the server stopped
// are sent by the next one; those the forge took are not sent again.
func TestUnsentSentAfterRestart(t *testing.T) {
	st := newFakeStore()
	down := &fakeForge{block: make(chan struct{})} // never answers
	ctx, cancel := context.WithCancelCause(t.Context())
	m := forge_mocks.NewMockForge(t)
	m.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(down.status).Maybe()
	first := New(ctx, st, func(*model.Repo) (forge.Forge, error) { return m, nil },
		func(context.Context, forge.Forge, *model.User) {}, fastPolicy)

	st.set(1, model.StatusSuccess, model.StatusSuccess) // taken before the outage
	st.taken[100] = "success"
	st.set(2, model.StatusFailure, model.StatusSuccess, model.StatusFailure) // finished while the forge was down
	first.Pipeline(2)
	time.Sleep(20 * time.Millisecond)
	cancel(nil) // the server stops
	idle(t, first)
	assert.Zero(t, down.Calls())
	first.Pipeline(2) // a mark after it stopped is dropped
	first.Resend(t.Context())
	idle(t, first)
	assert.Empty(t, first.marked)
	assert.Empty(t, first.busy)

	up := &fakeForge{}
	s := startSender(t, st, up, fastPolicy)
	s.Resend(t.Context())
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, up.Taken(200))
	assert.Equal(t, []model.StatusValue{model.StatusFailure}, up.Taken(201))
	assert.Equal(t, 2, up.Calls(), "the status of pipeline 1 was taken before")
	assert.Equal(t, resendCounts{queued: 2, taken: 2}, s.resent)

	// and once taken, they are not sent by the start after
	again := &fakeForge{}
	s = startSender(t, st, again, fastPolicy)
	s.Resend(t.Context())
	idle(t, s)
	assert.Zero(t, again.Calls())
}

// TestChangedAfterTakenSentAfterRestart: a workflow whose state changed after
// its forge took a status gets the new one.
func TestChangedAfterTakenSentAfterRestart(t *testing.T) {
	st := newFakeStore()
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	st.taken[100] = "running"
	ff := &fakeForge{}
	s := startSender(t, st, ff, fastPolicy)
	s.Resend(t.Context())
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, ff.Taken(100))
}

// TestResendBounded: only workflows of recent pipelines, and at most
// ResendMax of them, newest first, are sent again; none with resend off.
func TestResendBounded(t *testing.T) {
	st := newFakeStore()
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	st.pipelines[1].Created = time.Now().Add(-48 * time.Hour).Unix()
	st.set(2, model.StatusSuccess, model.StatusSuccess, model.StatusSuccess, model.StatusSuccess)
	policy := fastPolicy
	policy.ResendSince, policy.ResendMax = 24*time.Hour, 2
	ff := &fakeForge{}
	s := startSender(t, st, ff, policy)
	s.Resend(t.Context())
	idle(t, s)
	assert.Equal(t, 2, ff.Calls())
	assert.Empty(t, ff.Taken(100), "pipeline 1 is too old")
	assert.Empty(t, ff.Taken(200), "the oldest of pipeline 2 is past ResendMax")

	for _, off := range []Policy{{ResendSince: time.Hour}, {ResendMax: 100}} {
		ff := &fakeForge{}
		s := startSender(t, st, ff, off)
		s.Resend(t.Context())
		idle(t, s)
		assert.Zero(t, ff.Calls())
	}
}

// TestResendPaced: resent statuses go one every ResendEvery at most, and a
// live status does not wait behind them.
func TestResendPaced(t *testing.T) {
	st := newFakeStore()
	for id := int64(1); id <= 5; id++ {
		st.set(id, model.StatusSuccess, model.StatusSuccess)
	}
	st.set(9, model.StatusRunning, model.StatusRunning)
	st.taken[900] = "running"
	policy := fastPolicy
	policy.ResendEvery = 60 * time.Millisecond
	ff := &fakeForge{}
	s := startSender(t, st, ff, policy)
	start := time.Now()
	s.Resend(t.Context())
	require.Eventually(t, func() bool { return ff.Calls() == 1 }, time.Second, time.Millisecond)

	st.set(9, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(9, 900) // live, while the resend goes on
	require.Eventually(t, func() bool { return len(ff.Taken(900)) == 1 }, time.Second, time.Millisecond)
	assert.Less(t, ff.Calls(), 5, "sent before the resend is through")

	idle(t, s)
	assert.Equal(t, 6, ff.Calls())
	assert.GreaterOrEqual(t, time.Since(start), 4*policy.ResendEvery)
}

// TestResendAndLiveShareAPipeline: a live mark of a pipeline whose resent
// status waits for the resend slot is not held back by it.
func TestResendAndLiveShareAPipeline(t *testing.T) {
	st := newFakeStore()
	st.set(2, model.StatusSuccess, model.StatusSuccess, model.StatusRunning)
	ff := &fakeForge{}
	s := startSender(t, st, ff, fastPolicy)
	s.resendSlot <- struct{}{} // another resent status is in the forge

	m := newMarks()
	m.workflows[200] = true
	m.resend = true
	s.mark(2, m)
	time.Sleep(10 * time.Millisecond)
	s.Workflow(2, 201)
	require.Eventually(t, func() bool { return len(ff.Taken(201)) == 1 }, time.Second, time.Millisecond)
	<-s.resendSlot
	idle(t, s)
	assert.Len(t, ff.Taken(200), 1)
}

func TestMarksResend(t *testing.T) {
	resent := func() *marks {
		m := newMarks()
		m.workflows[1] = true
		m.resend = true
		return m
	}
	live := newMarks()
	live.workflows[2] = true

	m := newMarks()
	m.add(resent())
	assert.True(t, m.resend)
	m.add(resent())
	assert.True(t, m.resend)
	m.add(live)
	assert.False(t, m.resend, "a live mark makes it live")
	m.add(resent())
	assert.False(t, m.resend)
}

func resentMark(s *Sender, pipelineID, workflowID int64) {
	s.markResent(&model.ForgeStatusCandidate{WorkflowID: workflowID, PipelineID: pipelineID})
}

// TestResentDoneWith: a resent status is done with on every way out, so
// that none stays pending.
func TestResentDoneWith(t *testing.T) {
	t.Run("sent live before", func(t *testing.T) {
		s, st, ff := newTestSender(t, fastPolicy)
		st.set(1, model.StatusSuccess, model.StatusSuccess)
		s.Workflow(1, 100)
		idle(t, s)
		resentMark(s, 1, 100)
		idle(t, s)
		assert.Equal(t, 1, ff.Calls())
		assert.Zero(t, s.resendingNow())
		assert.Equal(t, resendCounts{queued: 1, taken: 1}, s.resent)
	})
	t.Run("workflow deleted", func(t *testing.T) {
		s, st, ff := newTestSender(t, fastPolicy)
		st.set(1, model.StatusSuccess, model.StatusSuccess)
		resentMark(s, 1, 999)
		idle(t, s)
		assert.Zero(t, ff.Calls())
		assert.Zero(t, s.resendingNow())
		assert.Equal(t, resendCounts{queued: 1, refused: 1}, s.resent)
	})
	t.Run("pipeline deleted", func(t *testing.T) {
		s, _, ff := newTestSender(t, fastPolicy)
		resentMark(s, 42, 4200)
		idle(t, s)
		assert.Zero(t, ff.Calls())
		assert.Zero(t, s.resendingNow())
		assert.Equal(t, resendCounts{queued: 1, refused: 1}, s.resent)
	})
	t.Run("sender panicked", func(t *testing.T) {
		st := newFakeStore()
		m := forge_mocks.NewMockForge(t)
		m.On("Status", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return( //nolint:forbidigo // tests a panic
			func(context.Context, *model.User, *model.Repo, *model.Pipeline, *model.Workflow) error {
				panic("boom") //nolint:forbidigo // tests a panic
			},
		).Maybe()
		s := New(t.Context(), st, func(*model.Repo) (forge.Forge, error) { return m, nil },
			func(context.Context, forge.Forge, *model.User) {}, fastPolicy)
		st.set(1, model.StatusSuccess, model.StatusSuccess)
		resentMark(s, 1, 100)
		idle(t, s)
		assert.Zero(t, s.resendingNow())
		assert.Equal(t, resendCounts{queued: 1, gaveUp: 1}, s.resent)
	})
}

// TestResendNotInLiveSlots: resent statuses never take a slot of the live
// ones, and live ones do not wait for the resend slot.
func TestResendNotInLiveSlots(t *testing.T) {
	policy := fastPolicy
	policy.Senders = 1
	s, st, ff := newTestSender(t, policy)
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	st.set(2, model.StatusSuccess, model.StatusSuccess)

	s.slots <- struct{}{} // the live slots are all taken
	resentMark(s, 1, 100)
	require.Eventually(t, func() bool { return len(ff.Taken(100)) == 1 }, time.Second, time.Millisecond)
	<-s.slots

	s.resendSlot <- struct{}{} // a resent status is in the forge
	s.Workflow(2, 200)
	require.Eventually(t, func() bool { return len(ff.Taken(200)) == 1 }, 10*policy.FirstWait, time.Millisecond)
	<-s.resendSlot
	idle(t, s)
}

// TestResendPages: candidates are read page by page, each workflow sent
// once, newest first.
func TestResendPages(t *testing.T) {
	for _, n := range []int{5, 2} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			st := newFakeStore()
			for id := int64(1); id <= int64(n); id++ {
				st.set(id, model.StatusSuccess, model.StatusSuccess)
			}
			policy := fastPolicy
			policy.Senders = 1 // one at a time: in order
			ff := &fakeForge{}
			s := startSender(t, st, ff, policy)
			s.page = 2
			s.Resend(t.Context())
			idle(t, s)

			var order []int64
			for id := int64(n); id >= 1; id-- {
				assert.Len(t, ff.Taken(id*100), 1, "workflow %d", id*100)
				order = append(order, id*100)
			}
			assert.Equal(t, order, ff.Order(), "newest first")
			wantBefores := []int64{0}
			for i := 2; i <= n; i += 2 {
				wantBefores = append(wantBefores, order[i-1])
			}
			assert.Equal(t, wantBefores, st.befores)
			assert.Equal(t, resendCounts{queued: n, taken: n}, s.resent)
		})
	}
}

// TestResendInFlightBounded: no more resent statuses are pending at once
// than Senders, with a forge that does not answer.
func TestResendInFlightBounded(t *testing.T) {
	st := newFakeStore()
	for id := int64(1); id <= 5; id++ {
		st.set(id, model.StatusSuccess, model.StatusSuccess)
	}
	ff := &fakeForge{block: make(chan struct{})}
	s := startSender(t, st, ff, fastPolicy)
	s.Resend(t.Context())
	deadline := time.Now().Add(100 * time.Millisecond)
	most := 0
	for time.Now().Before(deadline) {
		n := s.resendingNow()
		assert.LessOrEqual(t, n, fastPolicy.Senders)
		most = max(most, n)
		time.Sleep(time.Millisecond)
	}
	assert.Equal(t, fastPolicy.Senders, most)
	ff.mu.Lock()
	close(ff.block)
	ff.block = nil
	ff.mu.Unlock()
	idle(t, s)
	assert.Equal(t, 5, ff.Calls())
}

// TestResendRetriesPaced: the retries of a resent status wait their turn
// too.
func TestResendRetriesPaced(t *testing.T) {
	policy := fastPolicy
	policy.ResendEvery = 30 * time.Millisecond
	s, st, ff := newTestSender(t, policy, errDown, errDown, errDown)
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	resentMark(s, 1, 100)
	idle(t, s)
	require.Len(t, ff.reqTime, 4)
	for i := 1; i < len(ff.reqTime); i++ {
		assert.GreaterOrEqual(t, ff.reqTime[i].Sub(ff.reqTime[i-1]), policy.ResendEvery-5*time.Millisecond)
	}
}

func TestResendSinceClamped(t *testing.T) {
	policy := fastPolicy
	policy.ResendSince = 1000 * time.Hour
	s, _, _ := newTestSender(t, policy)
	assert.Equal(t, MaxResendSince, s.policy.ResendSince)
}

// TestResendOfAPipelinePaced: the resent statuses of several workflows of a
// pipeline go one at a time, not at once.
func TestResendOfAPipelinePaced(t *testing.T) {
	policy := fastPolicy
	policy.ResendEvery = 30 * time.Millisecond
	s, st, ff := newTestSender(t, policy, nil, errDown)
	st.set(1, model.StatusSuccess, model.StatusSuccess, model.StatusSuccess, model.StatusSuccess)
	for id := int64(100); id <= 102; id++ {
		resentMark(s, 1, id)
	}
	idle(t, s)
	require.Len(t, ff.reqTime, 4, "three, one of them again")
	for i := 1; i < len(ff.reqTime); i++ {
		assert.GreaterOrEqual(t, ff.reqTime[i].Sub(ff.reqTime[i-1]), policy.ResendEvery-5*time.Millisecond)
	}
	assert.Equal(t, resendCounts{queued: 3, taken: 3}, s.resent)
}

// TestPipelineStatusOnlyForDeployments: the end of a pipeline does not make
// the statuses taken of its other workflows stale; but for a deployment,
// whose status a forge shows, it does, and the server marks them all.
func TestPipelineStatusOnlyForDeployments(t *testing.T) {
	for _, event := range []model.WebhookEvent{model.EventPush, model.EventDeploy} {
		t.Run(string(event), func(t *testing.T) {
			st := newFakeStore()
			set := func(status model.StatusValue, states ...model.StatusValue) {
				st.set(1, status, states...)
				st.mu.Lock()
				st.pipelines[1].Event = event
				st.mu.Unlock()
			}
			ff := &fakeForge{}
			s := startSender(t, st, ff, fastPolicy)
			set(model.StatusRunning, model.StatusSuccess, model.StatusRunning)
			s.Workflow(1, 100) // the first is done
			idle(t, s)

			set(model.StatusFailure, model.StatusSuccess, model.StatusFailure)
			if event == model.EventDeploy {
				s.Pipeline(1) // as the server marks a deployment
			} else {
				s.Workflow(1, 101) // the pipeline ends on the second
			}
			idle(t, s)
			if event == model.EventDeploy {
				assert.Len(t, ff.Taken(100), 2, "the deployment's new status")
			} else {
				assert.Len(t, ff.Taken(100), 1)
			}

			again := &fakeForge{}
			s = startSender(t, st, again, fastPolicy)
			s.Resend(t.Context())
			idle(t, s)
			assert.Zero(t, again.Calls(), "nothing stale to send again")
		})
	}
}

// TestAuthRefusalNotRecorded: a status refused for the user's credentials
// is not tried again now, nor recorded: once the user logs in again, the
// next start sends it.
func TestAuthRefusalNotRecorded(t *testing.T) {
	s, st, ff := newTestSender(t, fastPolicy, &types.StatusError{Err: errors.New("401"), Final: true, Auth: true})
	st.set(1, model.StatusSuccess, model.StatusSuccess)
	s.Workflow(1, 100)
	idle(t, s)
	assert.Equal(t, 1, ff.Calls())
	assert.Empty(t, st.recorded(100))

	up := &fakeForge{}
	s = startSender(t, st, up, fastPolicy)
	s.Resend(t.Context())
	idle(t, s)
	assert.Equal(t, []model.StatusValue{model.StatusSuccess}, up.Taken(100))
	assert.Equal(t, "success", st.recorded(100))
}
