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

//go:build test

package webhook

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
	store_types "go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// clock is a clock a test moves.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type fixture struct {
	store store.Store
	repo  *model.Repo
	user  *model.User
	forge *forge_mocks.MockForge
	clock *clock
	inbox *Inbox
}

// newFixture is an inbox on a store with an active repo of a mock forge.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	s := datastore.NewTestStore(t)
	user := &model.User{ForgeID: 1, ForgeRemoteID: "1", Login: "owner", Hash: "hash"}
	require.NoError(t, s.CreateUser(user))
	repo := &model.Repo{
		ForgeID: 1, ForgeRemoteID: "10", Owner: "owner", Name: "repo", FullName: "owner/repo",
		IsActive: true, UserID: user.ID, AllowPull: true, WebhookSecret: "secret",
	}
	require.NoError(t, s.CreateRepo(repo))

	f := forge_mocks.NewMockForge(t)
	m := manager_mocks.NewMockManager(t)
	m.On("ForgeFromRepo", mock.Anything).Return(f, nil).Maybe()
	orig := server.Config.Services.Manager
	server.Config.Services.Manager = m
	t.Cleanup(func() { server.Config.Services.Manager = orig })

	// the real time: the store sets the creation of pipelines from it
	c := &clock{now: time.Now().Truncate(time.Second)}
	return &fixture{
		store: s, repo: repo, user: user, forge: f, clock: c,
		inbox: New(s, pipeline.WorkflowEnd{Store: s}, Options{Now: c.Now}),
	}
}

// receive keeps a webhook of the repo with the delivery ID.
func (fx *fixture) receive(t *testing.T, guid string) *model.WebhookDelivery {
	t.Helper()
	header := http.Header{}
	header.Set("X-GitHub-Delivery", guid)
	header.Set("X-GitHub-Event", "push")
	created, err := fx.inbox.Receive(fx.repo, header, []byte(`{"n":1}`))
	require.NoError(t, err)
	require.True(t, created)
	return fx.delivery(t, guid)
}

func (fx *fixture) delivery(t *testing.T, guid string) *model.WebhookDelivery {
	t.Helper()
	for _, d := range fx.deliveries(t) {
		if d.GUID == guid {
			return d
		}
	}
	t.Fatalf("delivery %s not kept", guid)
	return nil
}

// deliveries are all the deliveries kept, by ID.
func (fx *fixture) deliveries(t *testing.T) []*model.WebhookDelivery {
	t.Helper()
	var all []*model.WebhookDelivery
	for id := int64(1); ; id++ {
		d, err := fx.store.WebhookDeliveryGet(id)
		if errors.Is(err, store_types.ErrRecordNotExist) {
			return all
		}
		require.NoError(t, err)
		all = append(all, d)
	}
}

// run dispatches the deliveries due and waits for them.
func (fx *fixture) run(t *testing.T) {
	t.Helper()
	fx.inbox.dispatch(t.Context(), t.Context())
	fx.inbox.wg.Wait()
}

// skipped is a pipeline its commit message skips: Create filters it before
// any forge call.
func skipped() *model.Pipeline {
	return &model.Pipeline{Event: model.EventPush, Branch: "main", Message: "[CI SKIP] docs"}
}

func TestReceiveKeepsTheRequest(t *testing.T) {
	fx := newFixture(t)
	header := http.Header{}
	header.Set("X-Gitea-Delivery", "g-1")
	header.Set("X-Gitea-Event", "push")
	header.Set("Authorization", "Bearer token")
	created, err := fx.inbox.Receive(fx.repo, header, []byte(`{"ref":"main"}`))
	require.NoError(t, err)
	assert.True(t, created)

	d := fx.delivery(t, "g-1")
	assert.Equal(t, "push", d.Event)
	assert.Equal(t, model.DeliveryPending, d.State)
	assert.Equal(t, fx.clock.Now().Unix(), d.NextTry)
	assert.NotContains(t, d.Header, "Authorization")
	body, err := gunzipped(d.Body)
	require.NoError(t, err)
	assert.JSONEq(t, `{"ref":"main"}`, string(body))

	// a forge that gives no delivery ID: kept every time
	for range 2 {
		created, err := fx.inbox.Receive(fx.repo, http.Header{}, []byte(`{}`))
		require.NoError(t, err)
		assert.True(t, created)
	}
	assert.Len(t, fx.deliveries(t), 3)

	_, err = fx.inbox.Receive(fx.repo, http.Header{}, make([]byte, MaxBodyBytes+1))
	assert.ErrorIs(t, err, ErrTooLarge)
}

func TestProcessRetriesUntilTheForgeParses(t *testing.T) {
	fx := newFixture(t)
	fx.forge.EXPECT().Hook(mock.Anything, mock.Anything).Return(nil, nil, errors.New("forge unavailable")).Once()
	fx.receive(t, "g-1")

	fx.run(t)
	d := fx.delivery(t, "g-1")
	assert.Equal(t, model.DeliveryPending, d.State)
	assert.Equal(t, 1, d.Attempts)
	assert.Equal(t, fx.clock.Now().Add(firstRetryDelay).Unix(), d.NextTry)
	assert.Contains(t, d.Error, "forge unavailable")

	// not due yet
	fx.run(t)
	assert.Equal(t, 1, fx.delivery(t, "g-1").Attempts)

	fx.clock.Add(firstRetryDelay)
	fx.forge.EXPECT().Hook(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, r *http.Request) (*model.Repo, *model.Pipeline, error) {
		assert.Equal(t, "push", r.Header.Get("X-GitHub-Event"), "the request as kept")
		return fx.repo, skipped(), nil
	}).Once()
	fx.run(t)
	d = fx.delivery(t, "g-1")
	assert.Equal(t, model.DeliveryIgnored, d.State, "a skipped commit runs nothing")
	assert.Equal(t, 2, d.Attempts)
}

func TestProcessGivesUp(t *testing.T) {
	fx := newFixture(t)
	fx.forge.EXPECT().Hook(mock.Anything, mock.Anything).Return(nil, nil, errors.New("forge unavailable"))
	fx.receive(t, "g-1")
	for range 30 {
		fx.run(t)
		fx.clock.Add(maxRetryDelay)
	}
	d := fx.delivery(t, "g-1")
	assert.Equal(t, model.DeliveryFailed, d.State)
	assert.Less(t, d.Attempts, 30, "given up after an hour")
	assert.Greater(t, d.Attempts, 10)
}

func TestProcessSettles(t *testing.T) {
	fx := newFixture(t)
	other := &model.Repo{ForgeRemoteID: "other", FullName: "owner/other"}
	pull := &model.Pipeline{Event: model.EventPull, Branch: "main"}
	cases := []struct {
		name  string
		repo  *model.Repo
		p     *model.Pipeline
		err   error
		state model.WebhookDeliveryState
	}{
		{name: "ignored event", err: &types.ErrIgnoreEvent{Event: "star"}, state: model.DeliveryIgnored},
		{name: "no pipeline", repo: fx.repo, state: model.DeliveryIgnored},
		{name: "no repo", p: skipped(), state: model.DeliveryFailed},
		{name: "another repo", repo: other, p: skipped(), state: model.DeliveryFailed},
		{name: "skipped", repo: fx.repo, p: skipped(), state: model.DeliveryIgnored},
		{name: "pulls disabled", repo: fx.repo, p: pull, state: model.DeliveryIgnored},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx.repo.AllowPull = tc.p != pull
			require.NoError(t, fx.store.UpdateRepo(fx.repo))
			fx.forge.EXPECT().Hook(mock.Anything, mock.Anything).Return(tc.repo, tc.p, tc.err).Once()
			fx.receive(t, tc.name)
			fx.run(t)
			d := fx.delivery(t, tc.name)
			assert.Equal(t, tc.state, d.State, d.Error)
			assert.Equal(t, 1, d.Attempts)
		})
	}
}

func TestProcessInactiveRepo(t *testing.T) {
	fx := newFixture(t)
	fx.receive(t, "g-1")
	fx.repo.IsActive = false
	require.NoError(t, fx.store.UpdateRepo(fx.repo))
	fx.run(t)
	assert.Equal(t, model.DeliveryIgnored, fx.delivery(t, "g-1").State, "the forge not asked")
}

// A server that stopped after it stored the pipeline of a delivery: the
// next does not make another, the one left half made ends in error.
func TestProcessAfterAStop(t *testing.T) {
	fx := newFixture(t)
	half := &model.Pipeline{RepoID: fx.repo.ID, Status: model.StatusCreated, Event: model.EventPush}
	require.NoError(t, fx.store.CreatePipeline(half))
	d := fx.receive(t, "g-1")

	// claimed by the server that stopped, its claim expired
	claimed, err := fx.store.WebhookDeliveryClaim(d, fx.clock.Now().Add(claimFor).Unix(), fx.clock.Now().Unix())
	require.NoError(t, err)
	require.True(t, claimed)
	d.PipelineID = half.ID
	held, err := fx.store.WebhookDeliveryUpdate(d)
	require.NoError(t, err)
	require.True(t, held)
	fx.run(t)
	assert.Equal(t, model.DeliveryProcessing, fx.delivery(t, "g-1").State, "claimed until it expires")

	fx.clock.Add(claimFor)
	fx.run(t)
	d = fx.delivery(t, "g-1")
	assert.Equal(t, model.DeliveryDone, d.State)
	assert.Equal(t, half.ID, d.PipelineID)
	assert.Equal(t, 2, d.Attempts)
	p, err := fx.store.GetPipeline(half.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusError, p.Status)
}

// One delivery of a repo at a time, in order; other repos meanwhile.
func TestDispatchOnePerRepo(t *testing.T) {
	fx := newFixture(t)
	second := &model.Repo{
		ForgeID: 1, ForgeRemoteID: "20", Owner: "owner", Name: "second", FullName: "owner/second",
		IsActive: true, UserID: fx.user.ID, WebhookSecret: "secret",
	}
	require.NoError(t, fx.store.CreateRepo(second))

	release := make(chan struct{})
	var mu sync.Mutex
	var order []string
	fx.forge.EXPECT().Hook(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, r *http.Request) (*model.Repo, *model.Pipeline, error) {
		mu.Lock()
		order = append(order, r.Header.Get("X-GitHub-Delivery"))
		mu.Unlock()
		<-release
		return nil, nil, &types.ErrIgnoreEvent{}
	})
	fx.receive(t, "a-1")
	fx.receive(t, "a-2")
	repo := fx.repo
	fx.repo = second
	fx.receive(t, "b-1")
	fx.repo = repo

	fx.inbox.dispatch(t.Context(), t.Context())
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 2
	}, 5*time.Second, 10*time.Millisecond)
	mu.Lock()
	assert.ElementsMatch(t, []string{"a-1", "b-1"}, order, "a-2 waits for a-1")
	mu.Unlock()
	assert.Equal(t, model.DeliveryPending, fx.delivery(t, "a-2").State)
	close(release)
	fx.inbox.wg.Wait()
	fx.run(t)
	mu.Lock()
	require.Len(t, order, 3)
	assert.Equal(t, "a-2", order[2], "after a-1")
	mu.Unlock()
	for _, guid := range []string{"a-1", "a-2", "b-1"} {
		assert.Equal(t, model.DeliveryIgnored, fx.delivery(t, guid).State)
	}
}

func TestRunStopsAfterWhatItProcesses(t *testing.T) {
	fx := newFixture(t)
	started := make(chan struct{})
	release := make(chan struct{})
	fx.forge.EXPECT().Hook(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ *http.Request) (*model.Repo, *model.Pipeline, error) {
		close(started)
		<-release
		assert.NoError(t, ctx.Err(), "not stopped with the server")
		return nil, nil, &types.ErrIgnoreEvent{}
	}).Once()
	fx.receive(t, "g-1")

	ctx, cancel := context.WithCancelCause(t.Context())
	done := make(chan struct{})
	go func() {
		fx.inbox.Run(ctx)
		close(done)
	}()
	<-started
	cancel(nil)
	select {
	case <-done:
		t.Fatal("Run returned before the delivery it processes")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-done
	assert.Equal(t, model.DeliveryIgnored, fx.delivery(t, "g-1").State)
}

func TestPurge(t *testing.T) {
	fx := newFixture(t)
	fx.forge.EXPECT().Hook(mock.Anything, mock.Anything).Return(nil, nil, &types.ErrIgnoreEvent{}).Once()
	fx.receive(t, "g-1")
	fx.run(t)
	fx.receive(t, "g-2") // pending: kept

	fx.clock.Add(keepSettled + time.Second)
	fx.inbox.purge()
	known, err := fx.store.WebhookDeliveriesKnown(fx.repo.ForgeID, fx.repo.ID, []string{"g-1", "g-2"})
	require.NoError(t, err)
	assert.Equal(t, []string{"g-2"}, known)
}

func TestRetryDelay(t *testing.T) {
	assert.Equal(t, firstRetryDelay, retryDelay(1))
	assert.Equal(t, 2*firstRetryDelay, retryDelay(2))
	assert.Equal(t, maxRetryDelay, retryDelay(10))
	assert.Equal(t, maxRetryDelay, retryDelay(1000))
}

func TestReceiveHeaders(t *testing.T) {
	fx := newFixture(t)
	// Bitbucket's hook UUID is the webhook's, the same every time: not a
	// delivery's ID
	for _, body := range []string{`{"n":1}`, `{"n":2}`} {
		header := http.Header{}
		header.Set("X-Hook-UUID", "the-webhook")
		header.Set("X-Gitlab-Token", "the-token")
		created, err := fx.inbox.Receive(fx.repo, header, []byte(body))
		require.NoError(t, err)
		assert.True(t, created, "every delivery kept")
	}
	for _, d := range fx.deliveries(t) {
		assert.NotContains(t, d.Header, "X-Gitlab-Token", "the webhook's token is not kept")
	}

	// IDs longer than the column are kept hashed, still deduplicated
	long := strings.Repeat("y", maxGUIDLen+1)
	for i, want := range []bool{true, false} {
		header := http.Header{}
		header.Set("X-GitHub-Delivery", long)
		header.Set("X-GitHub-Event", strings.Repeat("e", maxEventLen+5))
		created, err := fx.inbox.Receive(fx.repo, header, []byte(`{}`))
		require.NoError(t, err)
		assert.Equal(t, want, created, i)
	}
	all := fx.deliveries(t)
	last := all[len(all)-1]
	assert.True(t, strings.HasPrefix(last.GUID, "sha256:"))
	assert.LessOrEqual(t, len(last.GUID), maxGUIDLen)
	assert.Len(t, last.Event, maxEventLen)

	header := http.Header{}
	header.Set("X-GitHub-Delivery", strings.Repeat("z", maxGUIDLen))
	created, err := fx.inbox.Receive(fx.repo, header, []byte(`{}`))
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, strings.Repeat("z", maxGUIDLen), fx.delivery(t, strings.Repeat("z", maxGUIDLen)).GUID, "as long as the column: kept as it is")
}

// A delivery waiting to be tried again holds back the next of its repo:
// made in order, an older push's pipeline does not cancel a newer one's.
func TestProcessInOrderWhileOneWaits(t *testing.T) {
	fx := newFixture(t)
	var order []string
	fx.forge.EXPECT().Hook(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, r *http.Request) (*model.Repo, *model.Pipeline, error) {
		guid := r.Header.Get("X-GitHub-Delivery")
		order = append(order, guid)
		if guid == "a-1" && len(order) == 1 {
			return nil, nil, errors.New("forge unavailable")
		}
		return nil, nil, &types.ErrIgnoreEvent{}
	})
	fx.receive(t, "a-1")
	fx.run(t)
	require.Equal(t, model.DeliveryPending, fx.delivery(t, "a-1").State)

	fx.receive(t, "a-2")
	fx.run(t)
	assert.Equal(t, []string{"a-1"}, order, "a-2 waits for a-1")
	assert.Equal(t, model.DeliveryPending, fx.delivery(t, "a-2").State)

	fx.clock.Add(firstRetryDelay)
	fx.run(t)
	fx.run(t)
	assert.Equal(t, []string{"a-1", "a-1", "a-2"}, order)
}

// A delivery asked again late is not made into a pipeline once a later
// one of its branch was made meanwhile.
func TestProcessLateDelivery(t *testing.T) {
	fx := newFixture(t)
	push := func() *model.Pipeline {
		return &model.Pipeline{Event: model.EventPush, Branch: "main", Ref: "refs/heads/main", Message: "[CI SKIP] x"}
	}
	fx.forge.EXPECT().Hook(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, *http.Request) (*model.Repo, *model.Pipeline, error) {
		return fx.repo, push(), nil
	})
	sent := fx.clock.Now()
	fx.clock.Add(10 * time.Minute)
	// a pipeline of main made after it was sent
	later := &model.Pipeline{RepoID: fx.repo.ID, Event: model.EventPush, Branch: "main", Status: model.StatusSuccess}
	require.NoError(t, fx.store.CreatePipeline(later))

	fx.inbox.ExpectLate(fx.repo, "late", sent)
	d := fx.receive(t, "late")
	assert.Equal(t, sent.Unix(), d.Sent)
	fx.run(t)
	d = fx.delivery(t, "late")
	assert.Equal(t, model.DeliveryIgnored, d.State)
	assert.Contains(t, d.Error, "came late")

	// one of another branch is made
	fx.inbox.ExpectLate(fx.repo, "late-dev", sent)
	fx.forge.EXPECT().Hook(mock.Anything, mock.Anything).Unset()
	fx.forge.EXPECT().Hook(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, *http.Request) (*model.Repo, *model.Pipeline, error) {
		p := push()
		p.Branch, p.Ref = "dev", "refs/heads/dev"
		return fx.repo, p, nil
	})
	fx.receive(t, "late-dev")
	fx.run(t)
	d = fx.delivery(t, "late-dev")
	assert.NotContains(t, d.Error, "came late", "skipped for its commit message, not for coming late")
}

func TestRunStopsWhatItProcessesAfterAWhile(t *testing.T) {
	fx := newFixture(t)
	fx.inbox.drain = 50 * time.Millisecond
	started := make(chan struct{})
	fx.forge.EXPECT().Hook(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ *http.Request) (*model.Repo, *model.Pipeline, error) {
		close(started)
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}).Once()
	fx.receive(t, "g-1")
	fx.receive(t, "other")

	ctx, cancel := context.WithCancelCause(t.Context())
	done := make(chan struct{})
	go func() {
		fx.inbox.Run(ctx)
		close(done)
	}()
	<-started
	cancel(nil)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop what it processed")
	}
	d := fx.delivery(t, "g-1")
	assert.Equal(t, model.DeliveryPending, d.State, "tried again by the next server")
	assert.Equal(t, model.DeliveryPending, fx.delivery(t, "other").State, "nothing claimed once stopping")
	assert.Zero(t, fx.delivery(t, "other").Attempts)

	// stopped: dispatch claims nothing
	fx.inbox.dispatch(ctx, t.Context())
	assert.Zero(t, fx.delivery(t, "other").Attempts)
}
