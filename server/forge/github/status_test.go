// Copyright 2026 Woodpecker Authors
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

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v93/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// reply is how the fake GitHub answers one request.
type reply struct {
	code    int // 0: 201; -1: drop the connection
	headers map[string]string
	body    string
	delay   time.Duration
}

var (
	created = reply{}
	dropped = reply{code: -1}
)

func code(c int) reply { return reply{code: c} }

// statusServer answers status requests with the given replies in turn, then
// with 201, and records the request path and state of each request.
type statusServer struct {
	mu      sync.Mutex
	replies []reply
	states  []string
	paths   []string
	stored  []string // the states it answered 201 to, in the order it did
}

func (s *statusServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body struct {
		State string `json:"state"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	s.mu.Lock()
	s.states = append(s.states, body.State)
	s.paths = append(s.paths, r.URL.Path)
	rep := created
	if len(s.replies) > 0 {
		rep, s.replies = s.replies[0], s.replies[1:]
	}
	s.mu.Unlock()

	time.Sleep(rep.delay)
	if rep.code == -1 {
		if hijacker, ok := w.(http.Hijacker); ok {
			if conn, _, err := hijacker.Hijack(); err == nil {
				_ = conn.Close()
			}
		}
		return
	}
	for k, v := range rep.headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "application/json")
	if rep.code == 0 {
		rep.code = http.StatusCreated
		s.mu.Lock()
		s.stored = append(s.stored, body.State)
		s.mu.Unlock()
	}
	w.WriteHeader(rep.code)
	if rep.body == "" {
		rep.body = `{}`
	}
	_, _ = w.Write([]byte(rep.body))
}

func (s *statusServer) States() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.states...)
}

// fastPolicy keeps the tests quick: the same shape as the default, in
// milliseconds.
var fastPolicy = statusPolicy{
	syncTimeout:    time.Second,
	attemptTimeout: time.Second,
	firstWait:      5 * time.Millisecond,
	maxWait:        20 * time.Millisecond,
	retryWindow:    3 * time.Second,
}

func newStatusClient(t *testing.T, replies ...reply) (*statusServer, *client, context.Context) {
	t.Helper()
	// the default format, as on a real server: one context per workflow
	format, name := server.Config.Server.StatusContextFormat, server.Config.Server.StatusContext
	server.Config.Server.StatusContextFormat = "{{ .context }}/{{ .event }}/{{ .workflow }}{{ if not (eq .axis_id 0) }}/{{ .axis_id }}{{ end }}"
	server.Config.Server.StatusContext = "ci/woodpecker"
	t.Cleanup(func() { server.Config.Server.StatusContextFormat, server.Config.Server.StatusContext = format, name })

	s := &statusServer{replies: replies}
	fake := httptest.NewServer(s)
	t.Cleanup(fake.Close)

	gh, err := github.NewClient(github.WithURLs(new(fake.URL+"/"), nil), github.WithHTTPClient(fake.Client()))
	require.NoError(t, err)
	c := &client{statuses: &statuses{}, statusPolicy: fastPolicy}
	t.Cleanup(func() { waitIdle(t, c.statuses) })
	return s, c, context.WithValue(t.Context(), githubClientKey, gh)
}

// waitIdle waits for every background sender to finish, so none outlives
// its test.
func waitIdle(t *testing.T, s *statuses) {
	t.Helper()
	assert.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, k := range s.keys {
			if k.busy {
				return false
			}
		}
		return true
	}, 10*time.Second, 5*time.Millisecond, "background senders still running")
}

var (
	statusRepo     = &model.Repo{ID: 7, Owner: "octocat", Name: "Hello-World"}
	statusPipeline = &model.Pipeline{Number: 9, Event: model.EventPull, Commit: "abc123"}
	deployment     = &model.Pipeline{
		Number:   9,
		Event:    model.EventDeploy,
		Status:   model.StatusSuccess,
		ForgeURL: "https://api.github.com/repos/octocat/Hello-World/deployments/42",
	}
)

func workflowIn(state model.StatusValue) *model.Workflow {
	return &model.Workflow{Name: "test", State: state}
}

func (s *statusServer) Stored() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.stored...)
}

func (s *statusServer) lastStored() string {
	stored := s.Stored()
	if len(stored) == 0 {
		return ""
	}
	return stored[len(stored)-1]
}

func TestStatusSentAtOnce(t *testing.T) {
	s, c, ctx := newStatusClient(t)
	require.NoError(t, c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess)))
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, []string{"success"}, s.States())
}

func TestStatusRetriedInTheBackground(t *testing.T) {
	tests := []struct {
		name      string
		replies   []reply
		wantCalls int
	}{
		{name: "a 500", replies: []reply{code(500)}, wantCalls: 2},
		{name: "a 502 and a 503", replies: []reply{code(502), code(503)}, wantCalls: 3},
		{name: "a plain 429", replies: []reply{code(429)}, wantCalls: 2},
		{name: "connection dropped", replies: []reply{dropped}, wantCalls: 2},
		{name: "five failures", replies: []reply{code(500), code(500), code(500), code(502), dropped}, wantCalls: 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, c, ctx := newStatusClient(t, tt.replies...)
			err := c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess))
			assert.ErrorContains(t, err, "retrying in the background")
			assert.Len(t, s.States(), 1, "the caller waits for one attempt only")
			assert.Eventually(t, func() bool { return s.lastStored() == "success" }, 2*time.Second, 5*time.Millisecond)
			time.Sleep(50 * time.Millisecond)
			assert.Len(t, s.States(), tt.wantCalls, "and stops once GitHub took it")
		})
	}
}

func TestStatusSlowGitHub(t *testing.T) {
	s, c, ctx := newStatusClient(t, reply{delay: time.Second})
	c.statusPolicy.syncTimeout = 100 * time.Millisecond

	start := time.Now()
	err := c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess))
	assert.ErrorContains(t, err, "retrying in the background")
	assert.Less(t, time.Since(start), 800*time.Millisecond, "the caller waits syncTimeout at most")
	assert.Eventually(t, func() bool { return len(s.States()) == 2 && s.lastStored() == "success" }, 2*time.Second, 5*time.Millisecond)
}

func TestStatusRefused(t *testing.T) {
	for _, refused := range []int{403, 404, 422} {
		t.Run(strconv.Itoa(refused), func(t *testing.T) {
			s, c, ctx := newStatusClient(t, code(refused))
			err := c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess))
			assert.Error(t, err)
			assert.NotContains(t, err.Error(), "background")
			time.Sleep(50 * time.Millisecond)
			assert.Len(t, s.States(), 1, "not retried")
		})
	}
}

func TestStatusRefusedInTheBackground(t *testing.T) {
	s, c, ctx := newStatusClient(t, code(500), code(404))

	assert.Error(t, c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess)))
	waitIdle(t, c.statuses)
	assert.Len(t, s.States(), 2, "not retried after the refusal")

	require.NoError(t, c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess)))
	assert.Len(t, s.States(), 3, "the key is free: the next status goes out at once")
	assert.Equal(t, "success", s.lastStored())
}

func TestStatusGivesUpAfterTheWindow(t *testing.T) {
	replies := make([]reply, 1000)
	for i := range replies {
		replies[i] = code(500)
	}
	s, c, ctx := newStatusClient(t, replies...)
	c.statusPolicy.retryWindow = 150 * time.Millisecond

	assert.Error(t, c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess)))
	assert.Eventually(t, func() bool { return len(s.States()) > 2 }, time.Second, time.Millisecond, "retried in the background")
	waitIdle(t, c.statuses)
	calls := len(s.States())
	time.Sleep(100 * time.Millisecond)
	assert.Len(t, s.States(), calls, "no attempt after the retry window")

	s.mu.Lock()
	s.replies = nil
	s.mu.Unlock()
	require.NoError(t, c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess)))
	assert.Equal(t, "success", s.lastStored(), "the key is free again")
}

func TestStatusRateLimited(t *testing.T) {
	tests := map[string]func() reply{
		"primary limit: waits for the reset": func() reply {
			reset := strconv.FormatInt(time.Now().Add(time.Second).Unix()+1, 10)
			return reply{code: 403, body: `{"message":"API rate limit exceeded"}`, headers: map[string]string{
				"X-RateLimit-Limit": "5000", "X-RateLimit-Remaining": "0", "X-RateLimit-Reset": reset,
			}}
		},
		"secondary limit: waits as asked": func() reply {
			return reply{
				code:    403,
				body:    `{"message":"You have exceeded a secondary rate limit","documentation_url":"https://docs.github.com/rest/overview/rate-limits-for-the-rest-api#about-secondary-rate-limits"}`,
				headers: map[string]string{"Retry-After": "1"},
			}
		},
	}
	for name, limited := range tests {
		t.Run(name, func(t *testing.T) {
			s, c, ctx := newStatusClient(t, limited())
			c.statusPolicy.retryWindow = 5 * time.Second

			start := time.Now()
			err := c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess))
			assert.ErrorContains(t, err, "retrying in the background")
			assert.Eventually(t, func() bool { return len(s.States()) == 2 }, 4*time.Second, 10*time.Millisecond)
			assert.GreaterOrEqual(t, time.Since(start), 900*time.Millisecond, "sent again only after the wait")
		})
	}
}

func TestStatusNewerWins(t *testing.T) {
	t.Run("a newer status replaces one being retried", func(t *testing.T) {
		s, c, ctx := newStatusClient(t, code(500))
		c.statusPolicy.firstWait = 200 * time.Millisecond

		assert.Error(t, c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusRunning)))
		require.NoError(t, c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess)), "the sender picks it up")
		assert.Eventually(t, func() bool { return s.lastStored() == "success" }, 2*time.Second, 5*time.Millisecond)
		time.Sleep(300 * time.Millisecond)
		assert.Equal(t, []string{"pending", "success"}, s.States(), "the older one is not sent again")
	})

	t.Run("a newer status waits for one in flight", func(t *testing.T) {
		s, c, ctx := newStatusClient(t, reply{delay: 200 * time.Millisecond})

		older := make(chan error, 1)
		go func() { older <- c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusRunning)) }()
		require.Eventually(t, func() bool { return len(s.States()) == 1 }, time.Second, time.Millisecond)

		require.NoError(t, c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess)))
		require.NoError(t, <-older)
		assert.Eventually(t, func() bool { return len(s.Stored()) == 2 }, 2*time.Second, 5*time.Millisecond)
		assert.Equal(t, []string{"pending", "success"}, s.Stored(), "GitHub stores success last")
	})

	t.Run("a status that may have landed is not trusted", func(t *testing.T) {
		// success lands; then failure times out on our side but GitHub
		// stores it; then success is asked for again and must be sent
		s, c, ctx := newStatusClient(t, created, reply{delay: 200 * time.Millisecond}, code(500))
		c.statusPolicy.syncTimeout = 50 * time.Millisecond
		c.statusPolicy.firstWait = 300 * time.Millisecond

		require.NoError(t, c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess)))
		assert.Error(t, c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusFailure)))
		require.NoError(t, c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess)))
		assert.Eventually(t, func() bool {
			stored := s.Stored()
			return len(stored) == 3 && stored[2] == "success"
		}, 3*time.Second, 5*time.Millisecond, "stored: %v", s.Stored())
	})
}

func TestStatusNotSentTwice(t *testing.T) {
	s, c, ctx := newStatusClient(t)
	send := func(p *model.Pipeline, w *model.Workflow) {
		require.NoError(t, c.Status(ctx, fakeUser, statusRepo, p, w))
	}

	send(statusPipeline, workflowIn(model.StatusPending))
	send(statusPipeline, workflowIn(model.StatusPending))
	send(statusPipeline, workflowIn(model.StatusRunning)) // also "pending" on GitHub, but described as running
	send(statusPipeline, workflowIn(model.StatusRunning))
	send(statusPipeline, workflowIn(model.StatusSuccess))
	send(statusPipeline, workflowIn(model.StatusSuccess))
	assert.Equal(t, []string{"pending", "pending", "success"}, s.States())

	send(statusPipeline, &model.Workflow{Name: "lint", State: model.StatusSuccess})
	assert.Len(t, s.States(), 4, "another workflow has its own context")

	restarted := &model.Pipeline{Number: 10, Event: model.EventPull, Commit: statusPipeline.Commit}
	send(restarted, workflowIn(model.StatusSuccess))
	assert.Len(t, s.States(), 5, "a restarted pipeline links to its own run")

	other := &model.Pipeline{Number: 11, Event: model.EventPull, Commit: "def456"}
	send(other, workflowIn(model.StatusSuccess))
	assert.Len(t, s.States(), 6, "another commit gets its own status")
}

func TestStatusOutlivesItsCaller(t *testing.T) {
	s, c, ctx := newStatusClient(t, reply{delay: 100 * time.Millisecond})
	ctx, cancel := context.WithCancelCause(ctx)

	done := make(chan error, 1)
	go func() { done <- c.Status(ctx, fakeUser, statusRepo, statusPipeline, workflowIn(model.StatusSuccess)) }()
	require.Eventually(t, func() bool { return len(s.States()) == 1 }, time.Second, time.Millisecond)
	cancel(nil)
	require.NoError(t, <-done, "the attempt is not cut short by the caller giving up")
	assert.Equal(t, []string{"success"}, s.Stored())
}

func TestDeploymentStatus(t *testing.T) {
	t.Run("retried", func(t *testing.T) {
		s, c, ctx := newStatusClient(t, code(502))
		assert.Error(t, c.Status(ctx, fakeUser, statusRepo, deployment, nil))
		assert.Eventually(t, func() bool { return len(s.Stored()) == 1 }, 2*time.Second, 5*time.Millisecond)
		assert.Len(t, s.States(), 2)
	})
	t.Run("each state once", func(t *testing.T) {
		s, c, ctx := newStatusClient(t)
		running := &model.Pipeline{Number: 9, Event: model.EventDeploy, Status: model.StatusRunning, ForgeURL: deployment.ForgeURL}
		require.NoError(t, c.Status(ctx, fakeUser, statusRepo, running, nil))
		require.NoError(t, c.Status(ctx, fakeUser, statusRepo, running, nil))
		require.NoError(t, c.Status(ctx, fakeUser, statusRepo, deployment, nil))
		require.NoError(t, c.Status(ctx, fakeUser, statusRepo, deployment, nil))
		assert.Equal(t, []string{"pending", "success"}, s.Stored())
	})
	t.Run("refused", func(t *testing.T) {
		s, c, ctx := newStatusClient(t, code(404))
		assert.Error(t, c.Status(ctx, fakeUser, statusRepo, deployment, nil))
		time.Sleep(50 * time.Millisecond)
		assert.Len(t, s.States(), 1)
		s.mu.Lock()
		defer s.mu.Unlock()
		assert.Equal(t, []string{"/repos/octocat/Hello-World/deployments/42/statuses"}, s.paths)
	})
}

func TestStatusKeysForgetOnlyIdleOnes(t *testing.T) {
	var s statuses
	s.mu.Lock()
	defer s.mu.Unlock()
	busy := s.key("busy")
	busy.busy = true
	idle := s.key("idle")
	for i := range maxStatusKeys {
		s.key(fmt.Sprintf("key %d", i))
	}
	assert.LessOrEqual(t, len(s.keys), maxStatusKeys)
	assert.Same(t, busy, s.key("busy"), "a key with a sender is kept")
	assert.NotSame(t, idle, s.key("idle"), "idle keys are forgotten")
}

func TestStatusesOutliveTheClient(t *testing.T) {
	assert.Same(t, statusesOf(1001), statusesOf(1001))
	assert.NotSame(t, statusesOf(1001), statusesOf(1002))

	a, err := New(1001, Opts{URL: defaultURL})
	require.NoError(t, err)
	b, err := New(1001, Opts{URL: defaultURL})
	require.NoError(t, err)
	assert.Same(t, a.(*client).statuses, b.(*client).statuses, "a recreated client keeps the memory") //nolint:forcetypeassert
}
