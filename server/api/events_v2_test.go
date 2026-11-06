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

package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/summary"
)

// eventsV2Env is a server of /api/v2/events over a sqlite store: a user who
// may read the private repo "mine", not the private repo "theirs", and the
// public repo "open".
type eventsV2Env struct {
	store     store.Store
	hub       *events.Hub
	durations *summary.Durations
	// stop stops the server: its long polls and streams end
	stop               context.CancelFunc
	user               *model.User
	mine, theirs, open *model.Repo
	srv                *httptest.Server
}

func newEventsV2Env(t *testing.T) *eventsV2Env {
	t.Helper()
	return newEventsV2EnvWith(t, events.Options{})
}

func newEventsV2EnvWith(t *testing.T, options events.Options) *eventsV2Env {
	t.Helper()
	gin.SetMode(gin.TestMode)
	s := newTestStore(t)
	env := &eventsV2Env{store: s, hub: events.NewHub(options), durations: summary.NewDurations(0)}
	stopping, stop := context.WithCancelCause(t.Context())
	env.stop = func() { stop(nil) }
	t.Cleanup(env.stop)

	env.user = &model.User{Login: "me", ForgeRemoteID: "u1", Hash: "h1"}
	require.NoError(t, s.CreateUser(env.user))
	other := &model.User{Login: "other", ForgeRemoteID: "u2", Hash: "h2"}
	require.NoError(t, s.CreateUser(other))

	repo := func(name string, owner *model.User, visibility model.RepoVisibility) *model.Repo {
		r := &model.Repo{
			UserID: owner.ID, ForgeRemoteID: model.ForgeRemoteID("r-" + name), Owner: owner.Login, Name: name,
			FullName: owner.Login + "/" + name, IsActive: true, Visibility: visibility,
		}
		require.NoError(t, s.CreateRepo(r))
		return r
	}
	env.mine = repo("mine", env.user, model.VisibilityPrivate)
	env.theirs = repo("theirs", other, model.VisibilityPrivate)
	env.open = repo("open", other, model.VisibilityPublic)
	env.perm(t, env.mine, true)
	env.perm(t, env.theirs, false)

	pingInterval, ttl, manager := eventsPingInterval, v2AccessTTL, server.Config.Services.Manager
	eventsPingInterval, v2AccessTTL = 50*time.Millisecond, time.Hour
	server.Config.Services.Manager = nil // no forge to sync a permission with, unless a test sets one
	t.Cleanup(func() {
		eventsPingInterval, v2AccessTTL, server.Config.Services.Manager = pingInterval, ttl, manager
	})

	e := gin.New()
	e.Use(middleware.Stopping(stopping))
	e.Use(func(c *gin.Context) {
		c.Set("store", s)
		if c.GetHeader("X-Test-User") != "" {
			if user, err := s.GetUser(env.user.ID); err == nil {
				c.Set("user", user)
			}
		}
	})
	// a request with a token is authenticated by it, as by the server
	e.Use(session.SetUser())
	bots := session.NewBotRoutes("")
	e.Use(session.AuditBot(bots))
	e.Use(session.GuardBot(bots))
	v2 := e.Group("/api/v2")
	read := session.BotRoute{Scope: model.BotScopeRead, Audit: session.BotAuditPipeline}
	bots.Handle(v2, http.MethodGet, "/events", session.BotRoute{Scope: model.BotScopeRead}, session.MustUser(), EventStreamV2(env.hub))
	bots.Handle(v2, http.MethodGet, "/pipelines/:pipeline_id/wait", read, session.MustUser(), WaitPipelineV2(env.hub, env.durations))
	bots.Handle(v2, http.MethodPost, "/pipelines/:pipeline_id/restart", session.BotRoute{Scope: model.BotScopeRestart, Audit: session.BotAuditPipeline},
		session.MustUser(), RestartPipelineV2)
	bots.Handle(v2, http.MethodGet, "/whoami", session.BotRoute{}, session.MustUser(), WhoamiV2)
	e.GET("/api/v2/bot-tokens", session.MustUser(), GetBotTokens)
	e.POST("/api/v2/bot-tokens", session.MustUser(), PostBotToken)
	e.DELETE("/api/v2/bot-tokens/:token_id", session.MustUser(), DeleteBotToken)
	e.GET("/api/v2/bot-tokens/:token_id/calls", session.MustUser(), GetBotTokenCalls)
	env.srv = httptest.NewServer(e)
	t.Cleanup(env.srv.Close)
	return env
}

// perm stores the user's permission to pull the repo, synced now.
func (env *eventsV2Env) perm(t *testing.T, repo *model.Repo, pull bool) {
	t.Helper()
	require.NoError(t, env.store.PermUpsert(&model.Perm{UserID: env.user.ID, RepoID: repo.ID, Pull: pull, Synced: time.Now().Unix()}))
}

// setAdmin makes the user an admin or not.
func (env *eventsV2Env) setAdmin(t *testing.T, admin bool) {
	t.Helper()
	user, err := env.store.GetUser(env.user.ID)
	require.NoError(t, err)
	user.Admin = admin
	require.NoError(t, env.store.UpdateUser(user))
}

// quiet collects the events of a stream until none comes for a moment, and
// fails the test if the stream ends.
func (c *sseClient) quiet(t *testing.T) []events.Event {
	t.Helper()
	var got []events.Event
	quiet := time.NewTimer(300 * time.Millisecond)
	defer quiet.Stop()
	for {
		select {
		case f, ok := <-c.frames:
			require.True(t, ok, "the stream ended")
			if f.comment {
				continue // a ping is quiet
			}
			var e events.Event
			require.NoError(t, json.Unmarshal([]byte(f.data), &e))
			got = append(got, e)
			quiet.Reset(300 * time.Millisecond)
		case <-quiet.C:
			return got
		}
	}
}

func repoIDs(got []events.Event) []int64 {
	var out []int64
	for _, e := range got {
		out = append(out, e.RepoID)
	}
	return out
}

// pipeline stores a pipeline of the repo and gives its snapshots to the hub:
// it runs one step that fails.
func (env *eventsV2Env) pipeline(t *testing.T, repo *model.Repo, number int64) *model.Pipeline {
	t.Helper()
	p := &model.Pipeline{RepoID: repo.ID, Number: number, Branch: "main", Event: model.EventPush, Status: model.StatusPending}
	require.NoError(t, env.store.CreatePipeline(p))
	env.hub.Observe(repo, p)
	return p
}

func (env *eventsV2Env) fail(repo *model.Repo, p *model.Pipeline) {
	running := *p
	running.Status, running.Started = model.StatusRunning, 100
	running.Workflows = []*model.Workflow{{ID: p.ID * 10, Name: "build", State: model.StatusRunning, Started: 100}}
	env.hub.Observe(repo, &running)
	failed := running
	failed.Status, failed.Finished = model.StatusFailure, 160
	failed.Workflows = []*model.Workflow{{
		ID: p.ID * 10, Name: "build", State: model.StatusFailure, Started: 100, Finished: 160,
		Children: []*model.Step{{ID: p.ID * 100, Name: "test", State: model.StatusFailure, ExitCode: 1, Started: 101, Finished: 159}},
	}}
	env.hub.Observe(repo, &failed)
}

type sseFrame struct {
	id, event, data string
	comment         bool
}

type sseClient struct {
	resp   *http.Response
	frames chan sseFrame
}

// connect opens a stream; it fails the test on a status other than 200.
func (env *eventsV2Env) connect(t *testing.T, query string, header http.Header) *sseClient {
	t.Helper()
	resp := env.get(t, query, header) //nolint:bodyclose // closed with the test
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	c := &sseClient{resp: resp, frames: make(chan sseFrame, 100)}
	go func() {
		defer close(c.frames)
		r := bufio.NewReader(resp.Body)
		var f sseFrame
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSuffix(line, "\n")
			switch {
			case line == "":
				if f != (sseFrame{}) {
					c.frames <- f
				}
				f = sseFrame{}
			case strings.HasPrefix(line, ":"):
				f.comment = true
			case strings.HasPrefix(line, "id: "):
				f.id = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "event: "):
				f.event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				f.data = strings.TrimPrefix(line, "data: ")
			}
		}
	}()
	t.Cleanup(func() { resp.Body.Close() })
	return c
}

func (env *eventsV2Env) get(t *testing.T, query string, header http.Header) *http.Response {
	t.Helper()
	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, env.srv.URL+"/api/v2/events?"+query, nil)
	require.NoError(t, err)
	maps.Copy(req.Header, header)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

var loggedIn = http.Header{"X-Test-User": {"1"}}

// next is the next frame that is not a ping.
func (c *sseClient) next(t *testing.T) sseFrame {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case f, ok := <-c.frames:
			require.True(t, ok, "the stream ended")
			if f.comment {
				continue
			}
			return f
		case <-timeout:
			t.Fatal("timeout waiting for an event")
		}
	}
}

func TestEventsV2NeedsALogin(t *testing.T) {
	env := newEventsV2Env(t)
	resp := env.get(t, "", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestEventsV2BadRequests(t *testing.T) {
	env := newEventsV2Env(t)
	p := env.pipeline(t, env.mine, 1)
	for _, tt := range []struct {
		query  string
		header http.Header
		status int
	}{
		{query: "min_severity=fatal", status: http.StatusBadRequest},
		{query: "types=step.exploded", status: http.StatusBadRequest},
		{query: fmt.Sprintf("pipeline=%d&number=1&repo=%d", p.ID, env.mine.ID), status: http.StatusBadRequest},
		{query: "number=1", status: http.StatusBadRequest},
		{query: "pipeline=abc", status: http.StatusBadRequest},
		{query: "since=abc", status: http.StatusBadRequest},
		{header: http.Header{"Last-Event-ID": {"-1"}}, status: http.StatusBadRequest},
		{query: fmt.Sprintf("repo=%d", env.theirs.ID), status: http.StatusNotFound},
		{query: "repo=" + url.QueryEscape(env.theirs.FullName), status: http.StatusNotFound},
		{query: "repo=nobody/nothing", status: http.StatusNotFound},
		{query: fmt.Sprintf("repo=%d&number=99", env.mine.ID), status: http.StatusNotFound},
		{query: "pipeline=99999", status: http.StatusNotFound},
		{query: fmt.Sprintf("pipeline=%d", env.pipeline(t, env.theirs, 1).ID), status: http.StatusNotFound},
	} {
		t.Run(tt.query+fmt.Sprint(tt.header), func(t *testing.T) {
			header := loggedIn.Clone()
			maps.Copy(header, tt.header)
			resp := env.get(t, tt.query, header)
			defer resp.Body.Close()
			assert.Equal(t, tt.status, resp.StatusCode)
		})
	}
}

func TestEventsV2Stream(t *testing.T) {
	env := newEventsV2Env(t)
	for _, repo := range []*model.Repo{env.mine, env.theirs, env.open} {
		env.fail(repo, env.pipeline(t, repo, 1))
	}

	c := env.connect(t, "since=0&min_severity=error", loggedIn)
	hello := c.next(t)
	assert.Equal(t, "hello", hello.event)
	var helloData events.Hello
	require.NoError(t, json.Unmarshal([]byte(hello.data), &helloData))
	assert.Equal(t, events.SchemaVersion, helloData.V)
	assert.Nil(t, helloData.Head, "the ids of the buffer are told to admins only")
	assert.Nil(t, helloData.Oldest)

	// replayed: the failures of mine and open, not of theirs
	var repos []int64
	var last sseFrame
	for range 6 {
		last = c.next(t)
		var e events.Event
		require.NoError(t, json.Unmarshal([]byte(last.data), &e))
		assert.Equal(t, fmt.Sprint(e.ID), last.id)
		assert.Equal(t, string(e.Type), last.event)
		assert.Equal(t, events.Error, e.Severity)
		repos = append(repos, e.RepoID)
	}
	assert.Equal(t, []int64{env.mine.ID, env.mine.ID, env.mine.ID, env.open.ID, env.open.ID, env.open.ID}, repos)
	first, err := strconv.ParseInt(hello.id, 10, 64)
	require.NoError(t, err, "hello has the id the stream goes on from")
	lastID, err := strconv.ParseInt(last.id, 10, 64)
	require.NoError(t, err)
	assert.Less(t, first, lastID, "the replay is after it")

	// live: a new failure of mine, then pings
	env.fail(env.mine, env.pipeline(t, env.mine, 2))
	live := c.next(t)
	assert.Equal(t, "step.failed", live.event)
	assert.Equal(t, "workflow.finished", c.next(t).event)
	assert.Equal(t, "pipeline.finished", c.next(t).event)
	select {
	case f := <-c.frames:
		assert.True(t, f.comment, "an idle stream is pinged")
	case <-time.After(5 * time.Second):
		t.Fatal("no ping")
	}

	// reconnecting with the last id gets the rest
	again := env.connect(t, "min_severity=error", http.Header{"X-Test-User": {"1"}, "Last-Event-ID": {last.id}})
	assert.Equal(t, "hello", again.next(t).event)
	assert.Equal(t, live.id, again.next(t).id)
}

func TestEventsV2Filters(t *testing.T) {
	env := newEventsV2Env(t)
	first := env.pipeline(t, env.mine, 1)
	second := env.pipeline(t, env.mine, 2)
	env.fail(env.mine, first)
	env.fail(env.mine, second)
	env.fail(env.open, env.pipeline(t, env.open, 1))

	// the pipelines of the events replayed
	pipelines := func(query string) []int64 {
		c := env.connect(t, "since=0&"+query, loggedIn)
		require.Equal(t, "hello", c.next(t).event)
		var got []int64
		for _, e := range c.quiet(t) {
			got = append(got, e.PipelineID)
		}
		return got
	}

	assert.Equal(t, []int64{second.ID, second.ID, second.ID}, pipelines(fmt.Sprintf("repo=%d&number=2&min_severity=error", env.mine.ID)))
	assert.Equal(t, []int64{first.ID}, pipelines(fmt.Sprintf("pipeline=%d&types=step.failed", first.ID)))
	assert.Equal(t, []int64{first.ID, second.ID}, pipelines("repo="+url.QueryEscape(env.mine.FullName)+"&types=pipeline.started&min_severity=debug"))
}

func TestEventsV2Reset(t *testing.T) {
	env := newEventsV2Env(t)
	env.fail(env.mine, env.pipeline(t, env.mine, 1))

	for cursor, reason := range map[string]events.ResetReason{
		"5":                   events.ResetRestart,
		"9000000000000000000": events.ResetInvalid,
	} {
		c := env.connect(t, "min_severity=error&since="+cursor, loggedIn)
		assert.Equal(t, "hello", c.next(t).event)
		reset := c.next(t)
		assert.Equal(t, "reset", reset.event)
		assert.Contains(t, reset.data, fmt.Sprintf(`"reason":"%s"`, reason))

		env.fail(env.mine, env.pipeline(t, env.mine, int64(len(cursor))+10))
		assert.Equal(t, "step.failed", c.next(t).event, "the stream goes on live after a reset")
	}
}

func TestEventsV2HelloOfAnAdmin(t *testing.T) {
	env := newEventsV2Env(t)
	env.fail(env.mine, env.pipeline(t, env.mine, 1))
	env.setAdmin(t, true)

	c := env.connect(t, "", loggedIn)
	hello := c.next(t)
	var data events.Hello
	require.NoError(t, json.Unmarshal([]byte(hello.data), &data))
	require.NotNil(t, data.Head)
	require.NotNil(t, data.Oldest)
	assert.Equal(t, fmt.Sprint(*data.Head), hello.id, "a live stream goes on from the head")
	assert.Equal(t, int64(6), *data.Head-*data.Oldest+1, "the six events of the pipeline are kept")
}

// TestEventsV2AccessIsOfNow decides from the repo as it is now: a repo
// public when its events were published and private when they are replayed
// is private.
func TestEventsV2AccessIsOfNow(t *testing.T) {
	env := newEventsV2Env(t)
	env.fail(env.open, env.pipeline(t, env.open, 1))
	env.open.Visibility = model.VisibilityPrivate
	require.NoError(t, env.store.UpdateRepo(env.open))

	c := env.connect(t, "since=0&min_severity=debug", loggedIn)
	require.Equal(t, "hello", c.next(t).event)
	assert.Empty(t, c.quiet(t))
}

// TestEventsV2AccessChangesMidStream follows the user and their permissions
// once v2AccessTTL passed.
func TestEventsV2AccessChangesMidStream(t *testing.T) {
	env := newEventsV2Env(t)
	v2AccessTTL = 100 * time.Millisecond
	c := env.connect(t, "min_severity=error", loggedIn)
	require.Equal(t, "hello", c.next(t).event)
	number := int64(0)
	failBoth := func() {
		number++
		env.fail(env.mine, env.pipeline(t, env.mine, number))
		env.fail(env.theirs, env.pipeline(t, env.theirs, number))
	}

	failBoth()
	assert.Equal(t, []int64{env.mine.ID, env.mine.ID, env.mine.ID}, repoIDs(c.quiet(t)), "mine only")

	env.setAdmin(t, true)
	time.Sleep(v2AccessTTL)
	failBoth()
	assert.Equal(t, []int64{env.mine.ID, env.mine.ID, env.mine.ID, env.theirs.ID, env.theirs.ID, env.theirs.ID},
		repoIDs(c.quiet(t)), "an admin reads all")

	env.setAdmin(t, false)
	env.perm(t, env.mine, false)
	time.Sleep(v2AccessTTL)
	failBoth()
	assert.Empty(t, c.quiet(t), "neither once the admin and the pull are gone")
}

func TestEventsV2EndsWhenTheUserIsGone(t *testing.T) {
	env := newEventsV2Env(t)
	v2AccessTTL = 100 * time.Millisecond
	c := env.connect(t, "", loggedIn)
	require.Equal(t, "hello", c.next(t).event)

	require.NoError(t, env.store.DeleteUser(env.user))
	f := c.next(t)
	assert.Equal(t, "error", f.event)
	assert.Contains(t, f.data, `"reason":"unauthorized"`)
	_, open := <-c.frames
	assert.False(t, open, "the stream ends")
}

// TestEventsV2NeverAsksTheForge decides from the stored permissions: one
// older than session.PermSyncInterval keeps its pull, none is none, and the
// forge is not asked.
func TestEventsV2NeverAsksTheForge(t *testing.T) {
	env := newEventsV2Env(t)
	require.NoError(t, env.store.PermUpsert(&model.Perm{UserID: env.user.ID, RepoID: env.mine.ID, Pull: true, Synced: 1}))
	other, err := env.store.GetUser(env.theirs.UserID)
	require.NoError(t, err)
	unknown := &model.Repo{
		UserID: other.ID, ForgeRemoteID: "r-unknown", Owner: other.Login, Name: "unknown",
		FullName: other.Login + "/unknown", IsActive: true, Visibility: model.VisibilityPrivate,
	}
	require.NoError(t, env.store.CreateRepo(unknown))
	// no expectations: a call of the manager fails the test
	server.Config.Services.Manager = manager_mocks.NewMockManager(t)

	env.fail(env.mine, env.pipeline(t, env.mine, 1))
	env.fail(unknown, env.pipeline(t, unknown, 1))
	c := env.connect(t, "since=0&min_severity=error", loggedIn)
	require.Equal(t, "hello", c.next(t).event)
	assert.Equal(t, []int64{env.mine.ID, env.mine.ID, env.mine.ID}, repoIDs(c.quiet(t)))

	perm, err := env.store.PermFind(env.user, env.mine)
	require.NoError(t, err)
	assert.Equal(t, int64(1), perm.Synced, "the permission is not synced")

	// the mock checks its calls once the stream is done with it
	c.resp.Body.Close()
	env.srv.Close()
}

func TestEventsV2EndsWhenTheTokensAreReset(t *testing.T) {
	env := newEventsV2Env(t)
	v2AccessTTL = 100 * time.Millisecond
	c := env.connect(t, "", loggedIn)
	require.Equal(t, "hello", c.next(t).event)

	user, err := env.store.GetUser(env.user.ID)
	require.NoError(t, err)
	user.Hash = "h1-reset"
	require.NoError(t, env.store.UpdateUser(user))
	f := c.next(t)
	assert.Equal(t, "error", f.event)
	assert.Contains(t, f.data, `"reason":"unauthorized"`)
	_, open := <-c.frames
	assert.False(t, open, "the stream ends")
}

// TestEventsV2PingMovesTheCursor moves the cursor of a quiet stream past the
// events the buffer dropped, so that it reconnects without a reset.
func TestEventsV2PingMovesTheCursor(t *testing.T) {
	env := newEventsV2EnvWith(t, events.Options{BufferSize: 3})
	c := env.connect(t, fmt.Sprintf("repo=%d", env.mine.ID), loggedIn)
	hello := c.next(t)
	require.Equal(t, "hello", hello.event)

	env.fail(env.open, env.pipeline(t, env.open, 1)) // six events, none for the stream
	var ping sseFrame
	timeout := time.After(5 * time.Second)
	for ping.id == "" || ping.id == hello.id {
		select {
		case f := <-c.frames:
			require.True(t, f.comment, "only pings: %v", f)
			ping = f
		case <-timeout:
			t.Fatal("no ping that moves the cursor")
		}
	}

	query := fmt.Sprintf("repo=%d", env.mine.ID)
	stale := env.connect(t, query, http.Header{"X-Test-User": {"1"}, "Last-Event-ID": {hello.id}})
	require.Equal(t, "hello", stale.next(t).event)
	assert.Equal(t, "reset", stale.next(t).event, "the cursor of hello is too old by now")

	moved := env.connect(t, query, http.Header{"X-Test-User": {"1"}, "Last-Event-ID": {ping.id}})
	require.Equal(t, "hello", moved.next(t).event)
	env.fail(env.mine, env.pipeline(t, env.mine, 1))
	assert.Equal(t, "pipeline.started", moved.next(t).event, "the one of the ping replays, without a reset")
}

func TestEventsV2AccessBackoff(t *testing.T) {
	user := &model.User{ID: 1, Login: "me", Hash: "h"}
	repo := &model.Repo{ID: 2, FullName: "o/r", Visibility: model.VisibilityPrivate}
	s := store_mocks.NewMockStore(t)
	s.On("GetRepo", repo.ID).Return(repo, nil).Once()
	s.On("PermFind", user, repo).Return(nil, errors.New("database is down")).Once()

	access := newRepoAccess(s, session.Principal{User: user})
	for range 3 {
		allowed, err := access.allowed(repo.ID)
		require.NoError(t, err)
		assert.False(t, allowed, "denied, and not looked up again for the backoff")
	}
}
