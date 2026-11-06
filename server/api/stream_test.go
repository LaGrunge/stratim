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

package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/logging"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

func TestEventStreamSSEConcurrentDisconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	broker := memory.New()
	server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), nil, nil, broker)
	t.Cleanup(func() { server.Config.Services.Scheduler = nil })

	for i := range 50 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)

			ctx, cancel := context.WithCancelCause(t.Context())
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/stream/events", nil)
			c.Request = req

			topic := map[string]struct{}{pubsub.PublicTopic: {}}

			done := make(chan struct{})
			go func() {
				defer close(done)
				EventStreamSSE(c)
			}()

			// Let the event handler subscribe
			time.Sleep(20 * time.Millisecond)

			// Fire concurrent publishes while canceling the request.
			var wg sync.WaitGroup
			for range 20 {
				wg.Go(func() {
					_ = broker.Publish(ctx, topic, pubsub.Message{
						Data: []byte(`{"pipeline":1}`),
					})
				})
			}

			// Simulate client disconnect mid-publish.
			cancel(nil)
			wg.Wait()
			<-done
		})
	}
}

func setupLogStreamContext(t *testing.T) (*httptest.ResponseRecorder, *gin.Context, context.CancelCauseFunc) {
	t.Helper()

	const stepID int64 = 42
	const pipelineID int64 = 10

	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("GetPipelineNumber", mock.Anything, mock.Anything).
		Return(&model.Pipeline{ID: pipelineID}, nil)
	mockStore.On("StepLoad", mock.Anything, mock.Anything).
		Return(&model.Step{
			ID:         stepID,
			PipelineID: pipelineID,
			State:      model.StatusRunning,
		}, nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	ctx, cancel := context.WithCancelCause(t.Context())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/stream/logs/1/1/42", nil)
	c.Request = req
	c.Params = gin.Params{
		{Key: "repo_id", Value: "1"},
		{Key: "pipeline", Value: "1"},
		{Key: "step_id", Value: "42"},
	}
	c.Set("repo", &model.Repo{ID: 1, FullName: "owner/repo"})
	c.Set("store", mockStore)

	return w, c, cancel
}

func TestLogStreamSSEConcurrentDisconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)

	logService := logging.New()
	server.Config.Services.Logs = logService
	t.Cleanup(func() { server.Config.Services.Logs = nil })

	const stepID int64 = 42

	for i := range 50 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			done := make(chan struct{})

			_, c, cancel := setupLogStreamContext(t)

			go func() {
				defer close(done)
				LogStreamSSE(c)
			}()

			// Let LogStreamSSE open the stream and start tailing.
			time.Sleep(20 * time.Millisecond)

			// Fire concurrent log writes while canceling the request.
			var wg sync.WaitGroup
			for i := range 20 {
				wg.Go(func() {
					_ = logService.Write(t.Context(), stepID, []*model.LogEntry{
						{Line: i, Data: []byte("log line")},
					})
				})
			}

			// Simulate client disconnect mid-write.
			cancel(nil)
			wg.Wait()
			<-done
		})
	}
}

// The handlers must not leave a goroutine behind that still reads
// server.Config after the handler returned: the t.Cleanup of the stress tests
// above resets the service, and the race detector reports the unsynchronized
// read/write pair (issue #6766). Swapping the service around every request
// reproduces that reliably.
func TestEventStreamSSEDisconnectedClient(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for range 50 {
		server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), nil, nil, memory.New())

		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)

		ctx, cancel := context.WithCancelCause(t.Context())
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/stream/events", nil)
		c.Request = req

		// The client is already gone when the handler starts.
		cancel(nil)
		EventStreamSSE(c)
		assert.Contains(t, w.Body.String(), ": ping\n\n")

		server.Config.Services.Scheduler = nil
	}
}

func TestLogStreamSSEDisconnectedClient(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for range 50 {
		server.Config.Services.Logs = logging.New()

		w, c, cancel := setupLogStreamContext(t)

		// The client is already gone when the handler starts.
		cancel(nil)
		LogStreamSSE(c)
		assert.Contains(t, w.Body.String(), ": ping\n\n")

		server.Config.Services.Logs = nil
	}
}

// TestEventStreamSSEStreamsEachEvent serves the event stream over a real
// connection: the response must carry the headers that keep proxies from
// buffering it, and every published pipeline update must reach the client,
// in order, while the stream is still open (so each one was flushed).
func TestEventStreamSSEStreamsEachEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sched := scheduler.NewScheduler(t.Context(), nil, nil, memory.New())
	server.Config.Services.Scheduler = sched
	t.Cleanup(func() { server.Config.Services.Scheduler = nil })

	router := gin.New()
	router.GET("/stream/events", EventStreamSSE)
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/stream/events", nil)
	require.NoError(t, err)
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	assert.Equal(t, "no", resp.Header.Get("X-Accel-Buffering"))
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))

	lines := make(chan string)
	scanner := bufio.NewScanner(resp.Body)
	go func() {
		defer close(lines)
		for scanner.Scan() {
			if line := scanner.Text(); line != "" {
				lines <- line
			}
		}
	}()
	next := func() string {
		select {
		case line := <-lines:
			return line
		case <-time.After(5 * time.Second):
			t.Fatal("no line on the event stream")
			return ""
		}
	}

	// The handler subscribes right after the first ping was flushed.
	require.Equal(t, ": ping", next())
	time.Sleep(50 * time.Millisecond)

	repo := &model.Repo{ID: 1}
	statuses := []model.StatusValue{model.StatusPending, model.StatusRunning, model.StatusSuccess}
	// distinct pipelines: queued updates of one pipeline may be coalesced
	for i, status := range statuses {
		pl := &model.Pipeline{ID: int64(i + 1), Number: int64(i + 1), Status: status}
		require.NoError(t, sched.PublishPipelineEvent(t.Context(), repo, pl))
	}
	for i, status := range statuses {
		line := next()
		require.True(t, strings.HasPrefix(line, "data: "), line)
		var event model.Event
		require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event))
		assert.Equal(t, int64(i+1), event.Pipeline.Number)
		assert.Equal(t, status, event.Pipeline.Status)
	}
}

// funcPubSub subscribes by calling subscribe.
type funcPubSub struct {
	subscribe func(ctx context.Context, receiver pubsub.Receiver) error
}

func (f *funcPubSub) Publish(context.Context, pubsub.Topics, pubsub.Message) error { return nil }

func (f *funcPubSub) Subscribe(ctx context.Context, _ pubsub.Topics, receiver pubsub.Receiver) error {
	return f.subscribe(ctx, receiver)
}

// TestEventStreamSSEDroppedSubscriber ends the stream of a client the pubsub
// dropped for not keeping up, and counts it.
func TestEventStreamSSEDroppedSubscriber(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), nil, nil, &funcPubSub{
		subscribe: func(context.Context, pubsub.Receiver) error { return pubsub.ErrSubscriberTooSlow },
	})
	t.Cleanup(func() { server.Config.Services.Scheduler = nil })
	before := testutil.ToFloat64(droppedEventStreams)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/stream/events", nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		EventStreamSSE(c)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream of a dropped subscriber did not end")
	}
	assert.InDelta(t, before+1, testutil.ToFloat64(droppedEventStreams), 0)
}

// TestEventStreamSSEWriteDeadline connects a client that never reads: once the
// connection's buffers are full, the write deadline ends the handler.
func TestEventStreamSSEWriteDeadline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	defer func(d time.Duration) { eventWriteTimeout = d }(eventWriteTimeout)
	eventWriteTimeout = 100 * time.Millisecond

	payload := []byte(`"` + strings.Repeat("x", 64*1024) + `"`)
	server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), nil, nil, &funcPubSub{
		subscribe: func(ctx context.Context, receiver pubsub.Receiver) error {
			for ctx.Err() == nil {
				receiver(pubsub.Message{Data: payload})
			}
			return nil
		},
	})
	t.Cleanup(func() { server.Config.Services.Scheduler = nil })

	handlerDone := make(chan struct{})
	router := gin.New()
	router.GET("/stream/events", func(c *gin.Context) {
		defer close(handlerDone)
		EventStreamSSE(c)
	})
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	_, err = io.WriteString(conn, "GET /stream/events HTTP/1.1\r\nHost: test\r\n\r\n")
	require.NoError(t, err)
	// never read from conn

	select {
	case <-handlerDone:
	case <-time.After(20 * time.Second):
		t.Fatal("the handler kept writing to a client that does not read")
	}
}
