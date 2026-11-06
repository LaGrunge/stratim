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

package setup

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/api"
	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
)

// SSEFrame is one server-sent event.
type SSEFrame struct {
	ID, Event, Data string
}

// EventsV2Stream reads a stream of /api/v2/events.
type EventsV2Stream struct {
	frames chan SSEFrame
}

// eventsV2URL serves /api/v2/events of the test server over HTTP, once, to
// the fixture owner, who may pull the fixture repo (a private one).
func (env *ServerEnv) eventsV2URL(t *testing.T) string {
	t.Helper()
	env.eventsV2Once.Do(func() {
		require.NoError(t, env.Store.PermUpsert(&model.Perm{UserID: env.Fixtures.Owner.ID, RepoID: env.Fixtures.Repo.ID, Pull: true, Synced: time.Now().Unix()}))
		gin.SetMode(gin.TestMode)
		e := gin.New()
		e.Use(func(c *gin.Context) {
			c.Set("store", env.Store)
			c.Set("user", env.Fixtures.Owner)
		})
		e.GET("/api/v2/events", session.MustUser(), api.EventStreamV2(env.Events))
		srv := httptest.NewServer(e)
		t.Cleanup(srv.Close)
		env.eventsV2Base = srv.URL
	})
	return env.eventsV2Base
}

// StreamEventsV2 opens a stream of /api/v2/events with the query, from the
// event with lastEventID if it is not empty. It returns once the stream said
// hello, so no event published afterwards is missed. The stream ends with
// the test.
func (env *ServerEnv) StreamEventsV2(t *testing.T, query, lastEventID string) *EventsV2Stream {
	t.Helper()
	ctx, cancel := context.WithCancelCause(t.Context())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, env.eventsV2URL(t)+"/api/v2/events?"+query, nil)
	require.NoError(t, err)
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // closed with the test
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "open the event stream")

	s := &EventsV2Stream{frames: make(chan SSEFrame, 1000)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(s.frames)
		r := bufio.NewReader(resp.Body)
		var f SSEFrame
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSuffix(line, "\n")
			name, value, _ := strings.Cut(line, ": ")
			switch name {
			case "":
				if f != (SSEFrame{}) {
					s.frames <- f
				}
				f = SSEFrame{}
			case "id":
				f.ID = value
			case "event":
				f.Event = value
			case "data":
				f.Data = value
			}
		}
	}()
	t.Cleanup(func() {
		cancel(nil)
		resp.Body.Close()
		<-done
	})

	hello := s.Next(t, shortTimeout)
	require.Equal(t, "hello", hello.Event, "the stream starts with hello")
	return s
}

// Next is the next frame, failing the test after timeout.
func (s *EventsV2Stream) Next(t *testing.T, timeout time.Duration) SSEFrame {
	t.Helper()
	select {
	case f, ok := <-s.frames:
		require.True(t, ok, "the event stream ended")
		return f
	case <-time.After(timeout):
		t.Fatal("timeout waiting for an event")
		return SSEFrame{}
	}
}

// Until collects the events until one matches, which is the last of them.
func (s *EventsV2Stream) Until(t *testing.T, timeout time.Duration, match func(events.Event) bool) ([]events.Event, []SSEFrame) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got []events.Event
	var frames []SSEFrame
	for {
		f := s.Next(t, time.Until(deadline))
		if f.ID == "" {
			continue // hello, reset
		}
		var e events.Event
		require.NoError(t, json.Unmarshal([]byte(f.Data), &e), "decode event %s", f.Data)
		got = append(got, e)
		frames = append(frames, f)
		if match(e) {
			return got, frames
		}
	}
}

// PipelineFinishedEvent matches the end of the pipeline.
func PipelineFinishedEvent(pipelineID int64) func(events.Event) bool {
	return func(e events.Event) bool {
		return e.Type == events.PipelineFinished && e.PipelineID == pipelineID
	}
}
