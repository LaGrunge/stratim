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
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/api"
	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// SSEFrame is one server-sent event.
type SSEFrame struct {
	ID, Event, Data string
}

// EventsV2Stream reads a stream of /api/v2/events.
type EventsV2Stream struct {
	frames chan SSEFrame
	close  func()
	once   sync.Once
}

// Close ends the stream from the client's side; the test does at its end.
func (s *EventsV2Stream) Close() {
	s.once.Do(s.close)
}

// eventsV2URL serves /api/v2 of the test server over HTTP, once, to the
// fixture owner, who may pull the fixture repo (a private one): the events,
// /api/v2/pipelines/{id}/wait, and the explanations and windows of the
// steps' logs.
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
		e.GET("/api/v2/pipelines/:pipeline_id/wait", session.MustUser(), api.WaitPipelineV2(env.Events, env.Durations))
		e.GET("/api/v2/steps/:step_id/explain", session.MustUser(), api.StepExplainV2(env.Explain))
		e.GET("/api/v2/steps/:step_id/log", session.MustUser(), api.StepLogV2(env.LogWindows))
		srv := httptest.NewServer(e)
		t.Cleanup(srv.Close)
		env.eventsV2Base = srv.URL
	})
	return env.eventsV2Base
}

// WaitV2 asks /api/v2/pipelines/{id}/wait with the query, as the fixture
// owner. The caller closes the body.
func (env *ServerEnv) WaitV2(t *testing.T, pipelineID int64, query string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		fmt.Sprintf("%s/api/v2/pipelines/%d/wait?%s", env.eventsV2URL(t), pipelineID, query), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// WaitV2Client is a woodpecker-go client of the test server's /api/v2, as
// the fixture owner.
func (env *ServerEnv) WaitV2Client(t *testing.T) woodpecker.Client {
	t.Helper()
	return woodpecker.NewClient(env.eventsV2URL(t), http.DefaultClient)
}

// GetV2 gets the path of /api/v2, e.g. "steps/12/explain", and decodes the
// JSON it answers with into out, failing the test on a status other than
// 200.
func (env *ServerEnv) GetV2(t *testing.T, path string, out any) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, env.eventsV2URL(t)+"/api/v2/"+path, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s", path)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
}

// StreamEventsV2 opens a stream of /api/v2/events with the query, from the
// event with lastEventID if it is not empty. It returns once the stream said
// hello, so no event published afterwards is missed. The stream ends with
// the test.
func (env *ServerEnv) StreamEventsV2(t *testing.T, query, lastEventID string) *EventsV2Stream {
	t.Helper()
	header := http.Header{}
	if lastEventID != "" {
		header.Set("Last-Event-ID", lastEventID)
	}
	return streamEventsV2(t, env.eventsV2URL(t)+"/api/v2/events?"+query, header)
}

// StreamEventsV2Bot opens a stream of /api/v2/events of APIURL with the
// query and the bot token, as StreamEventsV2 does.
func (env *ServerEnv) StreamEventsV2Bot(t *testing.T, query, secret string) *EventsV2Stream {
	t.Helper()
	return streamEventsV2(t, env.APIURL(t)+"/api/v2/events?"+query, http.Header{woodpecker.BotTokenHeader: {secret}})
}

func streamEventsV2(t *testing.T, url string, header http.Header) *EventsV2Stream {
	t.Helper()
	ctx, cancel := context.WithCancelCause(t.Context())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	maps.Copy(req.Header, header)
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
	s.close = func() {
		cancel(nil)
		resp.Body.Close()
		<-done
	}
	t.Cleanup(s.Close)

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
