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

package woodpecker

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipelineEvents(t *testing.T) {
	var gotPath, gotQuery, gotLastEventID string
	status := http.StatusOK
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery, gotLastEventID = r.URL.Path, r.URL.RawQuery, r.Header.Get("Last-Event-ID")
		if status != http.StatusOK {
			http.Error(w, "not found", status)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "retry: 3000\n\n"+
			"id: 40\nevent: hello\ndata: {\"v\":1,\"server\":\"https://ci.example.test\"}\n\n"+
			"id: 41\r\nevent: step.failed\r\ndata: {\"v\":1,\"id\":41,\"type\":\"step.failed\",\"severity\":\"error\",\r\n"+
			"data: \"pipeline_id\":812,\"status\":\"failure\"}\r\n\r\n"+
			"id: 42\n: ping\n\n"+
			": a comment alone\n\n"+
			"event: reset\ndata: {\"v\":1,\"reason\":\"expired\"}\n\n"+
			"id: 43\nevent: step.explained\ndata:{\"v\":1,\"id\":43,\"type\":\"step.explained\",\"severity\":\"error\",\"cause\":{\"kind\":\"exit\",\"title\":\"exit 1\",\"confidence\":\"low\",\"untrusted\":true}}\n\n"+
			"id: 44\nevent: step.started\ndata: {\"v\":1}") // cut before its end
	}))
	defer ts.Close()
	client := NewClient(ts.URL, ts.Client())

	severity := SeverityInfo
	stream, err := client.PipelineEvents(t.Context(), PipelineEventsOptions{
		Repos: []string{"owner/name", "7"}, Branch: "main", PipelineID: 812, MinSeverity: &severity,
		Types: []PipelineEventType{StepFailed, PipelineFinished}, LastEventID: "39",
	})
	require.NoError(t, err)
	defer stream.Close()
	assert.Equal(t, "/api/v2/events", gotPath)
	assert.Equal(t, "branch=main&min_severity=info&pipeline=812&repo=owner%2Fname&repo=7&types=step.failed%2Cpipeline.finished", gotQuery)
	assert.Equal(t, "39", gotLastEventID)

	next := func() *EventsFrame {
		t.Helper()
		f, err := stream.Next()
		require.NoError(t, err)
		return f
	}

	f := next()
	assert.Equal(t, 3*time.Second, f.Retry)
	assert.False(t, f.IsPipelineEvent())

	f = next()
	assert.Equal(t, "40", f.ID)
	assert.Equal(t, EventsFrameHello, f.Event)
	var hello EventsHello
	require.NoError(t, f.Decode(&hello))
	assert.Equal(t, "https://ci.example.test", hello.Server)

	f = next()
	assert.Equal(t, "41", f.ID)
	assert.True(t, f.IsPipelineEvent())
	var e PipelineEvent
	require.NoError(t, f.Decode(&e), "data lines are joined with newlines, CRLF ends a line")
	assert.Equal(t, StepFailed, e.Type)
	assert.Equal(t, SeverityError, e.Severity)
	assert.Equal(t, int64(812), e.PipelineID)

	f = next()
	assert.Equal(t, &EventsFrame{ID: "42", Data: []byte{}}, f, "a ping moves the cursor")

	f = next()
	assert.Equal(t, EventsFrameReset, f.Event)
	assert.Empty(t, f.ID)
	var reset EventsReset
	require.NoError(t, f.Decode(&reset))
	assert.Equal(t, EventsResetExpired, reset.Reason)

	f = next()
	require.NoError(t, f.Decode(&e), "data without a space after the colon")
	require.NotNil(t, e.Cause)
	assert.Equal(t, "exit", e.Cause.Kind)

	_, err = stream.Next()
	assert.ErrorIs(t, err, io.EOF, "a frame cut by the end of the stream is dropped")

	status = http.StatusNotFound
	_, err = client.PipelineEvents(t.Context(), PipelineEventsOptions{})
	var clientErr *ClientError
	require.True(t, errors.As(err, &clientErr))
	assert.Equal(t, http.StatusNotFound, clientErr.StatusCode)
	assert.Empty(t, gotQuery, "the server's defaults")
}

func TestEventsFrameReaderBounds(t *testing.T) {
	next := func(text string) (*EventsFrame, error) {
		return NewEventsFrameReader(io.NopCloser(strings.NewReader(text))).Next()
	}
	_, err := next("data: " + strings.Repeat("x", EventsMaxFrame) + "\n\n")
	assert.ErrorIs(t, err, ErrEventsFrameTooLarge, "a line beyond the bound")

	line := "data: " + strings.Repeat("x", 1000) + "\n"
	_, err = next(strings.Repeat(line, EventsMaxFrame/1000+1) + "\n")
	assert.ErrorIs(t, err, ErrEventsFrameTooLarge, "data lines beyond the bound together")

	f, err := next(strings.Repeat(line, EventsMaxFrame/1000-2) + "\n")
	require.NoError(t, err, "a frame within the bound")
	assert.Len(t, f.Data, (EventsMaxFrame/1000-2)*1001-1)

	f, err = next("id: 4\x002\nevent: hello\ndata: {}\n\n")
	require.NoError(t, err)
	assert.Empty(t, f.ID, "an id with NUL is ignored")
}
