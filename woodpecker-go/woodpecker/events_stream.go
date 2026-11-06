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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const pathEvents = "%s/api/v2/events"

// The names of the frames about the stream itself.
const (
	EventsFrameHello = "hello"
	EventsFrameReset = "reset"
	EventsFrameError = "error"
)

// EventsMaxFrame bounds a frame of the stream, its id, name and data
// together; an event takes at most 1536 bytes.
const EventsMaxFrame = 64 << 10

// ErrEventsFrameTooLarge is a frame beyond EventsMaxFrame.
var ErrEventsFrameTooLarge = fmt.Errorf("an events frame is larger than %d bytes", EventsMaxFrame)

// eventsMaxErrorBody bounds the body of an answer other than a stream read
// into a ClientError.
const eventsMaxErrorBody = 4096

// PipelineEventsOptions are the filters of a stream of pipeline events and
// where it goes on from.
type PipelineEventsOptions struct {
	// Repos are repositories by id or full name.
	Repos []string
	// Branch is the branch of the pipelines.
	Branch string
	// PipelineID is the id of the one pipeline.
	PipelineID int64
	// Number is the number of the one pipeline, of the one repo of Repos.
	Number int64
	// MinSeverity is the least severity; nil is the server's default,
	// notice.
	MinSeverity *EventSeverity
	// Types are the types of the events; none is all.
	Types []PipelineEventType
	// LastEventID is the id of the last frame got, sent as Last-Event-ID:
	// the stream replays the events after it.
	LastEventID string
}

// EventsFrame is one frame of a stream of pipeline events.
type EventsFrame struct {
	// ID is the frame's id, if it has one: the cursor to go on from.
	ID string
	// Event is hello, reset or error about the stream, or the type of a
	// pipeline event; none on a frame that only moves the cursor (a ping) or
	// sets Retry.
	Event string
	// Data is the JSON of the frame.
	Data []byte
	// Retry is the delay to reconnect after, when the frame sets it.
	Retry time.Duration
}

// IsPipelineEvent tells whether the frame is a pipeline event, not one about
// the stream.
func (f *EventsFrame) IsPipelineEvent() bool {
	switch f.Event {
	case "", EventsFrameHello, EventsFrameReset, EventsFrameError:
		return false
	}
	return len(f.Data) > 0
}

// Decode decodes the frame's JSON into v, e.g. a PipelineEvent, an
// EventsHello, an EventsReset or an EventsError.
func (f *EventsFrame) Decode(v any) error {
	if err := json.Unmarshal(f.Data, v); err != nil {
		return fmt.Errorf("decode %s frame: %w", f.Event, err)
	}
	return nil
}

// PipelineEvents opens a stream of pipeline events. The stream ends with
// the context or Close; a stream that ends otherwise is reconnected to with
// the ID of the last frame that had one in LastEventID, as an
// EventsFollower does.
func (c *client) PipelineEvents(ctx context.Context, opt PipelineEventsOptions) (EventsFrameReader, error) {
	query := url.Values{}
	for _, repo := range opt.Repos {
		query.Add("repo", repo)
	}
	if opt.Branch != "" {
		query.Set("branch", opt.Branch)
	}
	if opt.PipelineID > 0 {
		query.Set("pipeline", strconv.FormatInt(opt.PipelineID, 10))
	}
	if opt.Number > 0 {
		query.Set("number", strconv.FormatInt(opt.Number, 10))
	}
	if opt.MinSeverity != nil {
		query.Set("min_severity", opt.MinSeverity.String())
	}
	if len(opt.Types) > 0 {
		types := make([]string, len(opt.Types))
		for i, t := range opt.Types {
			types[i] = string(t)
		}
		query.Set("types", strings.Join(types, ","))
	}
	uri := fmt.Sprintf(pathEvents, c.addr)
	if len(query) > 0 {
		uri += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	if opt.LastEventID != "" {
		req.Header.Set("Last-Event-ID", opt.LastEventID)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, eventsMaxErrorBody))
		return nil, &ClientError{StatusCode: resp.StatusCode, Message: string(body)}
	}
	return NewEventsFrameReader(resp.Body), nil
}

// EventsFrameReader reads the frames of a stream of pipeline events.
type EventsFrameReader interface {
	// Next reads the next frame. It returns io.EOF once the stream ended,
	// and ErrEventsFrameTooLarge for a frame beyond EventsMaxFrame.
	// Comments, such as the text of a ping, are skipped.
	Next() (*EventsFrame, error)
	// Close ends the stream.
	Close() error
}

type eventsFrameReader struct {
	body    io.Closer
	scanner *bufio.Scanner
}

// NewEventsFrameReader reads the frames of a stream of server-sent events
// from the body.
func NewEventsFrameReader(body io.ReadCloser) EventsFrameReader {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 4096), EventsMaxFrame)
	return &eventsFrameReader{body: body, scanner: scanner}
}

func (s *eventsFrameReader) Next() (*EventsFrame, error) {
	var (
		f    EventsFrame
		data [][]byte
		seen bool
		size int
	)
	for s.scanner.Scan() {
		line := bytes.TrimSuffix(s.scanner.Bytes(), []byte("\r"))
		if len(line) == 0 {
			if !seen {
				continue
			}
			f.Data = bytes.Join(data, []byte("\n"))
			return &f, nil
		}
		if line[0] == ':' {
			continue // a comment
		}
		name, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(name) {
		case "id", "event", "data":
			size += len(value) + 1
			if size > EventsMaxFrame {
				return nil, ErrEventsFrameTooLarge
			}
		}
		switch string(name) {
		case "id":
			if bytes.IndexByte(value, 0) < 0 {
				f.ID = string(value)
				seen = true
			}
		case "event":
			f.Event = string(value)
			seen = true
		case "data":
			data = append(data, bytes.Clone(value))
			seen = true
		case "retry":
			if ms, err := strconv.ParseUint(string(value), 10, 32); err == nil {
				f.Retry = time.Duration(ms) * time.Millisecond
				seen = true
			}
		}
	}
	if err := s.scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return nil, ErrEventsFrameTooLarge
		}
		return nil, err
	}
	return nil, io.EOF
}

func (s *eventsFrameReader) Close() error {
	return s.body.Close()
}
