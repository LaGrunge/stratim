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
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub"
)

// probeID marks the messages RecordEvents publishes to learn that its
// subscription is live; they never reach the recorded events.
const probeID = "e2e-subscription-probe"

// EventRecorder collects the pipeline events the server publishes for the
// fixture repository, the same stream the UI's event feed reads.
//
// The in-memory pubsub delivers every message on its own goroutine, so the
// recorded order is not the order of publishing: assert on the set of events
// or wait for the one you need, never on positions.
type EventRecorder struct {
	mu     sync.Mutex
	events []model.Event
	wake   chan struct{}
}

// RecordEvents subscribes to the fixture repository's topic and returns once
// the subscription is live, so no event published afterwards is missed. The
// subscription ends with the test.
func (env *ServerEnv) RecordEvents(t *testing.T) *EventRecorder {
	t.Helper()

	r := &EventRecorder{wake: make(chan struct{}, 1)}
	topic := pubsub.GetRepoTopic(env.Fixtures.Repo)
	live := make(chan struct{})
	var once sync.Once

	ctx, cancel := context.WithCancelCause(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = env.PubSub.Subscribe(ctx, pubsub.Topics{topic: {}}, func(m pubsub.Message) {
			if m.ID == probeID {
				once.Do(func() { close(live) })
				return
			}
			var event model.Event
			if err := json.Unmarshal(m.Data, &event); err != nil {
				t.Errorf("decode pipeline event %s: %v", m.ID, err)
				return
			}
			r.add(event)
		})
	}()
	t.Cleanup(func() {
		cancel(nil)
		<-done
	})

	// Subscribe registers asynchronously; publish probes until one arrives.
	deadline := time.After(shortTimeout)
	for {
		require.NoError(t, env.PubSub.Publish(ctx, pubsub.Topics{topic: {}}, pubsub.Message{ID: probeID}))
		select {
		case <-live:
			return r
		case <-deadline:
			t.Fatal("timeout waiting for the event subscription to become live")
		case <-time.After(defaultInterval):
		}
	}
}

func (r *EventRecorder) add(event model.Event) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Events returns a copy of the events recorded so far.
func (r *EventRecorder) Events() []model.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]model.Event(nil), r.events...)
}

// WaitFor blocks until an event matching match has been recorded, either
// before or during the call, and returns it. It fails the test on timeout.
func (r *EventRecorder) WaitFor(t *testing.T, timeout time.Duration, match func(model.Event) bool) model.Event {
	t.Helper()

	deadline := time.After(timeout)
	for {
		for _, event := range r.Events() {
			if match(event) {
				return event
			}
		}
		select {
		case <-r.wake:
		case <-deadline:
			t.Fatalf("timeout waiting for a matching pipeline event; recorded %d events", len(r.Events()))
			return model.Event{}
		}
	}
}

// PipelineStatus matches events that report the pipeline with the given ID in
// the given status.
func PipelineStatus(pipelineID int64, status model.StatusValue) func(model.Event) bool {
	return func(event model.Event) bool {
		return event.Pipeline.ID == pipelineID && event.Pipeline.Status == status
	}
}
