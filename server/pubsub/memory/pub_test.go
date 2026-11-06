// Copyright 2023 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub"
)

func TestPubsub(t *testing.T) {
	var (
		wg sync.WaitGroup

		testTopic = map[string]struct{}{"test": {}}

		testMessage = pubsub.Message{
			Data: []byte("test"),
		}
	)

	ctx, cancel := context.WithCancelCause(
		t.Context(),
	)
	broker := New()

	assert.Error(t, broker.Subscribe(ctx, nil, func(pubsub.Message) {}))
	go func() {
		assert.NoError(t, broker.Subscribe(ctx, testTopic, func(message pubsub.Message) { assert.Equal(t, testMessage, message); wg.Done() }))
	}()
	go func() {
		assert.NoError(t, broker.Subscribe(ctx, testTopic, func(pubsub.Message) { wg.Done() }))
	}()

	<-time.After(500 * time.Millisecond)

	wg.Add(2)
	go func() {
		assert.NoError(t, broker.Publish(ctx, testTopic, testMessage))
	}()

	wg.Wait()
	cancel(nil)
}

func TestPubsubConcurrentCancel(t *testing.T) {
	testTopic := map[string]struct{}{"test": {}}
	broker := New()

	for range 100 {
		ctx, cancel := context.WithCancelCause(t.Context())
		ch := make(chan []byte) // Unbuffered to force blocking sends

		var wg sync.WaitGroup
		wg.Go(func() {
			_ = broker.Subscribe(ctx, testTopic, func(m pubsub.Message) {
				select {
				case <-ctx.Done():
				case ch <- m.Data:
				}
			})
		})

		// Start publishing many messages to increase chance of blocking send
		var pubWg sync.WaitGroup
		for range 100 {
			pubWg.Go(func() {
				_ = broker.Publish(ctx, testTopic, pubsub.Message{Data: []byte("x")})
			})
		}

		// Cancel while publishes are in flight to race with pending sends
		cancel(nil)
		pubWg.Wait()
		wg.Wait()
	}
}

// TestPubsubKeepsOrder publishes a run of messages and checks every
// subscriber receives them in publish order, also when it is slow to consume:
// a stale pipeline event delivered after a newer one would show the UI an old
// state.
func TestPubsubKeepsOrder(t *testing.T) {
	const count = 500
	testTopic := map[string]struct{}{"test": {}}
	broker := New()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)

	type result struct {
		mu   sync.Mutex
		got  []string
		done chan struct{}
	}
	subscribe := func(slow bool) *result {
		r := &result{done: make(chan struct{})}
		go func() {
			_ = broker.Subscribe(ctx, testTopic, func(m pubsub.Message) {
				if slow {
					time.Sleep(50 * time.Microsecond)
				}
				r.mu.Lock()
				r.got = append(r.got, string(m.Data))
				if len(r.got) == count {
					close(r.done)
				}
				r.mu.Unlock()
			})
		}()
		return r
	}
	fast, slow := subscribe(false), subscribe(true)
	waitForSubscribers(t, broker, 2)

	want := make([]string, 0, count)
	for i := range count {
		data := strconv.Itoa(i)
		want = append(want, data)
		assert.NoError(t, broker.Publish(ctx, testTopic, pubsub.Message{Data: []byte(data)}))
	}

	for _, r := range []*result{fast, slow} {
		select {
		case <-r.done:
		case <-time.After(10 * time.Second):
			t.Fatal("not all messages were delivered")
		}
		r.mu.Lock()
		assert.Equal(t, want, r.got)
		r.mu.Unlock()
	}
}

func TestPubsubOnlyMatchingTopics(t *testing.T) {
	broker := New()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)

	got := make(chan string, 10)
	go func() {
		_ = broker.Subscribe(ctx, map[string]struct{}{"a": {}}, func(m pubsub.Message) { got <- string(m.Data) })
	}()
	waitForSubscribers(t, broker, 1)

	assert.NoError(t, broker.Publish(ctx, map[string]struct{}{"b": {}}, pubsub.Message{Data: []byte("b")}))
	assert.NoError(t, broker.Publish(ctx, map[string]struct{}{"a": {}, "b": {}}, pubsub.Message{Data: []byte("ab")}))
	assert.Equal(t, "ab", <-got)
	assert.Empty(t, got)
}

// stalledSubscriber subscribes with a receiver that records the messages and
// blocks on the first one until release is closed.
type stalledSubscriber struct {
	mu      sync.Mutex
	got     []string
	first   chan struct{}
	release chan struct{}
	done    chan error
}

func subscribeStalled(ctx context.Context, broker pubsub.PubSub, topics pubsub.Topics) *stalledSubscriber {
	s := &stalledSubscriber{first: make(chan struct{}), release: make(chan struct{}), done: make(chan error, 1)}
	go func() {
		s.done <- broker.Subscribe(ctx, topics, func(m pubsub.Message) {
			s.mu.Lock()
			s.got = append(s.got, string(m.Data))
			n := len(s.got)
			s.mu.Unlock()
			if n == 1 {
				close(s.first)
				<-s.release
			}
		})
	}()
	return s
}

func (s *stalledSubscriber) received() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.got)
}

// TestPubsubCoalescesByKey keeps only the newest of the queued messages with
// the same key, in the place of the newest, so what is delivered stays in
// publish order.
func TestPubsubCoalescesByKey(t *testing.T) {
	testTopic := pubsub.Topics{"test": {}}
	broker := New()
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)

	s := subscribeStalled(ctx, broker, testTopic)
	waitForSubscribers(t, broker, 1)

	publish := func(key, data string) {
		assert.NoError(t, broker.Publish(ctx, testTopic, pubsub.Message{Key: key, Data: []byte(data)}))
	}
	publish("a", "a1")
	<-s.first // a1 is with the receiver, the rest queues up
	publish("a", "a2")
	publish("b", "b1")
	publish("", "x1")
	publish("a", "a3")
	publish("", "x2")
	publish("b", "b2")
	close(s.release)

	want := []string{"a1", "x1", "a3", "x2", "b2"}
	assert.Eventually(t, func() bool { return len(s.received()) == len(want) }, 5*time.Second, time.Millisecond)
	assert.Equal(t, want, s.received())
}

// TestPubsubDropsStalledSubscriber checks a subscriber that stops consuming
// queues at most its limit and is then dropped with ErrSubscriberTooSlow,
// while other subscribers keep receiving.
func TestPubsubDropsStalledSubscriber(t *testing.T) {
	const limit = 5
	testTopic := pubsub.Topics{"test": {}}
	p := &publisher{subs: make(map[*subscriber]struct{}), limit: limit}
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)

	stalled := subscribeStalled(ctx, p, testTopic)
	waitForSubscribers(t, p, 1)
	assert.NoError(t, p.Publish(ctx, testTopic, pubsub.Message{Data: []byte("first")}))
	<-stalled.first

	healthy := make(chan string, 1000)
	go func() {
		_ = p.Subscribe(ctx, testTopic, func(m pubsub.Message) { healthy <- string(m.Data) })
	}()
	waitForSubscribers(t, p, 2)

	for i := range 500 {
		assert.NoError(t, p.Publish(ctx, testTopic, pubsub.Message{Data: []byte(strconv.Itoa(i))}))
		p.RLock()
		for sub := range p.subs {
			sub.mu.Lock()
			assert.LessOrEqual(t, len(sub.queue), limit)
			sub.mu.Unlock()
		}
		p.RUnlock()
		// keep the healthy subscriber within its limit
		assert.Equal(t, strconv.Itoa(i), <-healthy)
	}

	close(stalled.release)
	select {
	case err := <-stalled.done:
		assert.ErrorIs(t, err, pubsub.ErrSubscriberTooSlow)
	case <-time.After(5 * time.Second):
		t.Fatal("the stalled subscriber was not dropped")
	}
	assert.Equal(t, []string{"first"}, stalled.received(), "nothing after the overflow is delivered")
	waitForSubscribers(t, p, 1)
}

func waitForSubscribers(t *testing.T, broker pubsub.PubSub, n int) {
	t.Helper()
	p, ok := broker.(*publisher)
	if !ok {
		t.Fatal("not the memory publisher")
	}
	assert.Eventually(t, func() bool {
		p.RLock()
		defer p.RUnlock()
		return len(p.subs) == n
	}, time.Second, time.Millisecond)
}
