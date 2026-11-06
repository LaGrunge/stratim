// Copyright 2026 Woodpecker Authors
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
	"errors"
	"fmt"
	"slices"
	"sync"

	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub"
)

// maxQueuedMessages bounds the messages queued for one subscriber. A
// subscriber that falls this far behind is dropped: its Subscribe returns
// pubsub.ErrSubscriberTooSlow, which ends an event stream so that its client
// reconnects and loads the current state.
const maxQueuedMessages = 1000

type publisher struct {
	sync.RWMutex

	subs map[*subscriber]struct{}
	// limit is the queue limit of each subscriber, maxQueuedMessages.
	limit int
}

// subscriber queues the messages published to its topics and hands them to
// its receiver one at a time, in the order they were published. Publish never
// waits for a receiver. Ordered delivery matters to the web UI: every event
// carries the whole pipeline, so an older event delivered after a newer one
// would show a finished step as still running until the next event.
//
// A queued message with the same key as a new one is superseded by it: only
// the newest is kept, at the place of the new one, so what is delivered stays
// in publish order. Beyond the limit, the subscriber is dropped.
type subscriber struct {
	topics []string
	limit  int

	mu        sync.Mutex
	queue     []pubsub.Message
	overflown bool
	wake      chan struct{}
}

func (s *subscriber) enqueue(message pubsub.Message) {
	s.mu.Lock()
	if s.overflown {
		s.mu.Unlock()
		return
	}
	if message.Key != "" {
		s.queue = slices.DeleteFunc(s.queue, func(m pubsub.Message) bool { return m.Key == message.Key })
	}
	if len(s.queue) >= s.limit {
		s.overflown = true
		s.queue = nil
	} else {
		s.queue = append(s.queue, message)
	}
	s.mu.Unlock()

	select {
	case s.wake <- struct{}{}:
	default: // a wake-up is already pending
	}
}

// next takes the oldest queued message. It reports false once the queue is
// empty, and an error once the subscriber fell too far behind.
func (s *subscriber) next() (pubsub.Message, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.overflown {
		return pubsub.Message{}, false, pubsub.ErrSubscriberTooSlow
	}
	if len(s.queue) == 0 {
		return pubsub.Message{}, false, nil
	}
	message := s.queue[0]
	s.queue[0] = pubsub.Message{} // release the data
	s.queue = s.queue[1:]
	return message, true, nil
}

// New creates an in-memory publisher.
func New() pubsub.PubSub {
	return &publisher{
		subs:  make(map[*subscriber]struct{}),
		limit: maxQueuedMessages,
	}
}

func (p *publisher) Publish(_ context.Context, topics pubsub.Topics, message pubsub.Message) error {
	if len(topics) == 0 {
		return fmt.Errorf("%w: specify at least one", pubsub.ErrNoTopic)
	}

	p.RLock()
	defer p.RUnlock()

	for s := range p.subs {
		for t := range topics {
			if slices.Contains(s.topics, t) {
				s.enqueue(message)
				break
			}
		}
	}

	return nil
}

func (p *publisher) Subscribe(c context.Context, topics pubsub.Topics, receiver pubsub.Receiver) error {
	if len(topics) == 0 {
		return fmt.Errorf("%w: subscribe to at least one", pubsub.ErrNoTopic)
	}
	if receiver == nil {
		return errors.New("subscribe needs a receiver")
	}

	sub := &subscriber{limit: p.limit, wake: make(chan struct{}, 1)}
	for k := range topics {
		sub.topics = append(sub.topics, k)
	}

	p.Lock()
	p.subs[sub] = struct{}{}
	p.Unlock()

	defer func() {
		p.Lock()
		delete(p.subs, sub)
		p.Unlock()
	}()

	for {
		select {
		case <-c.Done():
			return nil
		case <-sub.wake:
			for c.Err() == nil {
				message, ok, err := sub.next()
				if err != nil {
					return err
				}
				if !ok {
					break
				}
				receiver(message)
			}
		}
	}
}
