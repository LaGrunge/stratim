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

package pubsub

import (
	"context"
	"errors"
	"fmt"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// PubSub provider interface, used to signal pipeline state changes to WebUI.
//
// Delivery contract:
//   - Each subscriber gets its messages one at a time, in publish order.
//     Publish never waits for a subscriber.
//   - A message with a Key supersedes a message with the same Key that is
//     still queued for a subscriber: only the newest is delivered, at the
//     place of the newest, so delivery stays in publish order.
//   - A subscriber that falls too far behind is dropped: its Subscribe
//     returns ErrSubscriberTooSlow and it gets no further messages.
type PubSub interface {
	// Publish pushes a state change to all subscribers,
	// that have at least subscribed to one of the topics we publish it under.
	Publish(context.Context, Topics, Message) error
	// Subscribe gets all state changes that match the same topic.
	// If multiple topics are subscribed, and a message also match multiple,
	// the implementation takes care of deduplication.
	// It blocks until the context is done (returning nil) or the subscriber
	// is dropped (returning ErrSubscriberTooSlow).
	Subscribe(context.Context, Topics, Receiver) error
}

// Message defines a published message.
type Message struct {
	// ID identifies this message.
	ID string `json:"id,omitempty"`

	// Data is the actual data in the entry.
	Data []byte `json:"data"`

	// Key, if set, names what the message is about (e.g. PipelineKey): a
	// newer message with the same key supersedes one not delivered yet, as it
	// carries the whole current state.
	Key string `json:"-"`
}

// Receiver receives published messages.
type Receiver func(Message)

// Topics are key-value pairs, messages are filtered upon
// the the key is the base-key and the value to the sub-key.
type Topics map[string]struct{}

func GetRepoTopic(r *model.Repo) string {
	return fmt.Sprintf("repo.id.%d", r.ID)
}

// PipelineKey is the Message.Key of the state changes of a pipeline.
func PipelineKey(pipelineID int64) string {
	return fmt.Sprintf("pipeline.%d", pipelineID)
}

const PublicTopic = "public"

var (
	ErrNoTopic = errors.New("no topic specified")
	// ErrSubscriberTooSlow is returned by Subscribe when the subscriber did
	// not keep up with the published messages and was dropped.
	ErrSubscriberTooSlow = errors.New("subscriber does not keep up with the published messages")
)
