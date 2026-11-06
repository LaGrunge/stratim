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

package scheduler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub"
)

type recordingPubSub struct {
	topics   pubsub.Topics
	messages []pubsub.Message
}

func (r *recordingPubSub) Publish(_ context.Context, topics pubsub.Topics, m pubsub.Message) error {
	r.topics = topics
	r.messages = append(r.messages, m)
	return nil
}

func (r *recordingPubSub) Subscribe(context.Context, pubsub.Topics, pubsub.Receiver) error {
	return nil
}

// TestPublishPipelineEventKey keys every pipeline event by its pipeline, so a
// queued older state of the same pipeline can be superseded.
func TestPublishPipelineEventKey(t *testing.T) {
	ps := &recordingPubSub{}
	s := NewScheduler(t.Context(), nil, nil, ps)

	repo := &model.Repo{ID: 3, IsSCMPrivate: true}
	require.NoError(t, s.PublishPipelineEvent(t.Context(), repo, &model.Pipeline{ID: 11}))
	require.NoError(t, s.PublishPipelineEvent(t.Context(), repo, &model.Pipeline{ID: 12}))

	require.Len(t, ps.messages, 2)
	assert.Equal(t, "pipeline.11", ps.messages[0].Key)
	assert.Equal(t, "pipeline.12", ps.messages[1].Key)
	assert.Equal(t, pubsub.Topics{pubsub.GetRepoTopic(repo): {}}, ps.topics, "a private repo is not published publicly")
}
