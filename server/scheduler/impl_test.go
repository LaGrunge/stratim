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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
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

// TestPrioritizeWorkflows moves the pending ones of the workflows given, and
// does nothing for none.
func TestPrioritizeWorkflows(t *testing.T) {
	q := queue.NewMemoryQueue(t.Context())
	s := NewScheduler(t.Context(), nil, q, nil)
	require.NoError(t, q.PushAtOnce(t.Context(), []*model.Task{{ID: "1"}, {ID: "2"}}))

	moved, err := s.PrioritizeWorkflows(t.Context(), nil)
	require.NoError(t, err)
	assert.Empty(t, moved)

	moved, err = s.PrioritizeWorkflows(t.Context(), []string{"2", "3"})
	require.NoError(t, err)
	assert.Equal(t, []string{"2"}, moved)
}

type recordingObserver struct {
	pipelines []int64
}

func (r *recordingObserver) Observe(_ *model.Repo, p *model.Pipeline) {
	r.pipelines = append(r.pipelines, p.ID)
}

// TestPublishPipelineEventObserved hands every published snapshot to the
// observer, the one StartPipeline publishes too.
func TestPublishPipelineEventObserved(t *testing.T) {
	observer := &recordingObserver{}
	q := queue.NewMemoryQueue(t.Context())
	s := NewScheduler(t.Context(), nil, q, &recordingPubSub{}, WithPipelineObserver(observer))

	repo := &model.Repo{ID: 3}
	require.NoError(t, s.PublishPipelineEvent(t.Context(), repo, &model.Pipeline{ID: 11}))
	require.NoError(t, s.StartPipeline(t.Context(), repo, &model.Pipeline{ID: 12}, nil))

	assert.Equal(t, []int64{11, 12}, observer.pipelines)
}

// orderObserver notes its name on a shared log for every snapshot.
type orderObserver struct {
	name string
	log  *[]string
}

func (o *orderObserver) Observe(_ *model.Repo, p *model.Pipeline) {
	*o.log = append(*o.log, fmt.Sprintf("%s:%d", o.name, p.ID))
}

// TestPublishPipelineEventObservers hands every snapshot to each observer,
// in the order they were given.
func TestPublishPipelineEventObservers(t *testing.T) {
	var log []string
	q := queue.NewMemoryQueue(t.Context())
	s := NewScheduler(t.Context(), nil, q, &recordingPubSub{},
		WithPipelineObserver(&orderObserver{name: "first", log: &log}),
		WithPipelineObserver(&orderObserver{name: "second", log: &log}))

	repo := &model.Repo{ID: 3}
	require.NoError(t, s.PublishPipelineEvent(t.Context(), repo, &model.Pipeline{ID: 11}))
	require.NoError(t, s.StartPipeline(t.Context(), repo, &model.Pipeline{ID: 12}, nil))

	assert.Equal(t, []string{"first:11", "second:11", "first:12", "second:12"}, log)
}
