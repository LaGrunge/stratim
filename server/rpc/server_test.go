// Copyright 2026 Woodpecker Authors
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

package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/rpc/proto"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	queue_mocks "go.woodpecker-ci.org/woodpecker/v3/server/queue/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/version"
)

// newTestServer builds a WoodpeckerServer whose peer talks to the given mocked
// store.
func newTestServer(t *testing.T, store *store_mocks.MockStore) *WoodpeckerServer {
	t.Helper()
	server, ok := NewWoodpeckerServer(nil, nil, store, pipeline.WorkflowEnd{}).(*WoodpeckerServer)
	require.True(t, ok)
	return server
}

func ctxWithAgentID(id int64) context.Context {
	return context.WithValue(context.Background(), agentIDKey, id)
}

func TestNewWoodpeckerServer(t *testing.T) {
	t.Parallel()

	srv := NewWoodpeckerServer(nil, nil, store_mocks.NewMockStore(t), pipeline.WorkflowEnd{})
	assert.NotNil(t, srv)
	assert.IsType(t, &WoodpeckerServer{}, srv)
}

func TestServerVersion(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, store_mocks.NewMockStore(t))
	res, err := srv.Version(t.Context(), new(proto.Empty))

	require.NoError(t, err)
	assert.Equal(t, proto.Version, res.GetGrpcVersion())
	assert.Equal(t, version.String(), res.GetServerVersion())
}

func TestServerReportHealth(t *testing.T) {
	t.Parallel()

	t.Run("alive system agent updates last contact", func(t *testing.T) {
		t.Parallel()
		store := store_mocks.NewMockStore(t)
		agent := &model.Agent{ID: 7, OwnerID: model.IDNotSet, OrgID: model.IDNotSet}
		store.On("AgentFind", int64(7)).Return(agent, nil)
		store.On("AgentUpdate", mock.MatchedBy(func(a *model.Agent) bool {
			return a.ID == 7 && a.LastContact > 0
		})).Return(nil)

		srv := newTestServer(t, store)
		_, err := srv.ReportHealth(ctxWithAgentID(7), &proto.ReportHealthRequest{Status: "I am alive!"})
		require.NoError(t, err)
	})

	t.Run("unexpected status is rejected", func(t *testing.T) {
		t.Parallel()
		store := store_mocks.NewMockStore(t)
		agent := &model.Agent{ID: 7, OwnerID: model.IDNotSet, OrgID: model.IDNotSet}
		store.On("AgentFind", int64(7)).Return(agent, nil)

		srv := newTestServer(t, store)
		_, err := srv.ReportHealth(ctxWithAgentID(7), &proto.ReportHealthRequest{Status: "nope"})
		assert.Error(t, err)
	})

	t.Run("missing agent id in context errors", func(t *testing.T) {
		t.Parallel()
		srv := newTestServer(t, store_mocks.NewMockStore(t))
		_, err := srv.ReportHealth(t.Context(), &proto.ReportHealthRequest{Status: "I am alive!"})
		assert.Error(t, err)
	})
}

func TestServerUnregisterAgent(t *testing.T) {
	t.Parallel()

	t.Run("system agent is deleted", func(t *testing.T) {
		t.Parallel()
		store := store_mocks.NewMockStore(t)
		agent := &model.Agent{ID: 9, OwnerID: model.IDNotSet, OrgID: model.IDNotSet}
		store.On("AgentFind", int64(9)).Return(agent, nil)
		store.On("AgentDelete", agent).Return(nil)

		srv := newTestServer(t, store)
		_, err := srv.UnregisterAgent(ctxWithAgentID(9), new(proto.Empty))
		require.NoError(t, err)
	})

	t.Run("non-system agent is kept", func(t *testing.T) {
		t.Parallel()
		store := store_mocks.NewMockStore(t)
		// OwnerID set -> individual agent token -> must not be deleted
		agent := &model.Agent{ID: 9, OwnerID: 42, OrgID: model.IDNotSet}
		store.On("AgentFind", int64(9)).Return(agent, nil)

		srv := newTestServer(t, store)
		_, err := srv.UnregisterAgent(ctxWithAgentID(9), new(proto.Empty))
		require.NoError(t, err)
		store.AssertNotCalled(t, "AgentDelete", mock.Anything)
	})

	t.Run("propagates delete error", func(t *testing.T) {
		t.Parallel()
		delErr := errors.New("delete failed")
		store := store_mocks.NewMockStore(t)
		agent := &model.Agent{ID: 9, OwnerID: model.IDNotSet, OrgID: model.IDNotSet}
		store.On("AgentFind", int64(9)).Return(agent, nil)
		store.On("AgentDelete", agent).Return(delErr)

		srv := newTestServer(t, store)
		_, err := srv.UnregisterAgent(ctxWithAgentID(9), new(proto.Empty))
		assert.ErrorIs(t, err, delErr)
	})
}

// An agent's finalizers' end comes as how long it still lasts, counted from
// the server's clock when it comes: the agent's clock does not matter.
func TestExtendFinalizeLeft(t *testing.T) {
	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
	mockStore.On("AgentUpdate", mock.Anything).Return(nil).Maybe()
	mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
	mockStore.On("GetPipeline", int64(20)).Return(defaultPipeline(model.StatusRunning), nil)
	mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
	mockQueue := queue_mocks.NewMockQueue(t)
	var got time.Time
	mockQueue.On("ExtendFinalizing", mock.Anything, int64(1), "30", mock.Anything).Run(func(args mock.Arguments) {
		got, _ = args.Get(3).(time.Time)
	}).Return(nil).Once()
	srv := &WoodpeckerServer{peer: newTestRPC(t, mockStore, mockQueue)}

	left := int64(90_000)
	before := time.Now()
	_, err := srv.Extend(ctxWithAgentID(1), &proto.ExtendRequest{Id: "30", FinalizeLeftMs: &left})
	require.NoError(t, err)
	after := time.Now()
	assert.False(t, got.Before(before.Add(90*time.Second)), "counted from the server's clock")
	assert.False(t, got.After(after.Add(90*time.Second)))

	assert.Equal(t, before.Add(-time.Second), finalizeDeadline(-1000, before), "ended a second ago")
}

// An older agent tells no end: a plain Extend.
func TestExtendWithoutFinalizeLeft(t *testing.T) {
	mockStore := store_mocks.NewMockStore(t)
	mockStore.On("AgentFind", int64(1)).Return(defaultAgent(), nil)
	mockStore.On("AgentUpdate", mock.Anything).Return(nil).Maybe()
	mockStore.On("WorkflowLoad", int64(30)).Return(defaultWorkflow(model.StatusRunning), nil)
	mockStore.On("GetPipeline", int64(20)).Return(defaultPipeline(model.StatusRunning), nil)
	mockStore.On("GetRepo", int64(10)).Return(defaultRepo(), nil)
	mockQueue := queue_mocks.NewMockQueue(t)
	mockQueue.On("Extend", mock.Anything, int64(1), "30").Return(nil).Once()
	srv := &WoodpeckerServer{peer: newTestRPC(t, mockStore, mockQueue)}

	_, err := srv.Extend(ctxWithAgentID(1), &proto.ExtendRequest{Id: "30"})
	require.NoError(t, err)
}
