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

//go:build test

package setup

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/cache"
	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	"go.woodpecker-ci.org/woodpecker/v3/server/explain"
	"go.woodpecker-ci.org/woodpecker/v3/server/explain/worker"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/forgestatus"
	"go.woodpecker-ci.org/woodpecker/v3/server/logging"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	server_rpc "go.woodpecker-ci.org/woodpecker/v3/server/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	"go.woodpecker-ci.org/woodpecker/v3/server/services"
	"go.woodpecker-ci.org/woodpecker/v3/server/services/permissions"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/summary"
	"go.woodpecker-ci.org/woodpecker/v3/server/webhook"
	"go.woodpecker-ci.org/woodpecker/v3/shared/constant"
)

const (
	// TestAgentToken is the shared secret used between the in-process server
	// and agent. Hard-coded for tests — not a real secret.
	TestAgentToken = "test-agent-secret-for-integration-tests"

	// TestJWTSecret is used for signing gRPC auth JWTs.
	TestJWTSecret = "test-jwt-secret-for-integration-tests"

	// TestForgeType is the forge type the mock pretends to be.
	TestForgeType = model.ForgeTypeGitea
)

var configLock = sync.Mutex{}

// ServerEnv holds all the pieces of a running test server environment.
type ServerEnv struct {
	GRPCAddr string
	Store    store.Store
	Queue    queue.Queue
	Fixtures *Fixtures
	Forge    *forge_mocks.MockForge
	// Files are the config the forge serves; Set changes it.
	Files *Files
	// Statuses are the commit statuses the forge took.
	Statuses *Statuses
	Manager  services.Manager
	// PubSub carries the pipeline events the server publishes; see
	// RecordEvents.
	PubSub pubsub.PubSub
	// Events are the typed events of /api/v2/events; see StreamEventsV2.
	Events *events.Hub
	// Durations are the typical runs of the summaries of /api/v2.
	Durations *summary.Durations
	// Explain explains the steps for the events and /api/v2; see GetV2.
	Explain *explain.Explainer
	// LogWindows gives windows of the steps' logs for /api/v2; see GetV2.
	LogWindows *explain.Windows
	// WorkflowEnd is the server's end of workflows, what a cancel takes.
	WorkflowEnd pipeline.WorkflowEnd

	stopStatuses context.CancelCauseFunc
	// stopQueue and stopGRPC stop the server's queue and its gRPC service,
	// see RestartServer.
	stopQueue context.CancelCauseFunc
	stopGRPC  func()
	ctx       context.Context
	// rpcStore is the store the gRPC service uses: Store, failing when told
	// (see FailWorkflowFinish).
	rpcStore *FaultyStore

	eventsV2Once sync.Once
	eventsV2Base string

	// Inbox keeps the webhooks the API takes and makes pipelines of them.
	Inbox *webhook.Inbox
	// Reconciler has the forge deliver again the webhooks that did not
	// reach the server; it runs a little after the server starts.
	Reconciler *webhook.Reconciler
	// Hooks are the deliveries of webhooks of the forge; see Deliver.
	Hooks *Deliveries
	// stopInbox stops the inbox and the reconciler and waits for them.
	stopInbox func()

	apiOnce    sync.Once
	apiHandler http.Handler
	apiMu      sync.Mutex
	apiBase    string
	stopAPI    func()
}

func (env *ServerEnv) DummyPipeline(event model.WebhookEvent) *model.Pipeline {
	return &model.Pipeline{
		Event:  event,
		Branch: "main",
		Commit: "deadbeef",
		Ref:    "refs/heads/main",
		Author: env.Fixtures.Owner.Login,
		Sender: env.Fixtures.Owner.Login,
	}
}

// StartServer wires up the full in-process server stack:
//   - in-memory sqlite store (fully migrated) with seeded fixtures
//   - in-memory queue, pubsub, and logging
//   - MockForge that serves the provided workflow files
//   - gRPC server on a random TCP port
//
// files must contain at least one entry. Single-workflow scenarios pass one
// file named ".woodpecker.yaml"; multi-workflow scenarios pass multiple files
// named ".woodpecker/foo.yaml" etc. The repo's Config path is set accordingly.
//
// All resources are cleaned up via t.Cleanup.
func StartServer(ctx context.Context, t *testing.T, files []*forge_types.FileMeta) *ServerEnv {
	t.Helper()
	configLock.Lock()
	defer configLock.Unlock()

	memStore := newStore(ctx, t)
	fixtures := seedFixtures(t, memStore)
	statuses := newStatuses()
	served := &Files{files: files}
	mockForge := newMockForge(t, served, statuses)
	stubHook(mockForge, fixtures.Repo)
	hooks := &Deliveries{requests: make(map[string]*hookRequest)}

	mgr, err := newTestManager(memStore, &hookForge{MockForge: mockForge, Deliveries: hooks})
	require.NoError(t, err, "create services manager")

	// as the server does (cmd/server): backed by the store
	queueCtx, stopQueue := context.WithCancelCause(ctx)
	t.Cleanup(func() { stopQueue(nil) })
	memQueue, err := queue.New(queueCtx, queue.Config{Backend: queue.TypeMemory, Store: memStore})
	require.NoError(t, err, "create queue")

	// Save and restore server.Config around the test. server.Config is a
	// package-level global read by server/pipeline and server/rpc. Tests run
	// sequentially within a package, but we still need to clean up so the next
	// subtest starts from a known-zero state rather than the previous test's values.
	orig := server.Config
	t.Cleanup(func() {
		configLock.Lock()
		defer configLock.Unlock()
		server.Config = orig
	})

	server.Config.Services.Logs = logging.New()
	ps := memory.New()
	explainer := explain.New(memStore, explain.Options{})
	hub := events.NewHub(events.Options{})
	// as the server does (cmd/server): failed steps explained in the
	// background
	worker.New(hub, explainer, memStore, worker.Options{}).Start(t.Context()) //nolint:contextcheck
	server.Config.Services.Events = hub
	durations := summary.NewDurations(0)
	server.Config.Services.Durations = durations
	server.Config.Services.Scheduler = newScheduler(t.Context(), memStore, memQueue, ps, hub, durations) //nolint:contextcheck
	server.Config.Services.Explain = explainer
	server.Config.Services.Membership = cache.NewMembershipService(memStore)
	server.Config.Services.Manager = mgr
	server.Config.Services.LogStore = explain.ForgetDeleted(memStore, explainer)
	server.Config.Services.LogWindows = explain.NewWindows(memStore)
	statusCtx, stopStatuses := context.WithCancelCause(ctx)
	server.Config.Services.Statuses = newStatusSender(statusCtx, memStore, mgr)
	server.Config.Services.Statuses.Resend(statusCtx)

	server.Config.Server.AgentToken = TestAgentToken
	server.Config.Server.Host = "http://localhost"
	server.Config.Server.JWTSecret = TestJWTSecret

	server.Config.Pipeline.DefaultClonePlugin = "docker.io/woodpeckerci/plugin-git:latest"
	server.Config.Pipeline.TrustedClonePlugins = []string{"docker.io/woodpeckerci/plugin-git:latest"}
	server.Config.Pipeline.DefaultApprovalMode = model.RequireApprovalNone
	server.Config.Pipeline.DefaultTimeout = 60
	server.Config.Pipeline.MaxTimeout = 60

	server.Config.Permissions.Open = true
	server.Config.Permissions.Admins = permissions.NewAdmins([]string{})
	server.Config.Permissions.Orgs = permissions.NewOrgs([]string{})
	server.Config.Permissions.OwnersAllowlist = permissions.NewOwnersAllowlist([]string{})

	// as the server does (cmd/server): one end for Done, settle, cancel and
	// the sweep, which runs more often
	end := pipeline.WorkflowEnd{
		Store:     memStore,
		Publisher: server.Config.Services.Scheduler,
		Logs:      server.Config.Services.Logs,
		Statuses:  server.Config.Services.Statuses,
	}
	go pipeline.RunCanceledWorkflowsSweep(t.Context(), end, SweepInterval) //nolint:contextcheck

	rpcStore := &FaultyStore{Store: memStore}
	grpcAddr, stopGRPC := startGRPCServer(ctx, t, rpcStore, end, "127.0.0.1:0")

	inbox := webhook.New(memStore, end, webhook.Options{Poll: inboxPoll})
	env := &ServerEnv{
		GRPCAddr:   grpcAddr,
		Store:      memStore,
		Queue:      memQueue,
		Fixtures:   fixtures,
		Forge:      mockForge,
		Files:      served,
		Statuses:   statuses,
		Manager:    mgr,
		PubSub:     ps,
		Events:     hub,
		Durations:  durations,
		Explain:    explainer,
		LogWindows: server.Config.Services.LogWindows,

		WorkflowEnd: end,

		stopStatuses: stopStatuses,
		stopQueue:    stopQueue,
		stopGRPC:     stopGRPC,
		ctx:          ctx,
		rpcStore:     rpcStore,

		Inbox: inbox,
		Reconciler: webhook.NewReconciler(memStore, webhook.ReconcilerOptions{
			FirstAfter: reconcileFirstAfter, Link: server.Config.Server.Host, Inbox: inbox,
		}),
		Hooks: hooks,
	}
	hooks.url = env.apiURL
	env.startInbox(ctx, t)
	return env
}

const (
	// How often the inbox looks for deliveries due without a new one.
	inboxPoll = 100 * time.Millisecond
	// The wait of the reconciler after the server starts.
	reconcileFirstAfter = 500 * time.Millisecond
)

// startInbox runs the inbox and the reconciler until StopServer.
func (env *ServerEnv) startInbox(ctx context.Context, t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { env.Inbox.Run(ctx) })
	wg.Go(func() { env.Reconciler.Run(ctx) })
	var once sync.Once
	env.stopInbox = func() {
		once.Do(func() {
			cancel(nil)
			wg.Wait()
		})
	}
	t.Cleanup(env.stopInbox)
}

func newScheduler(ctx context.Context, s store.Store, q queue.Queue, ps pubsub.PubSub, hub *events.Hub, durations *summary.Durations) scheduler.Scheduler {
	return scheduler.NewScheduler(ctx, s, q, ps, scheduler.WithPipelineObserver(hub), scheduler.WithPipelineObserver(durations))
}

// RestartServer restarts the server's queue and its gRPC service on the
// same store and address, as a deploy does: what the queue holds in memory
// is lost, it starts from the store; the agents connected reconnect.
func (env *ServerEnv) RestartServer(t *testing.T) {
	t.Helper()
	env.StopServer()
	env.StartServerAgain(t)
}

// StopServer stops the server's gRPC service and its queue, as a server
// that went away: its agents cannot reach it until StartServerAgain.
func (env *ServerEnv) StopServer() {
	env.apiMu.Lock()
	stopAPI := env.stopAPI
	env.apiMu.Unlock()
	if stopAPI != nil {
		stopAPI()
	}
	env.stopInbox()
	env.stopGRPC()
	env.stopQueue(nil)
}

// StartServerAgain starts the server StopServer stopped, on the same store
// and address: the queue starts from the store, the agents reconnect.
func (env *ServerEnv) StartServerAgain(t *testing.T) {
	t.Helper()
	configLock.Lock()
	queueCtx, stopQueue := context.WithCancelCause(env.ctx)
	t.Cleanup(func() { stopQueue(nil) })
	restarted, err := queue.New(queueCtx, queue.Config{Backend: queue.TypeMemory, Store: env.Store})
	require.NoError(t, err, "create queue")
	sched := newScheduler(t.Context(), env.Store, restarted, env.PubSub, env.Events, env.Durations)
	server.Config.Services.Scheduler = sched
	env.WorkflowEnd.Publisher = sched
	env.Queue = restarted
	env.stopQueue = stopQueue
	configLock.Unlock()

	_, env.stopGRPC = startGRPCServer(env.ctx, t, env.rpcStore, env.WorkflowEnd, env.GRPCAddr)
	env.startInbox(env.ctx, t)
	if addr := strings.TrimPrefix(env.apiURL(), "http://"); addr != "" {
		env.serveAPI(t, addr)
	}
}

// FailWorkflowFinish has the next n writes of a workflow's end its agent
// reports fail with err, as the database going away would.
func (env *ServerEnv) FailWorkflowFinish(n int, err error) {
	env.rpcStore.failWorkflowFinish(n, err)
}

// WorkflowFinishFailed tells how many writes of a workflow's end failed so
// far, as FailWorkflowFinish told.
func (env *ServerEnv) WorkflowFinishFailed() int {
	return env.rpcStore.workflowFinishFailed()
}

// StopStatuses stops the sender of commit statuses as a server that stops
// does: what it has not sent by then it does not send.
func (env *ServerEnv) StopStatuses() {
	configLock.Lock()
	defer configLock.Unlock()
	env.stopStatuses(nil)
	server.Config.Services.Statuses.Wait()
}

// StartStatuses starts a sender of commit statuses on the same store, as the
// server started again does.
func (env *ServerEnv) StartStatuses(ctx context.Context) {
	configLock.Lock()
	defer configLock.Unlock()
	statusCtx, stopStatuses := context.WithCancelCause(ctx)
	env.stopStatuses = stopStatuses
	server.Config.Services.Statuses = newStatusSender(statusCtx, env.Store, env.Manager)
	server.Config.Services.Statuses.Resend(statusCtx)
}

func newStatusSender(ctx context.Context, s store.Store, mgr services.Manager) *forgestatus.Sender {
	return forgestatus.New(ctx, s, mgr.ForgeFromRepo, func(ctx context.Context, f forge.Forge, u *model.User) {
		forge.Refresh(ctx, f, s, u)
	}, statusPolicy)
}

// newTestManager builds a services.Manager whose SetupForge always returns
// the provided forge, bypassing real forge instantiation.
func newTestManager(s store.Store, mockForge forge.Forge) (services.Manager, error) {
	cmd := &cli.Command{
		Flags: []cli.Flag{
			// Config fetch tuning.
			&cli.DurationFlag{Name: "forge-timeout", Value: defaultTimeout},
			&cli.UintFlag{Name: "forge-retry", Value: defaultRetry},
			&cli.StringSliceFlag{Name: "environment"},
			// services.NewManager reads the forge type from a cli flag and
			// drives a type-switch in setupForgeService. We set the gitea flag
			// to satisfy that switch; the actual forge instance is overridden
			// below via the SetupForge hook, so the switch result is unused.
			&cli.BoolFlag{Name: string(TestForgeType), Value: true},
			&cli.StringFlag{Name: "forge-url", Value: "https://forge.example.test"},
			&cli.StringSliceFlag{Name: "default-pipeline-configs", Value: constant.DefaultConfigOrder},
			&cli.StringSliceFlag{Name: "default-pipeline-config-extensions", Value: constant.DefaultConfigExtensions},
		},
	}

	setupForge := services.SetupForge(func(*model.Forge) (forge.Forge, error) {
		return mockForge, nil
	})

	return services.NewManager(cmd, s, setupForge)
}

// startGRPCServer binds to a random TCP port and serves Woodpecker's gRPC
// services via the shared server_rpc.Serve helper. A fresh prometheus.Registry
// is passed so subtests don't collide on metric names.
//
// Shutdown is synchronous: t.Cleanup cancels the serve context (triggering
// GracefulStop inside Serve) and then blocks until Serve has returned.
// Without this wait, the next subtest can start while the previous server's
// goroutines are still live, which races on shared state like server.Config.
func startGRPCServer(ctx context.Context, t *testing.T, s store.Store, end pipeline.WorkflowEnd, addr string) (string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err, "listen on %s for gRPC", addr)
	addr = lis.Addr().String()

	serveCtx, cancel := context.WithCancelCause(ctx)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = server_rpc.Serve(serveCtx, server_rpc.ServeConfig{
			Listener:         lis,
			Store:            s,
			Scheduler:        server.Config.Services.Scheduler,
			Logger:           server.Config.Services.Logs,
			JWTSecret:        TestJWTSecret,
			AgentToken:       TestAgentToken,
			KeepaliveMinTime: shortTimeout,
			WorkflowEnd:      end,
		})
	}()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel(nil)
			<-stopped
		})
	}
	t.Cleanup(stop)
	return addr, stop
}
