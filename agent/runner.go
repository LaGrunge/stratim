// Copyright 2022 Woodpecker Authors
// Copyright 2018 Drone.IO Inc.
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

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/metadata"

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	pipeline_runtime "go.woodpecker-ci.org/woodpecker/v3/pipeline/runtime"
	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/shared/constant"
	"go.woodpecker-ci.org/woodpecker/v3/shared/utils"
)

const (
	shutdownTimeout = time.Second * 5
	// Leases are extended extendsPerLease times in the time one lasts.
	extendsPerLease = 3
	// The lease of a workflow whose finalizers ran is extended for
	// finalizeLeaseGrace past the end of their time (the runtime's grace
	// included): the workflow's services are torn down after them.
	finalizeLeaseGrace = time.Minute
)

type Runner struct {
	client   rpc.Peer
	filter   rpc.Filter
	hostname string
	agentID  int64
	counter  *State
	backend  backend_types.Backend
	// timeoutUnit is what one unit of a workflow timeout lasts; zero means a
	// minute, as the server means it.
	timeoutUnit time.Duration
	// finalizeLeaseGrace replaces the const of the name when set; only tests
	// set it (see WithFinalizeLeaseGrace).
	finalizeLeaseGrace time.Duration
}

func NewRunner(workEngine rpc.Peer, f rpc.Filter, h string, agentID int64, state *State, backend backend_types.Backend) Runner {
	return Runner{
		client:   workEngine,
		filter:   f,
		hostname: h,
		agentID:  agentID,
		counter:  state,
		backend:  backend,
	}
}

// workflowTimeout is how long a workflow may run: the server sends the
// timeout in minutes, zero meaning none set, which gets an hour. A non-zero
// unit replaces the minute; only tests set one (see WithTimeoutUnit).
func workflowTimeout(timeout int64, unit time.Duration) time.Duration {
	if timeout == 0 {
		return time.Hour
	}
	if unit == 0 {
		unit = time.Minute
	}
	return time.Duration(timeout) * unit
}

// formatAgentLabels renders the agent's labels as a JSON object so they can be
// exposed to steps through a single environment variable. JSON keeps the format
// consistent with other structured pipeline data such as CI_PIPELINE_FILES.
// Map keys are sorted by json.Marshal, so the output is deterministic across runs.
func formatAgentLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	out, err := json.Marshal(labels)
	if err != nil {
		return ""
	}
	return string(out)
}

func GetShutdownContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), shutdownTimeout)
}

// TODO: refactor this big function into subfunctions in it's own subpackage

// Run executes a workflow using a backend, tracks its state and reports the state back to the server.
func (r *Runner) Run(runnerCtx context.Context) error {
	log.Debug().Msg("request next execution")

	// Preserve metadata AND cancellation from runnerCtx.
	meta, _ := metadata.FromOutgoingContext(runnerCtx)
	ctxMeta := metadata.NewOutgoingContext(runnerCtx, meta)

	// Fetch next workflow from the queue
	workflow, err := r.client.Next(runnerCtx, r.filter)
	if err != nil {
		return err
	}
	if workflow == nil {
		return nil
	}

	// Compute workflow timeout
	timeout := workflowTimeout(workflow.Timeout, r.timeoutUnit)

	repoName := extractRepositoryName(workflow.Config)       // hack
	pipelineNumber := extractPipelineNumber(workflow.Config) // hack

	// Track workflow execution in runner state
	r.counter.Add(workflow.ID, timeout, repoName, pipelineNumber)
	defer r.counter.Done(workflow.ID)

	logger := log.With().
		Str("repo", repoName).
		Str("pipeline", pipelineNumber).
		Str("workflow_id", workflow.ID).
		Logger()

	logger.Debug().Msg("received execution")

	// Workflow execution context.
	// This context is the SINGLE source of truth for cancellation.
	workflowCtx, _ := context.WithTimeout(ctxMeta, timeout) //nolint:govet
	workflowCtx, cancelWorkflowCtx := context.WithCancelCause(workflowCtx)
	defer cancelWorkflowCtx(nil)

	// Add sigterm support for internal context.
	// Required to be able to terminate the running workflow by external signals.
	workflowCtx = utils.WithContextSigtermCallback(workflowCtx, func() {
		logger.Error().Msg("received sigterm termination signal")
		// WithContextSigtermCallback would cancel the context too, but  we want our own custom error
		cancelWorkflowCtx(pipeline_errors.ErrCancel)
	})

	// Listen for remote cancel events (UI / API).
	// When canceled, we MUST cancel the workflow context
	// so that workflow execution stops immediately.
	// It listens until the run ended, not the workflow's context: a cancel
	// also stops the finalizers running after that ended, through
	// cancelSignal.
	runCtx, stopRun := context.WithCancelCause(ctxMeta)
	defer stopRun(nil)
	cancelSignal, signalCancel := context.WithCancelCause(context.Background())
	defer signalCancel(nil)
	// A second cancel stops the finalizers too, whenever it comes.
	killSignal, signalKill := context.WithCancelCause(context.Background())
	defer signalKill(nil)
	go func() {
		logger.Debug().Msg("start listening for server side cancel signal")

		canceled, err := r.client.Wait(runCtx, workflow.ID)
		switch {
		case runCtx.Err() != nil:
			logger.Debug().Msg("cancel listener exited with the run")
		case err != nil:
			logger.Error().Err(err).Msg("server returned unexpected err while waiting for workflow to finish run")
			cancelWorkflowCtx(err)
		case canceled:
			logger.Debug().Msg("server side cancel signal received")
			// the signal first: the runtime, seeing the workflow's context
			// canceled, must find it given already, not stop the finalizers
			// it starts on it
			signalCancel(pipeline_errors.ErrCancel)
			cancelWorkflowCtx(pipeline_errors.ErrCancel)

			// then wait for the finalizers to be stopped too: a second
			// cancel, or the server giving up on the workflow
			if killed, err := r.client.WaitKilled(runCtx, workflow.ID); err == nil && killed && runCtx.Err() == nil {
				logger.Debug().Msg("server side signal to stop the finalizers received")
				signalKill(pipeline_errors.ErrCancel)
			}
		default:
			// Wait returned without error, meaning the workflow finished normally
			logger.Debug().Msg("cancel listener exited normally")
		}
	}()

	run := pipeline_runtime.New(
		workflow.Config,
		r.backend,
		pipeline_runtime.WithContext(workflowCtx),
		pipeline_runtime.WithCancelSignal(cancelSignal),
		pipeline_runtime.WithKillSignal(killSignal),
		pipeline_runtime.WithTaskUUID(fmt.Sprint(workflow.ID)),
		pipeline_runtime.WithLogger(r.createLogger(logger, workflow)),
		pipeline_runtime.WithTracer(r.createTracer(ctxMeta, workflowCtx, logger, workflow)),
		pipeline_runtime.WithDescription(map[string]string{
			"workflow_id":     workflow.ID,
			"repo":            repoName,
			"pipeline_number": pipelineNumber,
		}),
	)

	// Periodically extend the workflow lease while running: until the run
	// ended, not the workflow's context, as finalizers run after that ended,
	// but no longer than a grace past the end of their time, for the
	// teardown of the workflow (its services) after them
	leaseGrace := r.finalizeLeaseGrace
	if leaseGrace == 0 {
		leaseGrace = finalizeLeaseGrace
	}
	extendEvery := constant.TaskTimeout / extendsPerLease
	go func() {
		for {
			select {
			case <-runCtx.Done():
				logger.Debug().Msg("workflow run done")
				return

			case <-time.After(extendEvery):
				finalizeDeadline := run.FinalizeDeadline()
				// only a canceled workflow's lease ends with its finalizers'
				// time: one not canceled is held until its run ended, long
				// teardown included, or the server would run it again
				if !leaseExtendable(cancelSignal.Err() != nil, finalizeDeadline, time.Now(), leaseGrace) {
					logger.Warn().Msg("canceled workflow's finalizers' time is up: its lease is no longer extended")
					return
				}
				logger.Debug().Msg("renewing workflow lease")
				extend := func() error { return r.client.Extend(runCtx, workflow.ID) }
				if !finalizeDeadline.IsZero() {
					// the server caps a canceled workflow's lease by it
					extend = func() error { return r.client.ExtendFinalizing(runCtx, workflow.ID, finalizeDeadline) }
				}
				if err := extend(); err != nil {
					// a canceled workflow left the queue: nothing to extend
					if workflowCtx.Err() != nil {
						logger.Debug().Err(err).Msg("failed to extend the lease of a stopped workflow")
					} else {
						logger.Error().Err(err).Msg("failed to extend workflow lease")
					}
				}
			}
		}
	}()

	state := rpc.WorkflowState{
		Started: time.Now().Unix(),
	}

	if err := r.client.Init(runnerCtx, workflow.ID, state); err != nil {
		logger.Error().Err(err).Msg("signaling workflow initialization to server failed")
		// We have an error, maybe the server is currently unreachable or other server-side errors occurred.
		// So let's clean up and end this not yet started workflow run.
		cancelWorkflowCtx(err)
		return err
	}

	// Enrich workflow env with agent info
	// TODO: find better way to track this state
	for _, stage := range workflow.Config.Stages {
		for _, step := range stage.Steps {
			step.Environment["CI_MACHINE"] = r.hostname
			step.Environment["CI_SYSTEM_PLATFORM"] = runtime.GOOS + "/" + runtime.GOARCH
			step.Environment["CI_AGENT_ID"] = strconv.FormatInt(r.agentID, 10)
			step.Environment["CI_AGENT_LABELS"] = formatAgentLabels(r.filter.Labels)
		}
	}

	// Run pipeline
	err = run.Run(runnerCtx)
	stopRun(nil)

	state.Finished = time.Now().Unix()

	if err != nil {
		if errors.Is(err, pipeline_errors.ErrCancel) {
			// A canceled workflow did not fail. The server stores Error as the
			// workflow error and shows it as a runtime error, so only report
			// the cancellation itself, and whether the timeout caused it.
			state.Canceled = true
			state.TimedOut = workflowTimedOut(workflowCtx)
		} else {
			state.Error = err.Error()
		}
	}

	logger.Debug().
		Str("error", state.Error).
		Bool("canceled", state.Canceled).
		Bool("timed_out", state.TimedOut).
		Msg("workflow finished")

	// Update workflow state
	doneCtx := runnerCtx //nolint:contextcheck
	if doneCtx.Err() != nil {
		shutdownCtx, shutdownCtxCancel := GetShutdownContext()
		defer shutdownCtxCancel()
		// keep the gRPC metadata
		doneCtx = metadata.NewOutgoingContext(shutdownCtx, meta)
	}

	if err := r.client.Done(doneCtx, workflow.ID, state); err != nil {
		logger.Error().Err(err).Msg("failed to update workflow status")
	} else {
		logger.Debug().Msg("signaling workflow stopped done")
	}

	return nil
}

// leaseExtendable reports whether a workflow's lease is still extended at
// now: always for a workflow not canceled, held until its run ended, long
// teardown included, or the server would run it again elsewhere; for a
// canceled one until grace past the end of its finalizers' time,
// finalizeDeadline, always while they did not start (it is zero then).
func leaseExtendable(canceled bool, finalizeDeadline, now time.Time, grace time.Duration) bool {
	return !canceled || finalizeDeadline.IsZero() || !now.After(finalizeDeadline.Add(grace))
}

func extractRepositoryName(config *backend_types.Config) string {
	return config.Stages[0].Steps[0].Environment["CI_REPO"]
}

func extractPipelineNumber(config *backend_types.Config) string {
	return config.Stages[0].Steps[0].Environment["CI_PIPELINE_NUMBER"]
}
