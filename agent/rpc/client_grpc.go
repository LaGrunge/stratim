// Copyright 2023 Woodpecker Authors
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
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v7"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	grpc_proto "google.golang.org/protobuf/proto"

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/rpc/proto"
)

var (
	ErrConnectionLost = errors.New("connection to server lost")
	errNotConnected   = errors.New("grpc: not connected")
)

const (
	// Set grpc version on compile time to compare against server version response.
	ClientGrpcVersion int32 = proto.Version

	// Maximum size of an outgoing log message.
	// Picked to prevent it from going over GRPC size limit (4 MiB) with a large safety margin.
	maxLogBatchSize int = 1 * 1024 * 1024

	// Maximum amount of time between sending consecutive batched log messages.
	// Controls the delay between the CI job generating a log record, and web users receiving it.
	maxLogFlushPeriod time.Duration = time.Second

	// The end of a step waits at least maxLogFlushWait for the log lines
	// queued before it to be sent, longer while the server is unreachable
	// (see flushWait).
	maxLogFlushWait = 30 * time.Second

	// DefaultConnectionRetryTimeout is how long an agent waits by default for
	// the server it lost: its workflows keep running meanwhile, as a deploy
	// or a database restart takes the server away for minutes.
	DefaultConnectionRetryTimeout = 30 * time.Minute
)

type client struct {
	client proto.WoodpeckerClient
	conn   *grpc.ClientConn
	logs   chan *proto.LogEntry
	// flushes asks processLogs to send what is queued and to close the
	// channel given once it did.
	flushes chan chan struct{}
	// connectionRetryTimeout is the maximum time to wait for a connection to be
	// restored before the agent gives up and exits. Zero means infinite.
	// Maps directly onto backoff.WithMaxElapsedTime.
	connectionRetryTimeout time.Duration
	// logEntryBufferSize sets the buffer size before the log reader is blocking.
	// If you have enough memory and log bursts, set it higher.
	logEntryBufferSize int
	// spool holds the log batches not sent yet; made by processLogs in
	// spoolDir, keeping at most spoolMaxBytes on disk.
	spool         *logSpool
	spoolDir      string
	spoolMaxBytes int64
}

// NewGrpcClient returns a new grpc Client.
func NewGrpcClient(ctx context.Context, conn *grpc.ClientConn, opts ...ClientOption) rpc.Peer {
	client := new(client)
	client.client = proto.NewWoodpeckerClient(conn)
	client.conn = conn
	client.logEntryBufferSize = 100
	client.connectionRetryTimeout = DefaultConnectionRetryTimeout
	client.spoolMaxBytes = DefaultLogSpoolMaxBytes

	for _, opt := range opts {
		opt(client)
	}

	client.logs = make(chan *proto.LogEntry, client.logEntryBufferSize) // max memory use: buffer count * 1 MiB
	client.flushes = make(chan chan struct{})

	go client.processLogs(ctx)
	return client
}

type ClientOption func(c *client)

func SetConnectionRetryTimeout(d time.Duration) ClientOption {
	if d == 0 {
		log.Warn().Msg("connection retry timeout set to infinite")
	}
	return func(c *client) {
		c.connectionRetryTimeout = d
	}
}

// SetLogSpool sets the directory the log batches not sent yet wait in, the
// system's temporary one if empty, and how many bytes of them it keeps.
func SetLogSpool(dir string, maxBytes int64) ClientOption {
	return func(c *client) {
		c.spoolDir = dir
		c.spoolMaxBytes = maxBytes
	}
}

func SetLogEntryBufferSize(count int) ClientOption {
	return func(c *client) {
		if count < 0 {
			log.Error().Msgf("LogEntry Buffer can not be negative")
			return
		}
		log.Info().Msgf("log-entry stream buffer size set to max %dMb", count)
		c.logEntryBufferSize = count
	}
}

// IsConnected reports whether the underlying gRPC connection is currently up.
// It is a pure observer with no side effects.
func (c *client) IsConnected() bool {
	state := c.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// retryOpts returns the backoff options used for every retry loop in this
// file. The exponential backoff parameters preserve the original tuning
// (10 ms initial, 10 s cap), and connectionRetryTimeout is wired straight into
// WithMaxElapsedTime — when it elapses, backoff.Retry returns the last error,
// which we translate into ErrConnectionLost in retryRPC.
func (c *client) retryOpts(op string) []backoff.RetryOption {
	b := backoff.NewExponentialBackOff()
	b.MaxInterval = 10 * time.Second          //nolint:mnd
	b.InitialInterval = 10 * time.Millisecond //nolint:mnd

	notify := func(err error, next time.Duration) {
		// The "too_many_pings" GOAWAY is well-known noise; demote to trace.
		// See https://github.com/woodpecker-ci/woodpecker/issues/717
		if strings.Contains(err.Error(), `"too_many_pings"`) {
			log.Trace().Err(err).Dur("retry_in", next).Msgf("grpc: %s(): too many keepalive pings without sending data", op)
			return
		}
		if errors.Is(err, errNotConnected) {
			log.Warn().Dur("retry_in", next).Msgf("grpc: %s() waiting for server connection...", op)
			return
		}
		log.Warn().Err(err).Dur("retry_in", next).Msgf("grpc error: %s(): code: %v", op, status.Code(err))
	}

	return []backoff.RetryOption{
		backoff.WithBackOff(b),
		backoff.WithMaxElapsedTime(c.connectionRetryTimeout),
		backoff.WithNotify(notify),
	}
}

// retryRPC is the workhorse used by every RPC method in this file. It runs op
// under backoff.Retry with the standard options, and translates the few
// special outcomes the callers care about:
//
//   - op succeeds          -> (result, nil)
//   - ctx canceled         -> (zero, nil)            same contract as before
//   - MaxElapsedTime hit   -> (zero, ErrConnectionLost)
//   - permanent (fatal)    -> (zero, underlying err)
//
// The op closure is responsible for:
//   - returning errNotConnected when IsConnected() is false (Retry will sleep
//     and call again — same effect as the old "if !c.IsConnected()" preamble)
//   - returning backoff.Permanent(err) for unrecoverable gRPC codes
//   - returning the raw error for retryable codes (Aborted/DataLoss/...)
func retryRPC[T any](ctx context.Context, c *client, opName string, op backoff.Operation[T]) (T, error) {
	res, err := backoff.Retry(ctx, op, c.retryOpts(opName)...)
	if err == nil {
		return res, nil
	}

	var zero T

	// Context canceled while inside Retry: callers historically swallowed this
	// and returned a zero-value error, so preserve that contract.
	if errors.Is(err, context.Canceled) {
		log.Debug().Err(err).Msgf("grpc: %s(): context canceled", opName)
		return zero, nil
	}

	re := backoff.AsRetryError(err)

	// MaxElapsedTime exhausted while we were still in errNotConnected — give up.
	if errors.Is(re.LastErr, errNotConnected) {
		log.Error().Msg("grpc: connection lost, giving up")
		return zero, ErrConnectionLost
	}

	log.Error().Err(err).Msgf("grpc error: %s(): code: %v", opName, status.Code(err))
	return zero, err
}

// classifyRPCErr inspects a gRPC error and returns either the same error (for
// retryable codes) or a backoff.Permanent wrapping it (for fatal codes). It is
// the single source of truth for which gRPC codes are worth retrying.
func classifyRPCErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.Canceled:
		// If our own ctx is dead, surface that as the cause so Retry's
		// context.Cause(ctx) check exits cleanly. Otherwise it's a server-side
		// cancel that we treat as permanent.
		if ctx.Err() != nil {
			return backoff.Permanent(ctx.Err())
		}
		return backoff.Permanent(err)
	case codes.Aborted,
		codes.DataLoss,
		codes.DeadlineExceeded,
		codes.Unauthenticated,
		codes.Unavailable:
		return err
	default:
		return backoff.Permanent(err)
	}
}

// Version returns the server- & grpc-version.
func (c *client) Version(ctx context.Context) (*rpc.Version, error) {
	res, err := c.client.Version(ctx, &proto.Empty{})
	if err != nil {
		return nil, err
	}
	return &rpc.Version{
		GrpcVersion:   res.GrpcVersion,
		ServerVersion: res.ServerVersion,
	}, nil
}

// Next returns the next workflow in the queue.
func (c *client) Next(ctx context.Context, filter rpc.Filter) (*rpc.Workflow, error) {
	req := &proto.NextRequest{Filter: &proto.Filter{Labels: filter.Labels}}

	res, err := retryRPC(ctx, c, "next", func() (*proto.NextResponse, error) {
		if !c.IsConnected() {
			return nil, errNotConnected
		}
		r, err := c.client.Next(ctx, req)
		return r, classifyRPCErr(ctx, err)
	})
	if err != nil {
		return nil, err
	}
	if res == nil || res.GetWorkflow() == nil {
		return nil, nil
	}

	w := &rpc.Workflow{
		ID:      res.GetWorkflow().GetId(),
		Timeout: res.GetWorkflow().GetTimeout(),
		Config:  new(backend_types.Config),
	}
	if err := json.Unmarshal(res.GetWorkflow().GetPayload(), w.Config); err != nil {
		log.Error().Err(err).Msgf("could not unmarshal workflow config of '%s'", w.ID)
	}
	return w, nil
}

// Wait blocks until the workflow with the given ID is marked as completed or canceled by the server.
func (c *client) Wait(ctx context.Context, workflowID string) (canceled bool, err error) {
	return c.wait(ctx, &proto.WaitRequest{Id: workflowID})
}

// WaitKilled blocks, for a workflow canceled, until its finalizers are to be
// stopped too.
func (c *client) WaitKilled(ctx context.Context, workflowID string) (killed bool, err error) {
	return c.wait(ctx, &proto.WaitRequest{Id: workflowID, AfterCancel: true})
}

func (c *client) wait(ctx context.Context, req *proto.WaitRequest) (bool, error) {
	resp, err := retryRPC(ctx, c, "wait", func() (*proto.WaitResponse, error) {
		if !c.IsConnected() {
			return nil, errNotConnected
		}
		r, err := c.client.Wait(ctx, req)
		return r, classifyRPCErr(ctx, err)
	})
	if err != nil {
		return false, err
	}
	if resp == nil {
		return false, nil
	}
	return resp.GetCanceled(), nil
}

// Init signals the workflow is initialized.
func (c *client) Init(ctx context.Context, workflowID string, state rpc.WorkflowState) error {
	req := &proto.InitRequest{
		Id: workflowID,
		State: &proto.WorkflowState{
			Started:  state.Started,
			Finished: state.Finished,
			Error:    state.Error,
			Canceled: state.Canceled,
		},
	}

	_, err := retryRPC(ctx, c, "init", func() (*proto.Empty, error) {
		if !c.IsConnected() {
			return nil, errNotConnected
		}
		r, err := c.client.Init(ctx, req)
		return r, classifyRPCErr(ctx, err)
	})
	return err
}

// Done let agent signal to server the workflow has stopped.
func (c *client) Done(ctx context.Context, workflowID string, state rpc.WorkflowState) error {
	req := &proto.DoneRequest{
		Id: workflowID,
		State: &proto.WorkflowState{
			Started:  state.Started,
			Finished: state.Finished,
			Error:    state.Error,
			Canceled: state.Canceled,
			TimedOut: state.TimedOut,
		},
	}

	_, err := retryRPC(ctx, c, "done", func() (*proto.Empty, error) {
		if !c.IsConnected() {
			return nil, errNotConnected
		}
		r, err := c.client.Done(ctx, req)
		return r, classifyRPCErr(ctx, err)
	})
	return err
}

// Extend extends the workflow deadline.
func (c *client) Extend(ctx context.Context, workflowID string) error {
	return c.extend(ctx, &proto.ExtendRequest{Id: workflowID})
}

// ExtendFinalizing extends the lease, telling when the finalizers' time ends,
// as how long it still lasts (see finalizeLeft).
func (c *client) ExtendFinalizing(ctx context.Context, workflowID string, finalizeDeadline time.Time) error {
	return c.extendWith(ctx, func() *proto.ExtendRequest {
		left := finalizeLeft(finalizeDeadline, time.Now())
		return &proto.ExtendRequest{Id: workflowID, FinalizeLeftMs: &left}
	})
}

// finalizeLeft is how long, in milliseconds, from now until deadline, both
// of the agent's clock: the server counts it from its own.
func finalizeLeft(deadline, now time.Time) int64 {
	return deadline.Sub(now).Milliseconds()
}

func (c *client) extend(ctx context.Context, req *proto.ExtendRequest) error {
	return c.extendWith(ctx, func() *proto.ExtendRequest { return req })
}

// extendWith extends the lease with the request newReq builds for each try.
func (c *client) extendWith(ctx context.Context, newReq func() *proto.ExtendRequest) error {
	_, err := retryRPC(ctx, c, "extend", func() (*proto.Empty, error) {
		if !c.IsConnected() {
			return nil, errNotConnected
		}
		r, err := c.client.Extend(ctx, newReq())
		return r, classifyRPCErr(ctx, err)
	})
	return err
}

// Update let agent updates the step state at the server.
func (c *client) Update(ctx context.Context, workflowID string, state rpc.StepState) error {
	if state.Exited {
		// the server reads the log of a step that ended, e.g. for why it
		// failed: it gets all of it first
		c.flushLogs(ctx)
	}
	req := &proto.UpdateRequest{
		Id: workflowID,
		State: &proto.StepState{
			StepUuid: state.StepUUID,
			Started:  state.Started,
			Finished: state.Finished,
			Exited:   state.Exited,
			ExitCode: int32(state.ExitCode),
			Error:    state.Error,
			Canceled: state.Canceled,
			Skipped:  state.Skipped,
			TimedOut: state.TimedOut,
		},
	}

	_, err := retryRPC(ctx, c, "update", func() (*proto.Empty, error) {
		if !c.IsConnected() {
			return nil, errNotConnected
		}
		r, err := c.client.Update(ctx, req)
		return r, classifyRPCErr(ctx, err)
	})
	return err
}

// EnqueueLog queues the log entry to be written in a batch later.
func (c *client) EnqueueLog(logEntry *rpc.LogEntry) {
	c.logs <- &proto.LogEntry{
		StepUuid: logEntry.StepUUID,
		Data:     logEntry.Data,
		Line:     int32(logEntry.Line),
		Time:     logEntry.Time,
		Type:     int32(logEntry.Type),
	}
}

// flushLogs sends the log entries queued so far, waiting at most
// maxLogFlushWait for processLogs to take the flush, and flushWait for them
// to be sent.
func (c *client) flushLogs(ctx context.Context) {
	if c.flushes == nil {
		return
	}
	done := make(chan struct{})
	taken, cancel := context.WithTimeout(ctx, maxLogFlushWait)
	defer cancel()
	select {
	case c.flushes <- done:
	case <-taken.Done():
		return
	}
	if wait := c.flushWait(); wait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, wait)
		defer cancel()
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// flushWait is how long a flush waits for the lines queued before it to be
// sent: as long as their sending tries, for while the server is away, the
// server takes the end of a step only once it has its log (lines coming
// after it may be refused); at least maxLogFlushWait. Zero for no limit.
func (c *client) flushWait() time.Duration {
	if c.connectionRetryTimeout == 0 {
		return 0
	}
	return max(maxLogFlushWait, c.connectionRetryTimeout)
}

// processLogs batches the log entries queued and hands the batches to the
// spool, which sendSpooled sends from: it never waits for the server, so
// the steps' output is read on while the server is away.
func (c *client) processLogs(ctx context.Context) {
	if c.spool == nil {
		c.spool = newLogSpool(c.spoolDir, c.spoolMaxBytes)
	}
	go c.sendSpooled(ctx, c.spool)

	var entries []*proto.LogEntry
	var bytes int

	send := func() {
		if len(entries) == 0 {
			return
		}

		log.Trace().
			Int("entries", len(entries)).
			Int("bytes", bytes).
			Msg("log drain: queueing logs to send")

		c.spool.push(entries, bytes)
		entries = nil
		bytes = 0
	}

	// Ensure periodic flush even during continuous log output
	ticker := time.NewTicker(maxLogFlushPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-c.logs:
			if !ok {
				log.Info().Msg("log drain: channel closed")
				send()
				return
			}

			entries = append(entries, entry)
			bytes += grpc_proto.Size(entry)

			if bytes >= maxLogBatchSize {
				send()
			}

		case <-ticker.C:
			send()

		case done := <-c.flushes:
			for queued := true; queued; {
				select {
				case entry, ok := <-c.logs:
					if !ok {
						queued = false
						break
					}
					entries = append(entries, entry)
					bytes += grpc_proto.Size(entry)
					if bytes >= maxLogBatchSize {
						send()
					}
				default:
					queued = false
				}
			}
			send()
			c.spool.pushFlush(done)
		}
	}
}

// sendSpooled sends the batches of spool in order, until ctx is done. A
// batch the server does not take is lost: lines in its steps' logs say so.
func (c *client) sendSpooled(ctx context.Context, spool *logSpool) {
	defer spool.close()
	for {
		item := spool.next(ctx)
		if item == nil {
			return
		}
		if item.flushed != nil {
			close(item.flushed)
			continue
		}
		err := c.sendLogs(ctx, item.entries)
		if err == nil || ctx.Err() != nil {
			continue
		}
		log.Error().Err(err).Int("entries", len(item.entries)).Msg("log drain: could not send logs to server")
		if !item.marks {
			spool.pushMarks(item.ranges, lostNotTaken)
		}
	}
}

func (c *client) sendLogs(ctx context.Context, entries []*proto.LogEntry) error {
	req := &proto.LogRequest{LogEntries: entries}

	// sendLogs intentionally does not gate on IsConnected — the original code
	// didn't either. backoff.Retry will keep trying through transient transport
	// errors until MaxElapsedTime elapses.
	_, err := retryRPC(ctx, c, "log", func() (*proto.Empty, error) {
		r, err := c.client.Log(ctx, req)
		return r, classifyRPCErr(ctx, err)
	})
	return err
}

func (c *client) RegisterAgent(ctx context.Context, info rpc.AgentInfo) (int64, error) {
	req := &proto.RegisterAgentRequest{
		Info: &proto.AgentInfo{
			Platform:     info.Platform,
			Backend:      info.Backend,
			Version:      info.Version,
			Capacity:     int32(info.Capacity),
			CustomLabels: info.CustomLabels,
		},
	}

	res, err := c.client.RegisterAgent(ctx, req)
	return res.GetAgentId(), err
}

func (c *client) UnregisterAgent(ctx context.Context) error {
	_, err := c.client.UnregisterAgent(ctx, &proto.Empty{})
	return err
}

func (c *client) ReportHealth(ctx context.Context) error {
	req := &proto.ReportHealthRequest{Status: "I am alive!"}

	_, err := retryRPC(ctx, c, "report_health", func() (*proto.Empty, error) {
		if !c.IsConnected() {
			return nil, errNotConnected
		}
		r, err := c.client.ReportHealth(ctx, req)
		return r, classifyRPCErr(ctx, err)
	})
	return err
}
