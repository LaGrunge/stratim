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

package rpc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	grpc_proto "google.golang.org/protobuf/proto"

	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/rpc/proto"
)

// flakyLogServer takes log batches as a server does, unless fail says it
// fails the call; it keeps the lines of each step as the server's log store
// does, dropping the ones up to the last it has.
type flakyLogServer struct {
	proto.WoodpeckerClient

	mu    sync.Mutex
	fail  func(call int, in *proto.LogRequest) error
	calls int
	lines map[string][]*proto.LogEntry
}

func (f *flakyLogServer) Log(_ context.Context, in *proto.LogRequest, _ ...grpc.CallOption) (*proto.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail != nil {
		if err := f.fail(f.calls, in); err != nil {
			return nil, err
		}
	}
	if f.lines == nil {
		f.lines = make(map[string][]*proto.LogEntry)
	}
	for _, e := range in.GetLogEntries() {
		got := f.lines[e.GetStepUuid()]
		if len(got) > 0 && e.GetLine() <= got[len(got)-1].GetLine() {
			continue
		}
		f.lines[e.GetStepUuid()] = append(got, e)
	}
	return &proto.Empty{}, nil
}

func (f *flakyLogServer) log(step string) []*proto.LogEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lines[step]
}

func numbers(entries []*proto.LogEntry) []int32 {
	out := make([]int32, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.GetLine())
	}
	return out
}

func lines(from, to int32) []int32 {
	out := make([]int32, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

func newSpoolingClient(t *testing.T, server proto.WoodpeckerClient) *client {
	t.Helper()
	return &client{
		client:                 server,
		logs:                   make(chan *proto.LogEntry, 10),
		flushes:                make(chan chan struct{}),
		connectionRetryTimeout: DefaultConnectionRetryTimeout,
		spoolDir:               t.TempDir(),
		spoolMaxBytes:          DefaultLogSpoolMaxBytes,
	}
}

// The server is away for three minutes while a step writes its log: the
// step's output is read on all along, and its whole log reaches the server
// once it is back, in order, before the step's end is told.
func TestLogsWaitForTheServer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)

		back := time.Now().Add(3 * time.Minute)
		server := &flakyLogServer{fail: func(int, *proto.LogRequest) error {
			if time.Now().Before(back) {
				return status.Error(codes.Unavailable, "connection refused")
			}
			return nil
		}}
		c := newSpoolingClient(t, server)
		go c.processLogs(ctx)

		const total = 500 // one line every 0.3 s: more than the channel holds
		for i := range total {
			start := time.Now()
			c.EnqueueLog(&rpc.LogEntry{StepUUID: "step", Line: i, Data: []byte("line")})
			assert.Zero(t, time.Since(start), "line %d waited for the server", i)
			time.Sleep(300 * time.Millisecond)
		}
		require.True(t, time.Now().Before(back), "the lines were written while the server was away")
		assert.Empty(t, server.log("step"))

		c.flushLogs(ctx) // as the step's end does
		assert.False(t, time.Now().Before(back), "the flush waited for the server")
		assert.Equal(t, lines(0, total-1), numbers(server.log("step")))

		cancel(nil)
		synctest.Wait()
	})
}

// Past what the spool keeps in memory, batches wait on disk; they are sent
// in order, and the files removed.
func TestLogSpoolSpillsToDisk(t *testing.T) {
	ctx := t.Context()
	s := newLogSpool(t.TempDir(), DefaultLogSpoolMaxBytes)
	s.memLimit = 1

	for b := range int32(4) {
		batch := []*proto.LogEntry{
			{StepUuid: "step", Line: 2 * b, Data: []byte("a")},
			{StepUuid: "step", Line: 2*b + 1, Data: []byte("b")},
		}
		s.push(batch, grpc_proto.Size(&proto.LogRequest{LogEntries: batch}))
	}
	require.NotEmpty(t, s.dir)
	files, err := os.ReadDir(s.dir)
	require.NoError(t, err)
	assert.Len(t, files, 3, "the first batch in memory, the others on disk")

	var got []int32
	for range 4 {
		item := s.next(ctx)
		require.NotNil(t, item)
		assert.False(t, item.marks)
		got = append(got, numbers(item.entries)...)
	}
	assert.Equal(t, lines(0, 7), got)
	files, err = os.ReadDir(s.dir)
	require.NoError(t, err)
	assert.Empty(t, files)
	assert.Zero(t, s.diskBytes)
	assert.Zero(t, s.memBytes)

	dir := s.dir
	s.close()
	assert.NoDirExists(t, dir)
}

// A batch the spool has no room for is dropped: a line in the step's log,
// in place of its last line, says how many are missing.
func TestLogSpoolFullMarksLostLines(t *testing.T) {
	s := newLogSpool(t.TempDir(), 1)
	s.memLimit = 1

	first := []*proto.LogEntry{{StepUuid: "step", Line: 0, Data: []byte("kept")}}
	s.push(first, 10)
	lost := []*proto.LogEntry{
		{StepUuid: "step", Line: 1, Time: 4, Data: []byte("x")},
		{StepUuid: "step", Line: 2, Time: 5, Data: []byte("y")},
		{StepUuid: "other", Line: 7, Time: 1, Data: []byte("z")},
	}
	s.push(lost, 1000)

	assert.Equal(t, first, s.next(t.Context()).entries)
	item := s.next(t.Context())
	require.True(t, item.marks)
	require.Len(t, item.entries, 2)
	assert.Equal(t, "step", item.entries[0].GetStepUuid())
	assert.Equal(t, int32(2), item.entries[0].GetLine(), "in place of the last line lost")
	assert.Equal(t, int64(5), item.entries[0].GetTime())
	assert.Equal(t, "[stratim] 2 log lines are missing here: "+lostSpoolFull, string(item.entries[0].GetData()))
	assert.Equal(t, int32(7), item.entries[1].GetLine())
	assert.Equal(t, "[stratim] 1 log lines are missing here: "+lostSpoolFull, string(item.entries[1].GetData()))
}

// A batch the server does not take: the line saying its lines are missing
// is sent next, before the lines after them, which the server would take
// it after no more.
func TestSendSpooledMarksBatchNotTaken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)

		server := &flakyLogServer{fail: func(call int, in *proto.LogRequest) error {
			if in.GetLogEntries()[0].GetLine() == 0 && !strings.HasPrefix(string(in.GetLogEntries()[0].GetData()), "[stratim]") {
				return status.Error(codes.InvalidArgument, "refused")
			}
			return nil
		}}
		c := newSpoolingClient(t, server)
		c.spool = newLogSpool(c.spoolDir, c.spoolMaxBytes)
		c.spool.push([]*proto.LogEntry{
			{StepUuid: "step", Line: 0, Data: []byte("a")},
			{StepUuid: "step", Line: 1, Data: []byte("b")},
		}, 10)
		c.spool.push([]*proto.LogEntry{{StepUuid: "step", Line: 2, Data: []byte("c")}}, 10)
		go c.sendSpooled(ctx, c.spool)
		synctest.Wait()

		got := server.log("step")
		assert.Equal(t, []int32{1, 2}, numbers(got))
		assert.Equal(t, "[stratim] 2 log lines are missing here: "+lostNotTaken, string(got[0].GetData()))
		assert.Equal(t, "c", string(got[1].GetData()))

		cancel(nil)
		synctest.Wait()
	})
}

// The line saying lines are missing is not replaced in turn when the server
// does not take it either: the sender goes on.
func TestSendSpooledDropsMarksNotTaken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)

		server := &flakyLogServer{fail: func(_ int, in *proto.LogRequest) error {
			if in.GetLogEntries()[0].GetStepUuid() == "ended" {
				return status.Error(codes.Unknown, "step not running")
			}
			return nil
		}}
		c := newSpoolingClient(t, server)
		c.spool = newLogSpool(c.spoolDir, c.spoolMaxBytes)
		c.spool.push([]*proto.LogEntry{{StepUuid: "ended", Line: 3, Data: []byte("late")}}, 10)
		c.spool.push([]*proto.LogEntry{{StepUuid: "step", Line: 0, Data: []byte("next")}}, 10)
		go c.sendSpooled(ctx, c.spool)
		synctest.Wait()

		assert.Equal(t, 3, server.calls, "the batch, its marks, the next batch")
		assert.Empty(t, server.log("ended"))
		assert.Equal(t, []int32{0}, numbers(server.log("step")))

		cancel(nil)
		synctest.Wait()
	})
}

func TestFlushWait(t *testing.T) {
	for _, tc := range []struct {
		retry, want time.Duration
	}{
		{0, 0},
		{10 * time.Second, maxLogFlushWait},
		{DefaultConnectionRetryTimeout, DefaultConnectionRetryTimeout},
	} {
		t.Run(fmt.Sprint(tc.retry), func(t *testing.T) {
			c := &client{connectionRetryTimeout: tc.retry}
			assert.Equal(t, tc.want, c.flushWait())
		})
	}
}

// An agent waits half an hour for the server it lost by default: a deploy
// or a database restart takes the server away for minutes.
func TestDefaultConnectionRetryTimeout(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	conn := newTestConn(t)
	defer conn.Close()

	c, ok := NewGrpcClient(ctx, conn).(*client)
	require.True(t, ok)
	assert.GreaterOrEqual(t, c.connectionRetryTimeout, 30*time.Minute)
	assert.Equal(t, DefaultLogSpoolMaxBytes, c.spoolMaxBytes)
	assert.LessOrEqual(t, reconnectParams.Backoff.MaxDelay, 10*time.Second, "an agent reconnects soon once the server is back")
}

func batchOf(lines ...int32) ([]*proto.LogEntry, int) {
	entries := make([]*proto.LogEntry, 0, len(lines))
	for _, l := range lines {
		entries = append(entries, &proto.LogEntry{StepUuid: "step", Line: l, Data: []byte("x")})
	}
	return entries, grpc_proto.Size(&proto.LogRequest{LogEntries: entries})
}

// A batch that fills what the spool keeps exactly is kept; one byte more
// is not.
func TestLogSpoolLimits(t *testing.T) {
	first, firstSize := batchOf(0)
	second, size := batchOf(1)
	third, _ := batchOf(2)

	t.Run("memory", func(t *testing.T) {
		s := newLogSpool(t.TempDir(), DefaultLogSpoolMaxBytes)
		s.memLimit = firstSize + size
		s.push(first, firstSize)
		s.push(second, size)
		assert.Empty(t, s.dir, "both fit in memory")
		s.push(third, size)
		assert.NotEmpty(t, s.dir, "one byte over: to disk")
	})

	t.Run("disk", func(t *testing.T) {
		s := newLogSpool(t.TempDir(), int64(size))
		s.memLimit = 1
		s.push(first, firstSize) // the first is in memory, always
		s.push(second, size)
		s.push(third, size)
		require.Len(t, s.items, 3)
		assert.False(t, s.items[1].marks, "fills the disk exactly: kept")
		assert.NotEmpty(t, s.items[1].path)
		assert.True(t, s.items[2].marks, "no room left: lost, said so")
	})
}

// A batch that cannot be read back from disk is lost: a line says so.
func TestLogSpoolUnreadable(t *testing.T) {
	s := newLogSpool(t.TempDir(), DefaultLogSpoolMaxBytes)
	s.memLimit = 1
	first, size := batchOf(0)
	s.push(first, size)
	spilled, size := batchOf(1, 2, 3)
	s.push(spilled, size)
	require.NotEmpty(t, s.items[1].path)
	require.NoError(t, os.WriteFile(s.items[1].path, []byte{0xff}, 0o600))

	s.next(t.Context())
	item := s.next(t.Context())
	require.True(t, item.marks)
	require.Len(t, item.entries, 1)
	assert.Equal(t, int32(3), item.entries[0].GetLine())
	assert.Equal(t, "[stratim] 3 log lines are missing here: "+lostUnread, string(item.entries[0].GetData()))
	assert.Zero(t, s.diskBytes)
	files, err := os.ReadDir(s.dir)
	require.NoError(t, err)
	assert.Empty(t, files, "the unreadable file removed")
}

// A batch pushed once the spool is closed, as the agent stops, is dropped:
// no directory is made for it.
func TestLogSpoolClosed(t *testing.T) {
	s := newLogSpool(t.TempDir(), DefaultLogSpoolMaxBytes)
	s.memLimit = 1
	s.close()
	first, size := batchOf(0)
	s.push(first, size)
	s.push(first, size)
	assert.Empty(t, s.items)
	assert.Empty(t, s.dir)
}

// An agent starting removes the spools agents left long ago, not the ones
// in use.
func TestRemoveStaleLogSpools(t *testing.T) {
	base := t.TempDir()
	stale := filepath.Join(base, logSpoolPrefix+"old")
	fresh := filepath.Join(base, logSpoolPrefix+"new")
	other := filepath.Join(base, "other")
	for _, dir := range []string{stale, fresh, other} {
		require.NoError(t, os.Mkdir(dir, 0o700))
	}
	now := time.Now()
	old := now.Add(-staleLogSpoolAge - time.Minute)
	require.NoError(t, os.Chtimes(stale, old, old))
	require.NoError(t, os.Chtimes(other, old, old))

	removeStaleLogSpools(base, now)
	assert.NoDirExists(t, stale)
	assert.DirExists(t, fresh)
	assert.DirExists(t, other, "not a spool")
}

func TestCheckLogSpoolDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, CheckLogSpoolDir(dir))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "the probe removed")
	assert.Error(t, CheckLogSpoolDir(filepath.Join(dir, "missing")))
}
