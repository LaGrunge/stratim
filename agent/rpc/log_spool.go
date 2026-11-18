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
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	grpc_proto "google.golang.org/protobuf/proto"

	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/rpc/proto"
)

const (
	// DefaultLogSpoolMaxBytes is how many bytes of log batches not sent yet
	// an agent keeps on disk by default.
	DefaultLogSpoolMaxBytes int64 = 1 << 30

	// The bytes of log batches not sent yet kept in memory before the next
	// ones go to disk.
	logSpoolMemBytes = 8 * maxLogBatchSize

	// The start of the names of the spools' directories.
	logSpoolPrefix = "stratim-agent-logs-"

	// How long a spool's directory is left untouched before an agent
	// starting takes it for one an agent stopped without removing (killed,
	// its machine rebooted): longer than an agent waits for the server,
	// every batch spilled touching it.
	staleLogSpoolAge = 2 * DefaultConnectionRetryTimeout
)

// Why lines are missing from a log, said in the line put in their place.
const (
	lostSpoolFull = "the agent's log buffer was full"
	lostNotTaken  = "the server did not take them"
	lostUnread    = "the agent could not read them back from disk"
)

// lineRange is the lines of one step a batch carries, first to last.
type lineRange struct {
	step        string
	first, last int32
	count       int
	time        int64
}

// spoolItem is one batch of log entries to send, or a flush: a mark the
// sender closes flushed at, once everything before it was sent.
type spoolItem struct {
	entries []*proto.LogEntry // nil while on disk
	path    string            // where the batch waits, if on disk
	size    int
	ranges  []lineRange
	// marks tells the batch is the lines put in place of lost ones: they are
	// not replaced in turn.
	marks   bool
	flushed chan struct{}
}

// logSpool holds the log batches not sent yet, in order: the first ones in
// memory, the next ones on disk, as long as they fit; a batch that does not
// is dropped, a line in each of its steps' logs saying so (see lostMarks).
type logSpool struct {
	mu        sync.Mutex
	items     []*spoolItem
	memBytes  int
	diskBytes int64
	memLimit  int
	diskLimit int64
	baseDir   string
	dir       string // made at the first batch to disk
	seq       int
	ready     chan struct{}
	// closed is set once the spool is closed: batches pushed after are
	// dropped, no directory made for them.
	closed bool
}

func newLogSpool(baseDir string, diskLimit int64) *logSpool {
	if baseDir == "" {
		baseDir = os.TempDir()
	}
	removeStaleLogSpools(baseDir, time.Now())
	return &logSpool{
		memLimit:  logSpoolMemBytes,
		diskLimit: diskLimit,
		baseDir:   baseDir,
		ready:     make(chan struct{}, 1),
	}
}

// push queues entries, a batch of size bytes, after the others.
func (s *logSpool) push(entries []*proto.LogEntry, size int) {
	item := &spoolItem{entries: entries, size: size, ranges: rangesOf(entries)}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if s.memBytes+size > s.memLimit && len(s.items) > 0 {
		if err := s.toDisk(item); err != nil {
			log.Error().Err(err).Int("entries", len(entries)).Msg("log spool: dropping log lines")
			item = marksItem(item.ranges, lostSpoolFull)
		}
	}
	s.add(item, false)
}

// pushMarks queues, first, the lines put in place of the lines of ranges,
// which were lost for why.
func (s *logSpool) pushMarks(ranges []lineRange, why string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.add(marksItem(ranges, why), true)
}

// pushFlush queues a flush, closing done once all before it was sent.
func (s *logSpool) pushFlush(done chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.add(&spoolItem{flushed: done}, false)
}

func (s *logSpool) add(item *spoolItem, first bool) {
	if item.entries != nil {
		s.memBytes += item.size
	}
	if first {
		s.items = append([]*spoolItem{item}, s.items...)
	} else {
		s.items = append(s.items, item)
	}
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// toDisk writes item's entries to a file of the spool's directory, if they
// fit in what the spool may keep on disk.
func (s *logSpool) toDisk(item *spoolItem) error {
	if s.diskBytes+int64(item.size) > s.diskLimit {
		return fmt.Errorf("%d bytes on disk already, at most %d", s.diskBytes, s.diskLimit)
	}
	if s.dir == "" {
		dir, err := os.MkdirTemp(s.baseDir, logSpoolPrefix)
		if err != nil {
			return err
		}
		s.dir = dir
	}
	data, err := grpc_proto.Marshal(&proto.LogRequest{LogEntries: item.entries})
	if err != nil {
		return err
	}
	s.seq++
	path := filepath.Join(s.dir, fmt.Sprintf("%08d.pb", s.seq))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		_ = os.Remove(path)
		return err
	}
	item.entries = nil
	item.path = path
	item.size = len(data)
	s.diskBytes += int64(len(data))
	return nil
}

// next takes the first item, waiting for one until ctx is done; a batch
// waiting on disk is read back. Nil once ctx is done.
func (s *logSpool) next(ctx context.Context) *spoolItem {
	for {
		s.mu.Lock()
		if len(s.items) > 0 {
			item := s.items[0]
			s.items[0] = nil
			s.items = s.items[1:]
			if item.path == "" {
				s.memBytes -= item.size
			} else {
				s.diskBytes -= int64(item.size)
			}
			s.mu.Unlock()
			if item.path != "" {
				item = readBack(item)
			}
			return item
		}
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil
		case <-s.ready:
		}
	}
}

// readBack reads the entries of item from its file, removing it; ones it
// cannot read are told lost.
func readBack(item *spoolItem) *spoolItem {
	defer os.Remove(item.path)
	data, err := os.ReadFile(item.path)
	if err == nil {
		req := new(proto.LogRequest)
		if err = grpc_proto.Unmarshal(data, req); err == nil {
			item.entries = req.GetLogEntries()
			item.path = ""
			return item
		}
	}
	log.Error().Err(err).Str("path", item.path).Msg("log spool: cannot read log lines back")
	return marksItem(item.ranges, lostUnread)
}

// close removes the spool's directory, with the batches waiting in it.
func (s *logSpool) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.dir != "" {
		_ = os.RemoveAll(s.dir)
		s.dir = ""
	}
}

// removeStaleLogSpools removes the spools' directories in baseDir untouched
// for staleLogSpoolAge before now: an agent stopped without removing its
// own; another agent's, in use, is touched by each batch it spills.
func removeStaleLogSpools(baseDir string, now time.Time) {
	stale, err := filepath.Glob(filepath.Join(baseDir, logSpoolPrefix+"*"))
	if err != nil {
		return
	}
	for _, dir := range stale {
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() || now.Sub(info.ModTime()) < staleLogSpoolAge {
			continue
		}
		if err := os.RemoveAll(dir); err != nil {
			log.Warn().Err(err).Str("dir", dir).Msg("log spool: cannot remove a stale spool")
			continue
		}
		log.Info().Str("dir", dir).Msg("log spool: removed the spool an agent left")
	}
}

// CheckLogSpoolDir checks that the logs not sent yet can wait in dir, the
// system's temporary directory if empty: an agent that cannot tells at
// start, not once the server is away.
func CheckLogSpoolDir(dir string) error {
	if dir == "" {
		dir = os.TempDir()
	}
	probe, err := os.MkdirTemp(dir, logSpoolPrefix)
	if err != nil {
		return fmt.Errorf("the log spool directory %s cannot be written: %w", dir, err)
	}
	return os.RemoveAll(probe)
}

// rangesOf tells, for each step entries are of, which of its lines they
// carry.
func rangesOf(entries []*proto.LogEntry) []lineRange {
	var ranges []lineRange
	index := make(map[string]int)
	for _, e := range entries {
		i, ok := index[e.GetStepUuid()]
		if !ok {
			index[e.GetStepUuid()] = len(ranges)
			ranges = append(ranges, lineRange{step: e.GetStepUuid(), first: e.GetLine(), last: e.GetLine(), count: 1, time: e.GetTime()})
			continue
		}
		r := &ranges[i]
		r.first = min(r.first, e.GetLine())
		r.last = max(r.last, e.GetLine())
		r.time = max(r.time, e.GetTime())
		r.count++
	}
	return ranges
}

// marksItem is the lines put in place of the lines of ranges, lost for why:
// one per step, as its last lost line, the first number the server still
// takes (it drops lines up to the last it has).
func marksItem(ranges []lineRange, why string) *spoolItem {
	entries := make([]*proto.LogEntry, 0, len(ranges))
	size := 0
	for _, r := range ranges {
		e := &proto.LogEntry{
			StepUuid: r.step,
			Line:     r.last,
			Time:     r.time,
			Type:     int32(rpc.LogEntryStdout),
			Data:     fmt.Appendf(nil, "[stratim] %d log lines are missing here: %s", r.count, why),
		}
		entries = append(entries, e)
		size += grpc_proto.Size(e)
	}
	return &spoolItem{entries: entries, size: size, ranges: ranges, marks: true}
}
