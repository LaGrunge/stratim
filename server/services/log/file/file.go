// Copyright 2024 Woodpecker Authors
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

package file

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	service_log "go.woodpecker-ci.org/woodpecker/v3/server/services/log"
)

const (
	// Add base64 overhead and space for other JSON fields (just to be safe).
	maxLineLength int = (pipeline.MaxLogLineLength/3)*4 + (64 * 1024) //nolint:mnd
)

type logStore struct {
	base string
	// appends orders the appends to a step's log, by its id.
	appends *[appendLocks]sync.Mutex
}

// appendLocks are the locks the appends of all steps share.
const appendLocks = 64

func NewLogStore(base string) (service_log.Service, error) {
	if base == "" {
		return nil, fmt.Errorf("file storage base path is required")
	}
	if _, err := os.Stat(base); err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(base, 0o700)
		if err != nil {
			return nil, err
		}
	}
	return logStore{base: base, appends: new([appendLocks]sync.Mutex)}, nil
}

func (l logStore) filePath(id int64) string {
	return filepath.Join(l.base, fmt.Sprintf("%d.json", id))
}

func (l logStore) LogFind(ctx context.Context, step *model.Step) ([]*model.LogEntry, error) {
	var entries []*model.LogEntry
	err := l.LogStream(ctx, step, func(entry *model.LogEntry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

// LogStream decodes the step's log file line by line, one entry in memory at a
// time. It reads the file as long as it was when opened, and a last record
// without its newline is still being written by LogAppend, so it is left out.
func (l logStore) LogStream(ctx context.Context, step *model.Step, fn func(*model.LogEntry) error) error {
	return l.LogStreamFrom(ctx, step, 0, fn)
}

// LogStreamFrom is LogStream from the entry with the line from on. The
// records follow the lines, so it finds the first one by a binary search over
// the file and reads only from there.
func (l logStore) LogStreamFrom(ctx context.Context, step *model.Step, from int, fn func(*model.LogEntry) error) error {
	file, size, err := l.open(step)
	if file == nil || err != nil {
		return err
	}
	defer file.Close()

	offset := int64(0)
	if from > 0 {
		if offset, err = firstRecordOf(file, size, from); err != nil {
			return err
		}
	}

	buf := make([]byte, 0, bufio.MaxScanTokenSize)
	s := bufio.NewScanner(io.NewSectionReader(file, offset, size-offset))
	s.Buffer(buf, maxLineLength)
	s.Split(scanWrittenLines)

	for s.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		j := s.Bytes()
		if len(bytes.TrimSpace(j)) == 0 {
			continue
		}
		entry := &model.LogEntry{}
		if err := json.Unmarshal(j, entry); err != nil {
			return err
		}
		if entry.Line < from {
			continue
		}
		if err := fn(entry); err != nil {
			return err
		}
	}

	return s.Err()
}

// LogLastLine gives the line of the file's last whole record, read from its
// end; -1 when there is none.
func (l logStore) LogLastLine(_ context.Context, step *model.Step) (int, error) {
	file, size, err := l.open(step)
	if file == nil || err != nil {
		return -1, err
	}
	defer file.Close()
	return lastLine(file, size)
}

// open opens the step's log file and gives its size now; no file when the
// step has none.
func (l logStore) open(step *model.Step) (*os.File, int64, error) {
	file, err := os.Open(l.filePath(step.ID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, 0, err
	}
	return file, info.Size(), nil
}

// recordLine is the line of a record.
type recordLine struct {
	Line int `json:"line"`
}

// recordChunk is how much of a record is read at a time from where a search
// lands: most records are shorter.
const recordChunk = 4096

// tailChunk is how much of a file is read at a time from its end.
const tailChunk = 64 * 1024

var errRecordTooLong = fmt.Errorf("a record of the log is longer than %d bytes", maxLineLength)

// lastLine reads back from the end of a file of records to its last whole
// one: the part after the last newline is still being written.
func lastLine(r io.ReaderAt, size int64) (int, error) {
	var tail []byte // the end of the file read so far
	pos := size
	end := -1 // in tail, the newline the last record ends with
	for {
		if end < 0 {
			end = bytes.LastIndexByte(tail, '\n')
		}
		if end >= 0 {
			start := bytes.LastIndexByte(tail[:end], '\n')
			if start >= 0 || pos == 0 {
				record := tail[start+1 : end]
				if len(bytes.TrimSpace(record)) == 0 { // a blank one: the one before
					if start < 0 {
						return -1, nil
					}
					end = start
					continue
				}
				var rec recordLine
				if err := json.Unmarshal(record, &rec); err != nil {
					return 0, err
				}
				return rec.Line, nil
			}
		}
		if pos == 0 {
			return -1, nil
		}
		if len(tail) > maxLineLength+tailChunk {
			return 0, errRecordTooLong
		}
		n := min(int64(tailChunk), pos)
		pos -= n
		chunk := make([]byte, n, int(n)+len(tail))
		if _, err := r.ReadAt(chunk, pos); err != nil {
			return 0, err
		}
		if end >= 0 {
			end += int(n)
		}
		tail = append(chunk, tail...)
	}
}

// firstRecordOf finds the offset of the first record whose line is at least
// line, by a binary search over the offsets of the file: a search that lands
// within a record goes on from the next one.
func firstRecordOf(r io.ReaderAt, size int64, line int) (int64, error) {
	lo, hi := int64(0), size
	for lo < hi {
		mid := lo + (hi-lo)>>1
		_, recLine, ok, err := recordAfter(r, size, mid)
		if err != nil {
			return 0, err
		}
		if !ok || recLine >= line {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	start, _, ok, err := recordAfter(r, size, lo)
	if !ok {
		return size, err
	}
	return start, err
}

// recordAfter gives the start and the line of the first whole record that
// starts at off or after it; not ok when there is none.
func recordAfter(r io.ReaderAt, size, off int64) (int64, int, bool, error) {
	start := off
	if off > 0 {
		var prev [1]byte
		if _, err := r.ReadAt(prev[:], off-1); err != nil {
			return 0, 0, false, err
		}
		if prev[0] != '\n' { // within a record: skip its rest
			skipped, ok, err := readRecord(r, size, off, nil)
			if err != nil || !ok {
				return 0, 0, false, err
			}
			start = off + skipped
		}
	}
	for start < size {
		var rec []byte
		n, ok, err := readRecord(r, size, start, &rec)
		if err != nil || !ok {
			return 0, 0, false, err
		}
		if len(bytes.TrimSpace(rec)) > 0 {
			var line recordLine
			if err := json.Unmarshal(rec, &line); err != nil {
				return 0, 0, false, err
			}
			return start, line.Line, true, nil
		}
		start += n
	}
	return 0, 0, false, nil
}

// readRecord reads from off to the next newline: how many bytes that takes
// with the newline, and into rec, if given, the record without it. Not ok
// when the file ends first.
func readRecord(r io.ReaderAt, size, off int64, rec *[]byte) (int64, bool, error) {
	br := bufio.NewReaderSize(io.NewSectionReader(r, off, size-off), recordChunk)
	var n int64
	for {
		part, err := br.ReadSlice('\n')
		n += int64(len(part))
		if rec != nil {
			*rec = append(*rec, part...)
		}
		switch {
		case err == nil:
			if rec != nil {
				*rec = (*rec)[:len(*rec)-1]
			}
			return n, true, nil
		case errors.Is(err, bufio.ErrBufferFull):
			if n > int64(maxLineLength) {
				return 0, false, errRecordTooLong
			}
		case errors.Is(err, io.EOF):
			return n, false, nil
		default:
			return 0, false, err
		}
	}
}

// scanWrittenLines is bufio.ScanLines without the final line that lacks its newline.
func scanWrittenLines(data []byte, atEOF bool) (int, []byte, error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[:i], nil
	}
	if atEOF {
		// a partial record: stop without a token
		return len(data), nil, nil
	}
	return 0, nil, nil
}

// LogAppend appends the entries to the step's log file in the order of their
// lines, so the file stays in that order, which LogStreamFrom and
// LogLastLine read it by. An entry whose line is not after the last line of
// the file is dropped, as the database refuses a line it has: a batch sent
// again, or the output of a workflow run again from line 0, adds nothing.
func (l logStore) LogAppend(_ context.Context, step *model.Step, logEntries []*model.LogEntry) error {
	path := l.filePath(step.ID)
	lock := &l.appends[uint64(step.ID)%appendLocks]
	lock.Lock()
	defer lock.Unlock()

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		log.Error().Err(err).Msgf("could not open log file %s", path)
		return err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}
	last, err := lastLine(file, info.Size())
	if err != nil {
		// a log whose end cannot be read still gets what comes
		log.Warn().Err(err).Int64("step", step.ID).Msg("could not read the last line of a log file")
		last = -1
	}
	entries := slices.Clone(logEntries)
	slices.SortStableFunc(entries, func(a, b *model.LogEntry) int { return cmp.Compare(a.Line, b.Line) })

	var data []byte
	dropped := 0
	for _, logEntry := range entries {
		if logEntry.Line <= last {
			dropped++
			continue
		}
		last = logEntry.Line
		if jsonLine, err := json.Marshal(logEntry); err == nil {
			data = append(data, jsonLine...)
			data = append(data, byte('\n'))
		} else {
			log.Error().Err(err).Msg("could not convert log entry to JSON")
		}
	}

	if dropped > 0 {
		log.Debug().Int64("step", step.ID).Int("dropped", dropped).Msg("log entries of lines the log has are dropped")
	}

	if _, err = file.Write(data); err != nil {
		log.Error().Err(err).Msg("could not write out log entries")
	}

	return file.Close()
}

func (l logStore) LogDelete(_ context.Context, step *model.Step) error {
	err := os.Remove(l.filePath(step.ID))
	if os.IsNotExist(err) {
		// a step that never wrote a log has nothing to delete
		return nil
	}
	return err
}

func (l logStore) StepFinished(_ context.Context, _ *model.Step) error { return nil }
