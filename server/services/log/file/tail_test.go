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

package file

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// countingFile counts the bytes read of a file.
type countingFile struct {
	*os.File
	read int64
}

func (f *countingFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	f.read += int64(n)
	return n, err
}

func TestLogStreamFromAndLastLine(t *testing.T) {
	t.Parallel()

	s, err := NewLogStore(t.TempDir())
	require.NoError(t, err)
	step := &model.Step{ID: 3}

	last, err := s.LogLastLine(t.Context(), step)
	require.NoError(t, err)
	assert.Equal(t, -1, last, "no file")

	// records of many sizes, some longer than a chunk read from the end
	const total = 400
	var entries []*model.LogEntry
	for i := range total {
		size := (i * 37) % 300
		if i%97 == 0 {
			size = 3 * tailChunk
		}
		entries = append(entries, &model.LogEntry{StepID: 3, Line: i, Data: bytes.Repeat([]byte("x"), size)})
	}
	require.NoError(t, s.LogAppend(t.Context(), step, entries))

	last, err = s.LogLastLine(t.Context(), step)
	require.NoError(t, err)
	assert.Equal(t, total-1, last)

	for _, from := range []int{0, 1, 96, 97, 98, 200, total - 1, total, total + 5} {
		var lines []int
		require.NoError(t, s.LogStreamFrom(t.Context(), step, from, func(entry *model.LogEntry) error {
			lines = append(lines, entry.Line)
			return nil
		}))
		want := max(total-from, 0)
		require.Len(t, lines, want, "from %d", from)
		if want > 0 {
			assert.Equal(t, from, lines[0], "from %d", from)
		}
	}
}

func TestLastLineSkipsTheRecordBeingWritten(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	s, err := NewLogStore(base)
	require.NoError(t, err)
	path := filepath.Join(base, "9.json")

	require.NoError(t, os.WriteFile(path, []byte("{\"line\":0}\n{\"line\":1}\n\n{\"line\":2,\"da"), 0o600))
	last, err := s.LogLastLine(t.Context(), &model.Step{ID: 9})
	require.NoError(t, err)
	assert.Equal(t, 1, last, "the last whole record, past a blank one")

	require.NoError(t, os.WriteFile(path, []byte("{\"line\":0,\"da"), 0o600))
	last, err = s.LogLastLine(t.Context(), &model.Step{ID: 9})
	require.NoError(t, err)
	assert.Equal(t, -1, last, "none whole yet")

	require.NoError(t, os.WriteFile(path, []byte("{\"line\":7}\n"), 0o600))
	last, err = s.LogLastLine(t.Context(), &model.Step{ID: 9})
	require.NoError(t, err)
	assert.Equal(t, 7, last, "the one record")
}

// record is a record of a log file with a line and data of n bytes, n large
// enough for the JSON, its newline included, to take size bytes.
func record(t *testing.T, line, size int) []byte {
	t.Helper()
	base := fmt.Appendf(nil, `{"line":%d,"x":""}`, line)
	pad := size - len(base) - 1
	require.GreaterOrEqual(t, pad, 0)
	return fmt.Appendf(nil, `{"line":%d,"x":"%s"}`+"\n", line, strings.Repeat("y", pad))
}

// The last line is found wherever the newlines fall against the chunks read
// from the end: a last record longer than a chunk, a newline just before,
// at and after a chunk's start.
func TestLastLineAcrossChunks(t *testing.T) {
	t.Parallel()

	for _, lastSize := range []int{tailChunk - 1, tailChunk, tailChunk + 1, 3*tailChunk + 7} {
		var data []byte
		data = append(data, record(t, 0, 100)...)
		data = append(data, record(t, 1, 300)...)
		data = append(data, record(t, 2, lastSize)...)
		data = append(data, `{"line":3,"x":"half`...)
		last, err := lastLine(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, err, "last record of %d bytes", lastSize)
		assert.Equal(t, 2, last, "last record of %d bytes", lastSize)
	}
}

func TestRecordsTooLong(t *testing.T) {
	t.Parallel()

	long := append(record(t, 0, maxLineLength+3*tailChunk), record(t, 1, 50)[:20]...)
	_, err := lastLine(bytes.NewReader(long), int64(len(long)))
	assert.ErrorIs(t, err, errRecordTooLong, "the last record cannot be found")

	_, _, err = readRecord(bytes.NewReader(long), int64(len(long)), 0, nil)
	assert.ErrorIs(t, err, errRecordTooLong, "a record is not read past the limit")
}

// A log file whose last record is half written is streamed from any line up
// to its last whole record.
func TestLogStreamFromAHalfWrittenEnd(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	s, err := NewLogStore(base)
	require.NoError(t, err)
	var data []byte
	for i := range 10 {
		data = append(data, record(t, i, 40+i*13)...)
	}
	data = append(data, `{"line":10,"x":"ha`...)
	require.NoError(t, os.WriteFile(filepath.Join(base, "5.json"), data, 0o600))
	step := &model.Step{ID: 5}

	for from, want := range map[int][]int{0: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 9: {9}, 10: nil, 11: nil, 4: {4, 5, 6, 7, 8, 9}} {
		var got []int
		require.NoError(t, s.LogStreamFrom(t.Context(), step, from, func(e *model.LogEntry) error {
			got = append(got, e.Line)
			return nil
		}))
		assert.Equal(t, want, got, "from %d", from)
	}
}

func lines(t *testing.T, s interface {
	LogStream(context.Context, *model.Step, func(*model.LogEntry) error) error
}, step *model.Step,
) []int {
	t.Helper()
	var got []int
	require.NoError(t, s.LogStream(t.Context(), step, func(e *model.LogEntry) error {
		got = append(got, e.Line)
		return nil
	}))
	return got
}

func appendLines(t *testing.T, s interface {
	LogAppend(context.Context, *model.Step, []*model.LogEntry) error
}, step *model.Step, numbers ...int,
) {
	t.Helper()
	var entries []*model.LogEntry
	for _, n := range numbers {
		entries = append(entries, &model.LogEntry{StepID: step.ID, Line: n, Data: fmt.Appendf(nil, "%d", n)})
	}
	require.NoError(t, s.LogAppend(t.Context(), step, entries))
}

// The file keeps its records in the order of their lines, as the search
// reads it by: a batch sent again, or late, adds only the lines after the
// last one, and so does a workflow run again from line 0, as the database
// refuses the lines it has.
func TestLogAppendKeepsTheLinesInOrder(t *testing.T) {
	t.Parallel()

	s, err := NewLogStore(t.TempDir())
	require.NoError(t, err)

	retried := &model.Step{ID: 1}
	appendLines(t, s, retried, 0, 1, 2, 3)
	appendLines(t, s, retried, 1, 2)
	appendLines(t, s, retried, 4, 5)
	appendLines(t, s, retried, 7, 6) // out of order within a batch
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, lines(t, s, retried))

	rerun := &model.Step{ID: 2}
	appendLines(t, s, rerun, 0, 1, 2, 3)
	appendLines(t, s, rerun, 0, 1)
	appendLines(t, s, rerun, 2, 3, 4, 5)
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, lines(t, s, rerun))

	for _, step := range []*model.Step{retried, rerun} {
		var got []int
		require.NoError(t, s.LogStreamFrom(t.Context(), step, 3, func(e *model.LogEntry) error {
			got = append(got, e.Line)
			return nil
		}))
		assert.Equal(t, 3, got[0])
		assert.True(t, slices.IsSorted(got))
	}
}

// The end of a long log is found by reading a little of it.
func TestFirstRecordOfReadsLittle(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	s, err := NewLogStore(base)
	require.NoError(t, err)
	step := &model.Step{ID: 4}
	const total = 50000
	entries := make([]*model.LogEntry, 0, total)
	for i := range total {
		entries = append(entries, &model.LogEntry{StepID: 4, Line: i, Data: bytes.Repeat([]byte("y"), 100)})
	}
	require.NoError(t, s.LogAppend(t.Context(), step, entries))

	file, err := os.Open(filepath.Join(base, "4.json"))
	require.NoError(t, err)
	defer file.Close()
	info, err := file.Stat()
	require.NoError(t, err)

	counting := &countingFile{File: file}
	offset, err := firstRecordOf(counting, info.Size(), total-100)
	require.NoError(t, err)
	_, line, ok, err := recordAfter(file, info.Size(), offset)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, total-100, line)
	assert.Less(t, counting.read, info.Size()/10, "a binary search, not a scan")
}
