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

package datastore

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func TestLogCreateFindDelete(t *testing.T) {
	store, closer := newTestStore(t, new(model.Step), new(model.LogEntry))
	defer closer()

	step := model.Step{
		ID: 1,
	}

	logEntries := []*model.LogEntry{
		{
			StepID: step.ID,
			Data:   []byte("hello"),
			Line:   1,
			Time:   0,
		},
		{
			StepID: step.ID,
			Data:   []byte("world"),
			Line:   2,
			Time:   10,
		},
	}

	assert.NoError(t, store.LogAppend(t.Context(), &step, logEntries))

	// we want to find our inserted logs
	_logEntries, err := store.LogFind(t.Context(), &step)
	assert.NoError(t, err)
	assert.Len(t, _logEntries, len(logEntries))

	// delete and check
	assert.NoError(t, store.LogDelete(t.Context(), &step))
	_logEntries, err = store.LogFind(t.Context(), &step)
	assert.NoError(t, err)
	assert.Len(t, _logEntries, 0)
}

func TestLogAppend(t *testing.T) {
	store, closer := newTestStore(t, new(model.Step), new(model.LogEntry))
	defer closer()

	step := model.Step{
		ID: 1,
	}
	logEntries := []*model.LogEntry{
		{
			StepID: step.ID,
			Data:   []byte("hello"),
			Line:   1,
			Time:   0,
		},
		{
			StepID: step.ID,
			Data:   []byte("world"),
			Line:   2,
			Time:   10,
		},
	}

	assert.NoError(t, store.LogAppend(t.Context(), &step, logEntries))

	logEntry := &model.LogEntry{
		StepID: step.ID,
		Data:   []byte("allo?"),
		Line:   3,
		Time:   20,
	}

	assert.NoError(t, store.LogAppend(t.Context(), &step, []*model.LogEntry{logEntry}))

	_logEntries, err := store.LogFind(t.Context(), &step)
	assert.NoError(t, err)
	assert.Len(t, _logEntries, len(logEntries)+1)
}

func TestLogFindOrdersByLine(t *testing.T) {
	store, closer := newTestStore(t, new(model.Step), new(model.LogEntry))
	defer closer()

	step := model.Step{
		ID: 1,
	}

	// The first batch is written through a node holding the id cache 4000+,
	// the second one through a node holding the lower cache 30+.
	assert.NoError(t, store.LogAppend(t.Context(), &step, []*model.LogEntry{
		{ID: 4000, StepID: step.ID, Data: []byte("first"), Line: 0, Time: 0},
		{ID: 4001, StepID: step.ID, Data: []byte("second"), Line: 1, Time: 10},
	}))
	assert.NoError(t, store.LogAppend(t.Context(), &step, []*model.LogEntry{
		{ID: 30, StepID: step.ID, Data: []byte("third"), Line: 2, Time: 20},
		{ID: 31, StepID: step.ID, Data: []byte("fourth"), Line: 3, Time: 30},
	}))

	logEntries, err := store.LogFind(t.Context(), &step)
	assert.NoError(t, err)

	lines := make([]int, 0, len(logEntries))
	data := make([]string, 0, len(logEntries))
	for _, logEntry := range logEntries {
		lines = append(lines, logEntry.Line)
		data = append(data, string(logEntry.Data))
	}

	assert.Equal(t, []int{0, 1, 2, 3}, lines)
	assert.Equal(t, []string{"first", "second", "third", "fourth"}, data)
}

func TestLogAppendRejectsResentEntries(t *testing.T) {
	store, closer := newTestStore(t, new(model.Step), new(model.LogEntry))
	defer closer()

	step := model.Step{
		ID: 1,
	}

	assert.NoError(t, store.LogAppend(t.Context(), &step, []*model.LogEntry{
		{StepID: step.ID, Data: []byte("hello"), Line: 0, Time: 0},
		{StepID: step.ID, Data: []byte("world"), Line: 1, Time: 10},
	}))
	// the agent retries with the batch it already sent
	assert.Error(t, store.LogAppend(t.Context(), &step, []*model.LogEntry{
		{StepID: step.ID, Data: []byte("hello"), Line: 0, Time: 0},
		{StepID: step.ID, Data: []byte("world"), Line: 1, Time: 10},
	}))

	logEntries, err := store.LogFind(t.Context(), &step)
	assert.NoError(t, err)

	lines := make([]int, 0, len(logEntries))
	data := make([]string, 0, len(logEntries))
	for _, logEntry := range logEntries {
		lines = append(lines, logEntry.Line)
		data = append(data, string(logEntry.Data))
	}

	assert.Equal(t, []int{0, 1}, lines)
	assert.Equal(t, []string{"hello", "world"}, data)
}

// holdConnection takes the only connection of a test store (MaxOpenConns=1), like a hung database,
// until release is called.
func holdConnection(t *testing.T, s *storage) (release func()) {
	t.Helper()
	conn, err := s.engine.DB().Conn(t.Context())
	require.NoError(t, err)
	return func() { _ = conn.Close() }
}

// assertCanceled runs call with a context that ends soon and fails if call keeps waiting.
func assertCanceled(t *testing.T, call func(ctx context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- call(ctx) }()
	select {
	case err := <-errc:
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("still waiting for the database after the context ended")
	}
}

func TestLogCanceled(t *testing.T) {
	store, closer := newTestStore(t, new(model.Step), new(model.LogEntry))
	defer closer()
	defer holdConnection(t, store)()
	step := &model.Step{ID: 1}

	assertCanceled(t, func(ctx context.Context) error {
		_, err := store.LogFind(ctx, step)
		return err
	})
	assertCanceled(t, func(ctx context.Context) error {
		return store.LogAppend(ctx, step, []*model.LogEntry{{StepID: step.ID, Line: 1}})
	})
	assertCanceled(t, func(ctx context.Context) error { return store.LogDelete(ctx, step) })
}

func TestLogStreamPages(t *testing.T) {
	for _, total := range []int{0, 1, logStreamPage - 1, logStreamPage, logStreamPage + 1, 2 * logStreamPage} {
		t.Run(fmt.Sprint(total), func(t *testing.T) {
			store, closer := newTestStore(t, new(model.Step), new(model.LogEntry))
			defer closer()

			step := &model.Step{ID: 1}
			other := &model.Step{ID: 2}
			entries := make([]*model.LogEntry, 0, total)
			// stored out of line order, the stream must still follow the lines
			for i := total - 1; i >= 0; i-- {
				entries = append(entries, &model.LogEntry{StepID: step.ID, Line: i, Data: []byte{byte(i)}})
			}
			require.NoError(t, store.LogAppend(t.Context(), step, entries))
			// another step's line inside the range must not leak in
			require.NoError(t, store.LogAppend(t.Context(), other, []*model.LogEntry{{StepID: other.ID, Line: total / 2}}))

			var lines []int
			require.NoError(t, store.LogStream(t.Context(), step, func(entry *model.LogEntry) error {
				assert.Equal(t, step.ID, entry.StepID)
				lines = append(lines, entry.Line)
				return nil
			}))
			require.Len(t, lines, total)
			for i, line := range lines {
				assert.Equal(t, i, line)
			}
		})
	}
}

func TestLogStreamBatchesByBytes(t *testing.T) {
	store, closer := newTestStore(t, new(model.Step), new(model.LogEntry))
	defer closer()

	step := &model.Step{ID: 1}
	sizes := []int{4, 4, 4, 30, 0, 9, 1, 12, 3}
	entries := make([]*model.LogEntry, 0, len(sizes))
	for i, size := range sizes {
		entries = append(entries, &model.LogEntry{StepID: step.ID, Line: i, Data: bytes.Repeat([]byte{'a' + byte(i)}, size)})
	}
	require.NoError(t, store.LogAppend(t.Context(), step, entries))

	var got []*model.LogEntry
	var batches [][2]int
	// pages of 4 lines, batches of up to 10 bytes
	require.NoError(t, store.logStream(t.Context(), step, 0, func(entry *model.LogEntry) error {
		got = append(got, entry)
		return nil
	}, 4, 4, 10, func(first, last int) {
		batches = append(batches, [2]int{first, last})
	}))
	require.Len(t, got, len(sizes))
	for i, entry := range got {
		assert.Equal(t, i, entry.Line)
		assert.Equal(t, string(entries[i].Data), string(entry.Data))
	}
	assert.Equal(t, [][2]int{
		{0, 1}, // 4+4, another 4 would pass 10
		{2, 2}, // the 30 byte line does not fit
		{3, 3}, // longer than a batch, loaded alone
		{4, 6}, // 0+9+1, exactly 10
		{7, 7}, // 12 bytes alone
		{8, 8}, // the last, short page
	}, batches)
}

func TestLogStreamSnapshot(t *testing.T) {
	store, closer := newTestStore(t, new(model.Step), new(model.LogEntry))
	defer closer()

	step := &model.Step{ID: 1}
	require.NoError(t, store.LogAppend(t.Context(), step, []*model.LogEntry{
		{StepID: step.ID, Line: 0}, {StepID: step.ID, Line: 1},
	}))

	var lines []int
	require.NoError(t, store.logStream(t.Context(), step, 0, func(entry *model.LogEntry) error {
		if entry.Line == 0 {
			// the step is still running; no query is open while fn runs
			require.NoError(t, store.LogAppend(t.Context(), step, []*model.LogEntry{{StepID: step.ID, Line: 2}}))
		}
		lines = append(lines, entry.Line)
		return nil
	}, 1, 1, logStreamBatchBytes, nil))
	assert.Equal(t, []int{0, 1}, lines)
}

func TestLogStreamStopsOnError(t *testing.T) {
	store, closer := newTestStore(t, new(model.Step), new(model.LogEntry))
	defer closer()

	step := &model.Step{ID: 1}
	require.NoError(t, store.LogAppend(t.Context(), step, []*model.LogEntry{
		{StepID: step.ID, Line: 0}, {StepID: step.ID, Line: 1},
	}))

	calls := 0
	err := store.LogStream(t.Context(), step, func(*model.LogEntry) error {
		calls++
		return assert.AnError
	})
	assert.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 1, calls)
}

func TestLogStreamFromAndLastLine(t *testing.T) {
	store, closer := newTestStore(t, new(model.Step), new(model.LogEntry))
	defer closer()

	step := &model.Step{ID: 1}
	empty := &model.Step{ID: 2}
	last, err := store.LogLastLine(t.Context(), empty)
	require.NoError(t, err)
	assert.Equal(t, -1, last, "no log")

	var entries []*model.LogEntry
	for i := range 2*logStreamPage + 10 {
		entries = append(entries, &model.LogEntry{StepID: step.ID, Line: i, Data: []byte{byte(i)}})
	}
	require.NoError(t, store.LogAppend(t.Context(), step, entries))
	last, err = store.LogLastLine(t.Context(), step)
	require.NoError(t, err)
	assert.Equal(t, 2*logStreamPage+9, last)

	for _, from := range []int{0, 5, logStreamPage, 2*logStreamPage + 9, 2*logStreamPage + 10} {
		var lines []int
		require.NoError(t, store.LogStreamFrom(t.Context(), step, from, func(entry *model.LogEntry) error {
			lines = append(lines, entry.Line)
			return nil
		}))
		require.Len(t, lines, 2*logStreamPage+10-from, "from %d", from)
		if len(lines) > 0 {
			assert.Equal(t, from, lines[0])
			assert.Equal(t, last, lines[len(lines)-1])
		}
	}
}

// A line the log has is refused: a batch sent again, or the output of a
// workflow run again from line 0, adds nothing; the file store drops the
// same lines.
func TestLogAppendRefusesTheLinesItHas(t *testing.T) {
	store, closer := newTestStore(t, new(model.Step), new(model.LogEntry))
	defer closer()

	step := &model.Step{ID: 1}
	batch := func(lines ...int) []*model.LogEntry {
		var out []*model.LogEntry
		for _, l := range lines {
			out = append(out, &model.LogEntry{StepID: step.ID, Line: l, Data: []byte{byte(l)}})
		}
		return out
	}
	require.NoError(t, store.LogAppend(t.Context(), step, batch(0, 1, 2, 3)))
	assert.ErrorIs(t, store.LogAppend(t.Context(), step, batch(0, 1)), types.ErrInsertDuplicateDetected)
	require.NoError(t, store.LogAppend(t.Context(), step, batch(4, 5)))

	var got []int
	require.NoError(t, store.LogStream(t.Context(), step, func(e *model.LogEntry) error {
		got = append(got, e.Line)
		return nil
	}))
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5}, got)
}
