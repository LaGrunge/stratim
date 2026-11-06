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

package explain

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	service_log "go.woodpecker-ci.org/woodpecker/v3/server/services/log"
)

func numbers(w *Window) []int {
	out := []int{}
	for _, l := range w.Lines {
		out = append(out, l.N)
	}
	return out
}

// A window reads only its lines: from where it starts to one past its end.
func TestWindowReadsOnlyItsLines(t *testing.T) {
	var log strings.Builder
	for i := range 10000 {
		fmt.Fprintf(&log, "line %d\n", i+1)
	}
	logs := newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, log.String())})
	w := NewWindows(logs)

	got, err := w.Window(t.Context(), &model.Step{ID: 1}, WindowQuery{From: 5000, Lines: 10})
	require.NoError(t, err)
	assert.Equal(t, []int{5000, 5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009}, numbers(got))
	assert.Equal(t, 5010, got.Next)
	assert.EqualValues(t, 11, logs.read.Load(), "the window and the line after it")

	logs.read.Store(0)
	got, err = w.Window(t.Context(), &model.Step{ID: 1}, WindowQuery{From: -3, Lines: 10})
	require.NoError(t, err)
	assert.Equal(t, []int{9998, 9999, 10000}, numbers(got))
	assert.Zero(t, got.Next)
	assert.EqualValues(t, 3, logs.read.Load(), "from the end, only the end")
}

func TestWindowGrepIgnoringTheCase(t *testing.T) {
	logs := newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, "ÄRGER: one\nfine\närger: two\nERROR\n\xff Ärger")})
	w := NewWindows(logs)
	got, err := w.Window(t.Context(), &model.Step{ID: 1}, WindowQuery{From: 1, Grep: "äRGER", IgnoreCase: true})
	require.NoError(t, err)
	assert.Equal(t, []int{1, 3, 5}, numbers(got), "case folded beyond ASCII, an invalid byte kept")
	got, err = w.Window(t.Context(), &model.Step{ID: 1}, WindowQuery{From: 1, Grep: "ÄRGER"})
	require.NoError(t, err)
	assert.Equal(t, []int{1}, numbers(got))
}

// Grepping ignoring the case reuses its buffers: it allocates per line kept,
// not per line read.
func TestWindowGrepAllocatesPerLineKept(t *testing.T) {
	var log strings.Builder
	for range 2000 {
		log.WriteString("Some line Of A Log\n")
	}
	logs := newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, log.String())})
	w := NewWindows(logs)
	allocs := testing.AllocsPerRun(5, func() {
		_, err := w.Window(t.Context(), &model.Step{ID: 1}, WindowQuery{From: 1, Grep: "nothing", IgnoreCase: true})
		require.NoError(t, err)
	})
	assert.Less(t, allocs, 100.0)
}

// deletingLogs is a log store that only deletes.
type deletingLogs struct {
	service_log.Service
	deleted []int64
}

func (d *deletingLogs) LogDelete(_ context.Context, step *model.Step) error {
	d.deleted = append(d.deleted, step.ID)
	return nil
}

func TestForgetDeleted(t *testing.T) {
	logs := newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, "a.cc:1:2: error: boom\n")})
	x := New(logs, Options{})
	_, err := x.Explain(t.Context(), failedStep(1))
	require.NoError(t, err)

	store := &deletingLogs{}
	require.NoError(t, ForgetDeleted(store, x).LogDelete(t.Context(), &model.Step{ID: 1}))
	assert.Equal(t, []int64{1}, store.deleted)
	_, kept := x.cache.byID[1]
	assert.False(t, kept, "the result of a deleted log is dropped")
}
