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

package datastore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// A stream from a line loads pages that grow from a few lines: a window of a
// few lines loads about those.
func TestLogStreamFromGrowsItsPages(t *testing.T) {
	store, closer := newTestStore(t, new(model.Step), new(model.LogEntry))
	defer closer()

	step := &model.Step{ID: 1}
	var entries []*model.LogEntry
	for i := range 3000 {
		entries = append(entries, &model.LogEntry{StepID: step.ID, Line: i, Data: []byte{byte(i)}})
	}
	require.NoError(t, store.LogAppend(t.Context(), step, entries))

	var batches [][2]int
	stop := errors.New("the window is full")
	read := 0
	err := store.logStream(t.Context(), step, 100, func(*model.LogEntry) error {
		read++
		if read > 10 {
			return stop
		}
		return nil
	}, logStreamFirstPage, logStreamPage, logStreamBatchBytes, func(first, last int) {
		batches = append(batches, [2]int{first, last})
	})
	require.ErrorIs(t, err, stop)
	assert.Equal(t, [][2]int{{100, 100 + logStreamFirstPage - 1}}, batches, "one small page")

	var lines []int
	require.NoError(t, store.LogStreamFrom(t.Context(), step, 100, func(e *model.LogEntry) error {
		lines = append(lines, e.Line)
		return nil
	}))
	require.Len(t, lines, 2900, "the pages grow to the end")
	for i, l := range lines {
		require.Equal(t, 100+i, l)
	}
}
