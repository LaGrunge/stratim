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

package log

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func TestStreamFound(t *testing.T) {
	entries := []*model.LogEntry{{Line: 0}, {Line: 1}, {Line: 2}}
	find := func(context.Context, *model.Step) ([]*model.LogEntry, error) { return entries, nil }

	var lines []int
	require.NoError(t, StreamFound(t.Context(), find, &model.Step{}, func(entry *model.LogEntry) error {
		lines = append(lines, entry.Line)
		return nil
	}))
	assert.Equal(t, []int{0, 1, 2}, lines)

	calls := 0
	err := StreamFound(t.Context(), find, &model.Step{}, func(*model.LogEntry) error {
		calls++
		return assert.AnError
	})
	assert.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 1, calls)

	failing := func(context.Context, *model.Step) ([]*model.LogEntry, error) { return nil, assert.AnError }
	assert.ErrorIs(t, StreamFound(t.Context(), failing, &model.Step{}, func(*model.LogEntry) error {
		t.Fatal("no entries expected")
		return nil
	}), assert.AnError)
}
