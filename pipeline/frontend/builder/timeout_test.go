// Copyright 2026 Woodpecker Authors
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

package builder

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTimeouts: a workflow's timeout reaches its item, a step's its step,
// and a step without one has none.
func TestTimeouts(t *testing.T) {
	t.Parallel()
	items, err := buildConfig(t, nil, nil, `
timeout: 1h30m
steps:
  - name: build
    image: golang
    commands: [make]
    timeout: 20m
  - name: test
    image: golang
    commands: [make test]
`)
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, 90*time.Minute, items[0].Timeout)
	steps := stepsByName(items)
	assert.Equal(t, 20*time.Minute, steps["build"].Timeout)
	assert.Zero(t, steps["test"].Timeout)
}
