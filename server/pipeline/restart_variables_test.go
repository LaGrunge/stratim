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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWithRestartVariables: a restart or deployment sets the run's
// variables it gives a value, over the old ones, without changing them.
func TestWithRestartVariables(t *testing.T) {
	old := map[string]string{"TIER": "light", "PIN": "main"}
	got := withRestartVariables(old, map[string]string{"TIER": "full", "PIN": "", "deploy_task": "migrate", "access_token": "secret", "NEW": "x"})
	assert.Equal(t, map[string]string{"TIER": "full", "PIN": "main", "NEW": "x"}, got)
	assert.Equal(t, map[string]string{"TIER": "light", "PIN": "main"}, old, "the old pipeline's variables stay")

	assert.Nil(t, withRestartVariables(nil, map[string]string{"A": ""}))
	assert.Equal(t, map[string]string{"A": "1"}, withRestartVariables(nil, map[string]string{"A": "1"}))
}
