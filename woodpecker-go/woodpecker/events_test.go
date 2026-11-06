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

package woodpecker

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventSeverityText(t *testing.T) {
	for s := SeverityDebug; s <= SeverityError; s++ {
		text, err := s.MarshalText()
		require.NoError(t, err)
		var parsed EventSeverity
		require.NoError(t, parsed.UnmarshalText(text))
		assert.Equal(t, s, parsed)
	}
	var strict EventSeverity
	require.Error(t, strict.UnmarshalText([]byte("critical")))
	require.Error(t, strict.UnmarshalText([]byte("")))
	_, err := ParseEventSeverity("fatal")
	assert.Error(t, err)
	_, err = EventSeverity(9).MarshalText()
	assert.Error(t, err)
}

func TestPipelineEventJSON(t *testing.T) {
	var e PipelineEvent
	require.NoError(t, json.Unmarshal([]byte(`{"v":1,"id":7,"type":"step.failed","severity":"error","status":"failure","exit_code":1,"new_field":true}`), &e))
	assert.Equal(t, StepFailed, e.Type)
	assert.Equal(t, SeverityError, e.Severity)
	require.NotNil(t, e.ExitCode)
	assert.Equal(t, 1, *e.ExitCode)

	_, err := ParsePipelineEventType("step.exploded")
	assert.Error(t, err)
}
