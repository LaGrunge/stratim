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

package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCanRestartFailed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status StatusValue
		states []StatusValue
		want   bool
	}{
		{"all passed", StatusSuccess, []StatusValue{StatusSuccess, StatusSkipped}, false},
		{"a failure", StatusFailure, []StatusValue{StatusSuccess, StatusFailure}, true},
		{"an error", StatusError, []StatusValue{StatusError}, true},
		{"a timeout", StatusTimeout, []StatusValue{StatusSuccess, StatusTimeout}, true},
		{"killed", StatusKilled, []StatusValue{StatusKilled, StatusSkipped}, true},
		{"canceled before anything ran", StatusCanceled, []StatusValue{StatusSkipped}, true},
		{"canceled with nothing left", StatusKilled, []StatusValue{StatusSuccess}, false},
		{"no workflows", StatusError, nil, false},
		{"still running", StatusFailure, []StatusValue{StatusFailure, StatusRunning}, false},
		{"pending", StatusPending, []StatusValue{StatusFailure}, false},
		{"running", StatusRunning, []StatusValue{StatusFailure}, false},
		{"blocked", StatusBlocked, []StatusValue{StatusBlocked}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, CanRestartFailed(tt.status, tt.states))
		})
	}
}

func TestAPIPipelineCanRestartFailed(t *testing.T) {
	t.Parallel()

	p := &Pipeline{Status: StatusFailure, Workflows: []*Workflow{{State: StatusFailure}}}
	if can := p.ToAPIModel().CanRestartFailed; assert.NotNil(t, can) {
		assert.True(t, *can)
	}

	p = &Pipeline{Status: StatusFailure}
	assert.Nil(t, p.ToAPIModel().CanRestartFailed, "unknown, and omitted, without the workflows")
	data, err := json.Marshal(p.ToAPIModel())
	assert.NoError(t, err)
	assert.NotContains(t, string(data), "can_restart_failed")
}
