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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatusValueHelpers(t *testing.T) {
	tests := []struct {
		s       StatusValue
		done    bool
		failure bool
	}{
		{StatusCreated, false, false},
		{StatusBlocked, false, false},
		{StatusPending, false, false},
		{StatusRunning, false, false},
		{StatusSuccess, true, false},
		{StatusSkipped, true, false},
		{StatusCanceled, true, false},
		{StatusDeclined, true, false},
		{StatusFailure, true, true},
		{StatusTimeout, true, true},
		{StatusKilled, true, true},
		{StatusError, true, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.s), func(t *testing.T) {
			assert.NoError(t, tt.s.Validate())
			assert.Equal(t, tt.done, tt.s.IsDone(), "IsDone")
			assert.Equal(t, tt.failure, tt.s.IsFailure(), "IsFailure")
		})
	}

	unknown := StatusValue("from-the-future")
	assert.ErrorIs(t, unknown.Validate(), ErrInvalidStatusValue)
	assert.False(t, unknown.IsDone())
	assert.False(t, unknown.IsFailure())
}
