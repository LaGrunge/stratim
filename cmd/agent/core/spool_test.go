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

package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogSpoolMaxBytes(t *testing.T) {
	for _, tc := range []struct {
		mib  int
		want int64
		ok   bool
	}{
		{0, 0, true},
		{1024, 1 << 30, true},
		{maxLogSpoolMiB, 1 << 40, true},
		{-1, 0, false},
		{maxLogSpoolMiB + 1, 0, false},
	} {
		got, err := logSpoolMaxBytes(tc.mib)
		if tc.ok {
			assert.NoError(t, err, "%d MiB", tc.mib)
			assert.Equal(t, tc.want, got, "%d MiB", tc.mib)
		} else {
			assert.Error(t, err, "%d MiB", tc.mib)
		}
	}
}
