// Copyright 2024 Woodpecker Authors
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

package utils_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/autoscaler/utils"
)

func TestRandomString(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want int
	}{
		{
			name: "zero length",
			n:    0,
			want: 0,
		},
		{
			name: "length 10",
			n:    10,
			want: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := utils.RandomString(tt.n)
			assert.Equal(t, tt.want, len(str))
		})

		t.Run("alphanumeric", func(t *testing.T) {
			str1 := utils.RandomString(10)
			for _, r := range str1 {
				assert.Contains(t, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", string(r))
			}
		})
	}
}

// Rapid successive calls must not repeat: a generator re-seeded from the wall
// clock on every call collapses to identical output within one clock tick on
// coarse-clock platforms, producing duplicate agent names.
func TestRandomStringRapidCallsAreUnique(t *testing.T) {
	const calls = 1000

	seen := make(map[string]struct{}, calls)
	for range calls {
		seen[utils.RandomString(10)] = struct{}{}
	}

	assert.Len(t, seen, calls)
}
