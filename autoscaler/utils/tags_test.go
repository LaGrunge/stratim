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

package utils_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/autoscaler/utils"
)

func TestCheckReservedTags(t *testing.T) {
	const reservedPrefix = "reserved/"

	errReserved := errors.New("reserved tag prefix")

	tests := []struct {
		name    string
		tags    []string
		wantErr error
	}{
		{
			name: "no tags at all",
		},
		{
			name: "operator tags are kept",
			tags: []string{"env=prod", "team-ci"},
		},
		{
			name:    "a reserved key is rejected",
			tags:    []string{reservedPrefix + "pool=other-pool"},
			wantErr: errReserved,
		},
		{
			name:    "a reserved key without a value is rejected",
			tags:    []string{reservedPrefix + "pool"},
			wantErr: errReserved,
		},
		{
			name:    "padding does not sneak a reserved tag through",
			tags:    []string{" " + reservedPrefix + "pool =other-pool"},
			wantErr: errReserved,
		},
		{
			name: "the prefix only counts in the key",
			tags: []string{"origin=" + reservedPrefix},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := utils.CheckReservedTags(tt.tags, reservedPrefix, errReserved)
			if tt.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tt.wantErr)
			require.Contains(t, err.Error(), reservedPrefix)
		})
	}
}
