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

package base

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
)

func TestTimeout(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"90s":    90 * time.Second,
		"1h30m":  90 * time.Minute,
		"30":     30 * time.Minute,
		"'45'":   45 * time.Minute,
		"500ms":  500 * time.Millisecond,
		"2h0m0s": 2 * time.Hour,
	} {
		t.Run(in, func(t *testing.T) {
			var d struct {
				D Timeout `yaml:"d"`
			}
			require.NoError(t, yaml.Unmarshal([]byte("d: "+in), &d))
			assert.Equal(t, want, time.Duration(d.D))
		})
	}
	for _, in := range []string{"0", "-5", "0s", "soon", "[1]", "1d", "153722867280912931", "9223372036854775807"} {
		t.Run(in, func(t *testing.T) {
			var d struct {
				D Timeout `yaml:"d"`
			}
			assert.Error(t, yaml.Unmarshal([]byte("d: "+in), &d))
		})
	}
}
