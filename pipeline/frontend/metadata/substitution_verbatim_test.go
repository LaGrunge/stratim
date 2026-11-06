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

package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"
)

// TestSubstituteVerbatim: backslashes come through as they are, outside
// ${...} and inside it; ${...} and $$ work as in Substitute.
func TestSubstituteVerbatim(t *testing.T) {
	env := map[string]string{"X": `v\al`, "B": `a\b`}
	for in, want := range map[string]string{
		`a\nb`:             `a\nb`,
		`a\\b \/`:          `a\\b \/`,
		`${X} \n`:          `v\al \n`,
		`${UNSET:-a\b}`:    `a\b`,
		`${UNSET:-${X}}\`:  `v\al\`,
		`$${X} \\`:         `${X} \\`,
		`${B/\\/x}`:        `axb`,
		`no escapes`:       `no escapes`,
		`${unterminated \`: `${unterminated \`,
	} {
		got, err := SubstituteVerbatim(in, env)
		if in == `${unterminated \` {
			// envsubst's own error, unchanged by the verbatim form
			_, plainErr := Substitute(in, env)
			assert.Equal(t, plainErr != nil, err != nil, in)
			continue
		}
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}

	// string by string: a value is never YAML
	got, err := SubstituteValues("steps:\n  - commands:\n      - 'echo ${X} # ${M}'\n      - plain\n      - $$HOME\n", map[string]string{"X": `v\al`, "M": "fix: a #1\n- b"})
	require.NoError(t, err)
	var w struct {
		Steps []struct {
			Commands []string `yaml:"commands"`
		} `yaml:"steps"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(got), &w), got)
	assert.Equal(t, []string{"echo v\\al # fix: a #1\n- b", "plain", "$HOME"}, w.Steps[0].Commands)

	// a value that looks like a number stays a string
	got, err = SubstituteValues("timeout: ${T}\n", map[string]string{"T": "90"})
	require.NoError(t, err)
	var v map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(got), &v))
	assert.Equal(t, "90", v["timeout"])

	got, err = SubstituteValues("", nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestSubstituteValuesError: a broken ${...} is reported with the string it
// is in, which the config shows, not a line of the YAML made from it.
func TestSubstituteValuesError(t *testing.T) {
	_, err := SubstituteValues("steps:\n  - commands: ['echo ${X']\n", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"echo ${X"`)

	text := "steps:\n  - commands: [echo hi]\n"
	got, err := SubstituteValues(text, nil)
	require.NoError(t, err)
	assert.Equal(t, text, got, "nothing to substitute: the text as it is")
}
