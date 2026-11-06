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

package jsonnet

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
)

// commands a jsonnet config writes, each with what a step must get. The
// substitution after jsonnet treats them as it treats a YAML file's: ${X}
// and $$ are its own, everything else stays as written.
var commandCases = []struct{ name, command, want string }{
	{"a backslash escape for the command", `printf 'a\nb'`, `printf 'a\nb'`},
	{"awk with \\n in a double-quoted string", `awk 'BEGIN { printf "%09d x\n", 1 }'`, `awk 'BEGIN { printf "%09d x\n", 1 }'`},
	{"two backslashes", `sed 's/\\/\//g'`, `sed 's/\\/\//g'`},
	{"double quotes", `echo "a b" "c\"d"`, `echo "a b" "c\"d"`},
	{"single quotes", `echo 'it''s'`, `echo 'it''s'`},
	{"a tab and a backslash", "printf 'x\\ty'\t# tab", "printf 'x\\ty'\t# tab"},
	{"multi-line", "if true; then\n  echo \"x\\n\"\nfi\n", "if true; then\n  echo \"x\\n\"\nfi\n"},
	{"trailing spaces on a line", "echo a  \necho b \\\n  c", "echo a  \necho b \\\n  c"},
	{"YAML-looking", `key: value # not a comment`, `key: value # not a comment`},
	{"looks like a number", `123`, `123`},
	{"empty", ``, ``},
	{"unicode", `echo é ✓`, `echo é ✓`},
	{"a variable", `echo ${CI_COMMIT_SHA}`, `echo deadbeef`},
	{"an escaped dollar", `echo $${HOME}`, `echo ${HOME}`},
	{"a shell variable", `echo $HOME`, `echo $HOME`},
	{"a backslash in a default", `echo ${UNSET:-a\b}`, `echo a\b`},
	{"an escaped ${ with a backslash after", `echo $${X} \n`, `echo ${X} \n`},
}

// TestCommandsSurviveSubstitution: a step's command comes out of jsonnet,
// through the substitution the builder runs on the file's text, as the
// config wrote it. As JSON, every backslash was escaped and the
// substitution halved it: \n in a command became a line break.
func TestCommandsSurviveSubstitution(t *testing.T) {
	var commands []string
	for _, c := range commandCases {
		commands = append(commands, c.command)
	}
	source, err := json.Marshal(map[string]any{"steps": []map[string]any{{"name": "s", "image": "alpine", "commands": commands}}})
	require.NoError(t, err)

	out, err := Expand([]*File{{Name: ".woodpecker/w.jsonnet", Data: source}})
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.True(t, out[0].FromJsonnet())
	substituted, err := metadata.SubstituteValues(string(out[0].Data), map[string]string{"CI_COMMIT_SHA": "deadbeef"})
	require.NoError(t, err)

	var w struct {
		Steps []struct {
			Commands []string `yaml:"commands"`
		} `yaml:"steps"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(substituted), &w), substituted)
	require.Len(t, w.Steps, 1)
	require.Len(t, w.Steps[0].Commands, len(commandCases), substituted)
	for i, c := range commandCases {
		assert.Equal(t, c.want, w.Steps[0].Commands[i], c.name)
	}
}

// TestSubstitutedValueStaysAValue: a value substituted into a string of a
// jsonnet workflow stays in that string, whatever it holds: a YAML comment
// sign, a key, quotes, backslashes, line breaks.
func TestSubstitutedValueStaysAValue(t *testing.T) {
	out, err := Expand([]*File{{Name: ".woodpecker/w.jsonnet", Data: []byte(
		`{ steps: [{ name: 's', image: 'alpine', commands: ['echo ${MSG}', 'git log -1 # ${MSG}', 'printf "x\\ty" ${MSG} '], environment: { MSG: '${MSG}' } }] }`,
	)}})
	require.NoError(t, err)

	for _, msg := range []string{"fix #12 thing", "feat: a", `say "hi" to C:\temp\new`, "- not a list", "it's", "{x: 1}"} {
		substituted, err := metadata.SubstituteValues(string(out[0].Data), map[string]string{"MSG": msg})
		require.NoError(t, err)
		var w struct {
			Steps []struct {
				Commands    []string          `yaml:"commands"`
				Environment map[string]string `yaml:"environment"`
			} `yaml:"steps"`
		}
		require.NoError(t, yaml.Unmarshal([]byte(substituted), &w), "%q:\n%s", msg, substituted)
		require.Len(t, w.Steps, 1)
		assert.Equal(t, []string{"echo " + msg, "git log -1 # " + msg, `printf "x\ty" ` + msg + " "}, w.Steps[0].Commands, "%q", msg)
		assert.Equal(t, map[string]string{"MSG": msg}, w.Steps[0].Environment, "%q", msg)
	}

	multi := "fix: a\n\nbody"
	substituted, err := metadata.SubstituteValues(string(out[0].Data), map[string]string{"MSG": multi})
	require.NoError(t, err)
	var w struct {
		Steps []struct {
			Commands []string `yaml:"commands"`
		} `yaml:"steps"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(substituted), &w), substituted)
	assert.Equal(t, "echo "+multi, w.Steps[0].Commands[0], "line breaks and all")
	assert.Equal(t, `printf "x\ty" `+multi+" ", w.Steps[0].Commands[2], "in a string with a tab and a trailing space too")
}

// TestStringsStayStrings12: the strings TestStringsStayStrings checks for a
// YAML 1.1 reader are strings to the server's YAML 1.2 reader too.
func TestStringsStayStrings12(t *testing.T) {
	data := ambiguousStrings(t)
	var w struct {
		Steps []struct {
			Environment map[string]any `yaml:"environment"`
		} `yaml:"steps"`
	}
	require.NoError(t, yaml.Unmarshal(data, &w))
	for key, v := range ambiguousValues() {
		assert.Equal(t, v, w.Steps[0].Environment[key], "%q", v)
	}
}
