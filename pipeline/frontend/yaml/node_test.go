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

package yaml

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
)

func parsePlain(t *testing.T, text string) PlainDoc {
	t.Helper()
	doc, ok := ParsePlain([]byte(text))
	require.True(t, ok, "%q is plain", text)
	return doc
}

func TestParsePlain(t *testing.T) {
	for text, want := range map[string]bool{
		sampleYaml:                           true,
		sampleDeepYaml:                       true,
		"steps: []\n":                        true,
		"":                                   false, // empty: no document to decode
		"steps: [\n":                         false, // broken: the text says what is wrong
		simpleYamlAnchors:                    false,
		sampleVarYaml:                        false,
		"a: &x [1]\nb: *x\n":                 false,
		"steps:\n  - <<: {name: a}\n":        false,
		"steps:\n  - \"<<\": {name: a}\n":    true, // a quoted key is a string
		"params: {P: {default: '${X}'}}\n":   true,
		"when:\n  - event: push\n    x: 1\n": true,
	} {
		_, ok := ParsePlain([]byte(text))
		assert.Equal(t, want, ok, "%q", text)
	}
}

// TestPlainDocZero: a PlainDoc ParsePlain did not make decodes nothing.
func TestPlainDocZero(t *testing.T) {
	var doc PlainDoc
	_, err := doc.Workflow()
	require.ErrorIs(t, err, errNoPlainDoc)
	_, err = doc.Substitute(nil)
	require.ErrorIs(t, err, errNoPlainDoc)
}

// TestPlainWorkflowAsParseBytes: a plain document decodes to what its text
// parses to, as often as decoded.
func TestPlainWorkflowAsParseBytes(t *testing.T) {
	for _, text := range []string{sampleYaml, sampleDeepYaml, "steps:\n  - name: a\n    image: alpine\n    commands: [\"yes\", '0777', 1:20]\n"} {
		want, err := ParseString(text)
		require.NoError(t, err)
		doc := parsePlain(t, text)
		for range 2 {
			got, err := doc.Workflow()
			require.NoError(t, err)
			assert.Equal(t, want, got)
		}
	}
}

// TestDecodeDepth: a plain document too deep for Unmarshal is too deep to
// decode, at the same depth, through mappings and sequences; checkDepth
// mirrors xyaml, and this shows if they part.
func TestDecodeDepth(t *testing.T) {
	for depth := 10; depth <= 18; depth++ {
		for _, nest := range [][2]string{{"{a: ", "}"}, {"[", "]"}} {
			text := strings.Repeat(nest[0], depth) + "x" + strings.Repeat(nest[1], depth) + "\n"
			var want, got any
			wantErr := Unmarshal([]byte(text), &want)
			gotErr := parsePlain(t, text).Decode(&got)
			assert.Equal(t, wantErr, gotErr, "depth %d of %q", depth, nest[0])
			assert.Equal(t, want, got, "depth %d of %q", depth, nest[0])
			// the document and the scalar inside count too
			assert.Equal(t, depth+2 > int(maxMergeDepth), gotErr != nil, "depth %d of %q", depth, nest[0])
		}
	}
}

func TestPlainParams(t *testing.T) {
	text := "params:\n  A: {default: 'a\\b ${X}', options: ['a\\b ${X}', z]}\n  B: {}\nsteps: []\n"
	want, err := ParseParams([]byte(text))
	require.NoError(t, err)
	require.Len(t, want, 2)
	got, err := parsePlain(t, text).Params()
	require.NoError(t, err)
	assert.Equal(t, want, got)

	_, wantErr := ParseParams([]byte("params:\n  CI_X: {}\n"))
	_, gotErr := parsePlain(t, "params:\n  CI_X: {}\n").Params()
	require.Error(t, gotErr)
	assert.Equal(t, wantErr, gotErr)
}

type substituted struct {
	Image    string            `yaml:"image"`
	Commands []string          `yaml:"commands"`
	Env      map[string]string `yaml:"environment"`
}

func decodeSubstituted(t *testing.T, doc PlainDoc) substituted {
	t.Helper()
	var w substituted
	require.NoError(t, doc.Decode(&w))
	return w
}

// TestSubstituteCopies: each substitution of one document is a copy of its
// own; none leaks into the document or into another copy, in either order,
// and a value that holds ${...} is substituted once, in its own copy.
func TestSubstituteCopies(t *testing.T) {
	const text = "image: golang:${GO}\ncommands:\n  - echo ${GO} ${OS}\n  - echo $${HOME} a\\nb\n  - plain\nenvironment:\n  GO: ${GO}\n  N: '1'\n"
	doc := parsePlain(t, text)
	before, err := yaml.Dump(doc.root)
	require.NoError(t, err)

	axes := []map[string]string{
		{"GO": "1.26", "OS": "${GO}"}, // a value with ${...} stays as it is
		{"GO": "1.27", "OS": "linux"},
		{},
	}
	want := []substituted{
		{"golang:1.26", []string{"echo 1.26 ${GO}", `echo ${HOME} a\nb`, "plain"}, map[string]string{"GO": "1.26", "N": "1"}},
		{"golang:1.27", []string{"echo 1.27 linux", `echo ${HOME} a\nb`, "plain"}, map[string]string{"GO": "1.27", "N": "1"}},
		{"golang:", []string{"echo  ", `echo ${HOME} a\nb`, "plain"}, map[string]string{"GO": "", "N": "1"}},
	}
	copies := make([]PlainDoc, len(axes))
	for _, order := range [][]int{{0, 1, 2}, {2, 1, 0}} {
		for _, i := range order {
			copies[i], err = doc.Substitute(axes[i])
			require.NoError(t, err)
		}
		for i := range axes {
			assert.Equal(t, want[i], decodeSubstituted(t, copies[i]), "axis %d", i)
		}
		after, err := yaml.Dump(doc.root)
		require.NoError(t, err)
		assert.Equal(t, string(before), string(after), "the document is as parsed")
	}

	// a copy changed afterwards changes no other
	copies[0].root.Content[0].Content[1].Value = "changed"
	assert.Equal(t, "golang:1.27", decodeSubstituted(t, copies[1]).Image)
	assert.Equal(t, "golang:${GO}", doc.root.Content[0].Content[1].Value)

	// what nothing was substituted into is shared, not copied
	commands := func(d PlainDoc) *yaml.Node { return d.root.Content[0].Content[3] }
	assert.NotSame(t, commands(doc), commands(copies[1]), "a list with a substituted command is copied")
	assert.Same(t, commands(doc).Content[2], commands(copies[1]).Content[2], "plain, untouched")
	unchanged := parsePlain(t, "steps: [{commands: [echo a]}]\n")
	again, err := unchanged.Substitute(axes[0])
	require.NoError(t, err)
	assert.Same(t, unchanged.root, again.root, "nothing to substitute: the document itself")
}

// TestSubstituteAsValues: a document substituted decodes as the text
// metadata.SubstituteValues makes of it, parsed again.
func TestSubstituteAsValues(t *testing.T) {
	const text = "timeout: ${T}\nflag: ${F}\ncommands:\n  - 'echo ${X} # ${M}'\n  - \"yes\"\n  - $$HOME\n"
	env := map[string]string{"T": "90", "F": "yes", "X": `v\al "q"`, "M": "fix: a #1\n- b"}

	text2, err := metadata.SubstituteValues(text, env)
	require.NoError(t, err)
	var want map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(text2), &want))

	doc, err := parsePlain(t, text).Substitute(env)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, doc.Decode(&got))
	assert.Equal(t, want, got)
	assert.Equal(t, "90", got["timeout"], "a substituted number is a string")
	assert.Equal(t, "yes", got["flag"])
}

func TestSubstituteError(t *testing.T) {
	_, err := parsePlain(t, "steps:\n  - commands: ['echo ${X']\n").Substitute(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"echo ${X"`)
}
