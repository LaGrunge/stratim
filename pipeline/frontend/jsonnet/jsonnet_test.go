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

package jsonnet

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

var update = flag.Bool("update", false, "rewrite the golden files of the corpus")

// TestCorpus expands each config set under testdata/corpus/<case>/.woodpecker
// and compares the workflows with testdata/corpus/<case>/golden/, as YAML.
// Every workflow must also parse and lint as one.
func TestCorpus(t *testing.T) {
	cases, err := os.ReadDir("testdata/corpus")
	require.NoError(t, err)
	for _, c := range cases {
		t.Run(c.Name(), func(t *testing.T) {
			dir := filepath.Join("testdata/corpus", c.Name())
			workflows, err := Expand(readConfigs(t, filepath.Join(dir, ".woodpecker")))
			require.NoError(t, err)
			require.NotEmpty(t, workflows)

			golden := filepath.Join(dir, "golden")
			if *update {
				require.NoError(t, os.RemoveAll(golden))
				require.NoError(t, os.MkdirAll(golden, 0o755))
			}
			var names []string
			for _, w := range workflows {
				name := path.Base(w.Name)
				names = append(names, name)
				got, err := yaml.JSONToYAML(w.Data)
				require.NoError(t, err, "%s as YAML", w.Name)

				if *update {
					require.NoError(t, os.WriteFile(filepath.Join(golden, name), got, 0o600))
				}
				want, err := os.ReadFile(filepath.Join(golden, name))
				require.NoError(t, err, "golden file of %s (go test -update writes them)", w.Name)
				assert.Equal(t, string(want), string(got), "%s against %s", w.Name, filepath.Join(golden, name))

				checkWorkflow(t, w)
			}

			goldenFiles, err := os.ReadDir(golden)
			require.NoError(t, err)
			var stale []string
			for _, g := range goldenFiles {
				if !slices.Contains(names, g.Name()) {
					stale = append(stale, g.Name())
				}
			}
			assert.Empty(t, stale, "golden files no config yields")
		})
	}
}

func readConfigs(t *testing.T, dir string) []*File {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var files []*File
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		files = append(files, &File{Name: ".woodpecker/" + e.Name(), Data: data})
	}
	return files
}

// files makes config files in .woodpecker/ from names and contents.
func files(contents map[string]string) []*File {
	var out []*File
	for name, data := range contents {
		out = append(out, &File{Name: ".woodpecker/" + name, Data: []byte(data)})
	}
	slices.SortFunc(out, func(a, b *File) int { return strings.Compare(a.Name, b.Name) })
	return out
}

func TestExpandKeepsOtherFiles(t *testing.T) {
	yamlFile := &File{Name: ".woodpecker/lint.yaml", Data: []byte("steps: []\n")}
	out, err := Expand([]*File{yamlFile, {Name: ".woodpecker/lib.libsonnet", Data: []byte("{}")}})
	require.NoError(t, err)
	assert.Equal(t, []*File{yamlFile}, out, "YAML passes as it is, libraries are left out")

	twins := []*File{{Name: ".woodpecker/a.yml", Data: []byte("steps: []\n")}, {Name: ".woodpecker/a.yaml", Data: []byte("steps: []\n")}}
	out, err = Expand(twins)
	require.NoError(t, err, "YAML files of the same name are left as they always were")
	assert.Len(t, out, 2)
}

func TestExpandSingleFileConfig(t *testing.T) {
	out, err := Expand([]*File{{Name: ".woodpecker.jsonnet", Data: []byte(`{steps: [{name: 'a', image: 'alpine'}]}`)}})
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "woodpecker", WorkflowName(out[0].Name), "named like .woodpecker.yaml")
}

func TestExpandErrors(t *testing.T) {
	step := `{name: 'a', image: 'alpine'}`
	tests := []struct {
		name  string
		files map[string]string
		want  string
	}{
		{name: "syntax", files: map[string]string{"a.jsonnet": `{steps: [}`}, want: "a.jsonnet"},
		{name: "runtime error", files: map[string]string{"a.jsonnet": `error 'no platforms'`}, want: "no platforms"},
		{name: "not an object", files: map[string]string{"a.jsonnet": `[1, 2]`}, want: "must evaluate to an object"},
		{name: "workflow not an object", files: map[string]string{"a.jsonnet": `{b: 1}`}, want: `workflow "b" is not an object`},
		{name: "workflow without steps", files: map[string]string{"a.jsonnet": `{b: {when: []}}`}, want: `workflow "b" has no list of steps`},
		{name: "steps not a list", files: map[string]string{"a.jsonnet": `{steps: 1}`}, want: `workflow "steps" is not an object`},
		{name: "a workflow named steps", files: map[string]string{"a.jsonnet": `{steps: {steps: [` + step + `]}, b: 1}`}, want: `workflow "b" is not an object`},
		{name: "nothing", files: map[string]string{"a.jsonnet": `{}`}, want: "declares no workflows"},
		{
			name:  "empty matrix",
			files: map[string]string{"a.jsonnet": `local s = import 'stratim.libsonnet'; { ['t-' + v.A]: {steps: []} for v in s.matrix({A: []}) }`},
			want:  "declares no workflows",
		},
		{name: "unicode workflow name", files: map[string]string{"a.jsonnet": `{'сборка': {steps: [` + step + `]}}`}, want: `workflow name "сборка"`},
		{name: "unicode file name", files: map[string]string{"сборка.jsonnet": `{steps: [` + step + `]}`}, want: `workflow name "сборка"`},
		{name: "bad workflow name", files: map[string]string{"a.jsonnet": `{'x/y': {steps: [` + step + `]}}`}, want: `workflow name "x/y"`},
		{name: "missing import", files: map[string]string{"a.jsonnet": `import 'nope.libsonnet'`}, want: "nope.libsonnet not found"},
		{name: "import outside", files: map[string]string{"a.jsonnet": `import '../secrets.libsonnet'`}, want: "not found"},
		{name: "file system", files: map[string]string{"a.jsonnet": `importstr '/etc/passwd'`}, want: "not found"},
		{
			name:  "same name twice",
			files: map[string]string{"a.jsonnet": `{b: {steps: [` + step + `]}}`, "b.yaml": "steps: []\n"},
			want:  `two workflows are named "b"`,
		},
		{
			name:  "same name as a YAML file, single workflow",
			files: map[string]string{"build.jsonnet": `{steps: [` + step + `]}`, "build.yml": "steps: []\n"},
			want:  `two workflows are named "build"`,
		},
		{
			name:  "same name as a hidden YAML file",
			files: map[string]string{"a.jsonnet": `{x: {steps: [` + step + `]}}`, ".x.yaml": "steps: []\n"},
			want:  `two workflows are named "x"`,
		},
		{
			name:  "same name from two configs",
			files: map[string]string{"a.jsonnet": `{c: {steps: [` + step + `]}}`, "b.jsonnet": `{c: {steps: [` + step + `]}}`},
			want:  `two workflows are named "c"`,
		},
		{name: "deep recursion", files: map[string]string{"a.jsonnet": `local f(n) = f(n + 1); f(0)`}, want: "max stack"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Expand(files(tt.files))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestExpandReportsEveryBrokenConfig(t *testing.T) {
	_, err := Expand(files(map[string]string{"a.jsonnet": `error 'first'`, "b.jsonnet": `error 'second'`}))
	var first *Error
	require.ErrorAs(t, err, &first)
	assert.Contains(t, err.Error(), "first")
	assert.Contains(t, err.Error(), "second")
	assert.True(t, errors.As(err, &first) && strings.HasSuffix(first.File, ".jsonnet"))
}

func TestLibraryMatrix(t *testing.T) {
	out, err := Expand([]*File{{Name: ".woodpecker/m.jsonnet", Data: []byte(`
local s = import 'stratim.libsonnet';
{ steps: [{ name: std.manifestJsonMinified(s.matrix({ A: [1, 2], B: ['x', 'y'] })), image: 'alpine' }] }`)}})
	require.NoError(t, err)
	assert.Contains(t, string(out[0].Data), `[{\"A\":1,\"B\":\"x\"},{\"A\":1,\"B\":\"y\"},{\"A\":2,\"B\":\"x\"},{\"A\":2,\"B\":\"y\"}]`)

	out, err = Expand([]*File{{Name: ".woodpecker/m.jsonnet", Data: []byte(`
local s = import 'stratim.libsonnet';
{ steps: [{ name: std.manifestJsonMinified(s.matrix({})), image: 'alpine' }] }`)}})
	require.NoError(t, err)
	assert.Contains(t, string(out[0].Data), `"name": "[{}]"`, "no axes: one empty combination")
}

func TestLibraryManualQuotesTheTask(t *testing.T) {
	out, err := Expand(files(map[string]string{"m.jsonnet": `
local s = import 'stratim.libsonnet';
{ steps: [{ name: 'a', image: 'alpine' }], when: [s.when.manual('a" || true || "\\b')] }`}))
	require.NoError(t, err)
	assert.Contains(t, string(out[0].Data), `"evaluate": "TASK == \"a\\\" || true || \\\"\\\\b\""`,
		"the task is one quoted string, not part of the expression")
}

func TestExpandPaths(t *testing.T) {
	lib := &File{Name: "./.woodpecker//lib.libsonnet", Data: []byte(`{ steps: [{ name: 'a', image: 'alpine' }] }`)}
	config := &File{Name: "./.woodpecker/a.jsonnet", Data: []byte(`import 'lib.libsonnet'`)}
	out, err := Expand([]*File{config, lib})
	require.NoError(t, err, "names are cleaned the same way for configs and imports")
	assert.Equal(t, ".woodpecker/a.yaml", out[0].Name)
}

func TestTraceIsNotLogged(t *testing.T) {
	out, err := Expand(files(map[string]string{"a.jsonnet": `std.trace('noise', { steps: [{ name: 'a', image: 'alpine' }] })`}))
	require.NoError(t, err)
	assert.Len(t, out, 1)
}

func TestWorkflowName(t *testing.T) {
	for file, want := range map[string]string{
		".woodpecker/build.yaml": "build",
		".woodpecker/build.yml":  "build",
		".woodpecker.yaml":       "woodpecker",
		"a/b/.x.yaml":            "x",
	} {
		assert.Equal(t, want, WorkflowName(file), file)
	}
}

func TestLibraryStep(t *testing.T) {
	out, err := Expand(files(map[string]string{"a.jsonnet": `
local s = import 'stratim.libsonnet';
s.workflow([s.step('plugin', 'plugins/s3'), s.step('cmd', 'alpine', ['true'], { environment: { A: '1' } })])`}))
	require.NoError(t, err)
	got, err := yaml.JSONToYAML(out[0].Data)
	require.NoError(t, err)
	assert.Equal(t, `steps:
- image: plugins/s3
  name: plugin
- commands:
  - "true"
  environment:
    A: "1"
  image: alpine
  name: cmd
`, string(got), "a step without commands has no commands key, as a plugin step")
}

func TestExpandLimits(t *testing.T) {
	many := make(map[string]string)
	for i := range maxConfigs + 1 {
		many[fmt.Sprintf("c%03d.jsonnet", i)] = "{ steps: [] }"
	}
	_, err := Expand(files(many))
	require.ErrorContains(t, err, "jsonnet configs, more than the 100 allowed")

	_, err = Expand(files(map[string]string{"a.jsonnet": "{ ['w' + i]: { steps: [] } for i in std.range(0, 500) }"}))
	require.ErrorContains(t, err, "more than the 500 workflows allowed")

	out, err := Expand(files(map[string]string{"a.jsonnet": "{ ['w' + i]: { steps: [] } for i in std.range(1, 500) }"}))
	require.NoError(t, err)
	assert.Len(t, out, 500, "the limit itself is allowed")
}

func TestExpandStopsAtTheFirstTimeout(t *testing.T) {
	var calls int
	_, err := expand(files(map[string]string{"a.jsonnet": "x", "b.jsonnet": "x", "c.jsonnet": "x"}), func(string, map[string]string) (string, error) {
		calls++
		return "", &transient{errors.New("evaluation took longer than 30s")}
	})
	require.ErrorContains(t, err, "took longer")
	assert.Equal(t, 1, calls, "a set of hanging configs takes one timeout")
}
