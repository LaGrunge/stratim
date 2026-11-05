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

package common

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadConfigFile(t *testing.T) {
	root := t.TempDir()
	write := func(name string) string {
		p := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte("{}"), 0o600))
		return p
	}
	write("lib.libsonnet")
	rootConfig := write(".woodpecker.jsonnet")
	write(".woodpecker/lib.libsonnet")
	folderConfig := write(".woodpecker/build.jsonnet")
	yamlConfig := write(".woodpecker/lint.yaml")

	names := func(file string) []string {
		files, err := ReadConfigFile(file)
		require.NoError(t, err)
		var out []string
		for _, f := range files {
			out = append(out, filepath.Base(f.Name))
		}
		return out
	}
	assert.Equal(t, []string{"build.jsonnet", "lib.libsonnet"}, names(folderConfig), "a config in a folder sees its libraries")
	assert.Equal(t, []string{".woodpecker.jsonnet"}, names(rootConfig), "the root config alone, as the server fetches it")
	assert.Equal(t, []string{"lint.yaml"}, names(yamlConfig))
}

func TestReadConfigDirTakesJsonnetFromTheFolderOnly(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a.yaml", "b.jsonnet", "lib.libsonnet", "sub/c.yaml", "sub/d.jsonnet", "sub/e.libsonnet", "notes.txt"} {
		p := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte("{}"), 0o600))
	}
	files, err := ReadConfigDir(root)
	require.NoError(t, err)
	var names []string
	for _, f := range files {
		rel, err := filepath.Rel(root, f.Name)
		require.NoError(t, err)
		names = append(names, filepath.ToSlash(rel))
	}
	assert.ElementsMatch(t, []string{"a.yaml", "b.jsonnet", "lib.libsonnet", "sub/c.yaml"}, names,
		"jsonnet in subfolders is left out, as the server does not fetch it; YAML is walked as before")
}
