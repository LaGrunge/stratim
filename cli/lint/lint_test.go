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

package lint

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestLintPrefersWoodpeckerDirectory(t *testing.T) {
	repoRoot := t.TempDir()

	writeFile(t, filepath.Join(repoRoot, ".woodpecker", "pipeline.yaml"), `steps:
  test:
    image: alpine
    commands:
      - echo ok
`)
	writeFile(t, filepath.Join(repoRoot, "compose.yaml"), `services:
  database:
    image: postgres
`)

	runLintCommand(t, repoRoot, func(c *cli.Command) {
		require.NoError(t, lint(t.Context(), c))
	})
}

func TestLintWithFileConfig(t *testing.T) {
	repoRoot := t.TempDir()

	writeFile(t, filepath.Join(repoRoot, ".woodpecker.yml"), `steps:
  test:
    image: alpine
    commands:
      - echo ok
`)

	runLintCommand(t, repoRoot, func(c *cli.Command) {
		require.NoError(t, lint(t.Context(), c))
	})
}

func TestLintWalksAllYAMLWhenNoConfig(t *testing.T) {
	repoRoot := t.TempDir()

	writeFile(t, filepath.Join(repoRoot, "pipeline-a.yaml"), `steps:
  test:
    image: alpine
    commands:
      - echo a
`)
	writeFile(t, filepath.Join(repoRoot, "pipeline-b.yml"), `steps:
  test:
    image: alpine
    commands:
      - echo b
`)

	runLintCommand(t, repoRoot, func(c *cli.Command) {
		require.NoError(t, lint(t.Context(), c))
	})
}

func runLintCommand(t *testing.T, dir string, fn func(c *cli.Command)) {
	t.Helper()

	command := &cli.Command{
		Flags: Command.Flags,
		Action: func(_ context.Context, c *cli.Command) error {
			fn(c)
			return nil
		},
	}
	require.NoError(t, command.Run(t.Context(), []string{"woodpecker-cli", dir}))
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

const jsonnetLibrary = `local s = import 'stratim.libsonnet';
{ test(v):: s.workflow([s.step('test', 'golang:' + v, ['go test ./...'])]) }
`

func TestLintJsonnetDirectory(t *testing.T) {
	repoRoot := t.TempDir()
	writeFile(t, filepath.Join(repoRoot, ".woodpecker", "lint.yaml"), "steps:\n  - name: lint\n    image: alpine\n    commands: [echo ok]\n")
	writeFile(t, filepath.Join(repoRoot, ".woodpecker", "lib.libsonnet"), jsonnetLibrary)
	writeFile(t, filepath.Join(repoRoot, ".woodpecker", "test.jsonnet"), "local lib = import 'lib.libsonnet';\n{ ['test-' + v]: lib.test(v) for v in ['1.26', '1.27'] }\n")

	runLintCommand(t, filepath.Join(repoRoot, ".woodpecker"), func(c *cli.Command) {
		require.NoError(t, lint(t.Context(), c))
	})
}

func TestLintJsonnetFileWithItsLibrary(t *testing.T) {
	repoRoot := t.TempDir()
	writeFile(t, filepath.Join(repoRoot, ".woodpecker", "lib.libsonnet"), jsonnetLibrary)
	writeFile(t, filepath.Join(repoRoot, ".woodpecker", "test.jsonnet"), "(import 'lib.libsonnet').test('1.27')\n")

	runLintCommand(t, filepath.Join(repoRoot, ".woodpecker", "test.jsonnet"), func(c *cli.Command) {
		require.NoError(t, lint(t.Context(), c))
	})
}

func TestLintJsonnetErrors(t *testing.T) {
	repoRoot := t.TempDir()
	writeFile(t, filepath.Join(repoRoot, ".woodpecker", "broken.jsonnet"), "error 'no platforms given'\n")
	writeFile(t, filepath.Join(repoRoot, ".woodpecker", "invalid.jsonnet"), "{ steps: [{ name: 'a' }] }\n")

	runLintCommand(t, filepath.Join(repoRoot, ".woodpecker", "broken.jsonnet"), func(c *cli.Command) {
		err := lint(t.Context(), c)
		require.ErrorContains(t, err, "no platforms given")
		require.ErrorContains(t, err, "broken.jsonnet")
	})
	runLintCommand(t, filepath.Join(repoRoot, ".woodpecker", "invalid.jsonnet"), func(c *cli.Command) {
		require.Error(t, lint(t.Context(), c), "a step without an image is a lint error, as in YAML")
	})
}
