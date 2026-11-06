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

//go:build !windows

package common

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runPosix runs commands the way a container does: posixEntrypoint with
// CI_SCRIPT in its environment, stdin empty.
func runPosix(t *testing.T, commands []string, extraEnv ...string) (string, error) {
	t.Helper()
	dir := t.TempDir()
	env, entry, err := GenerateContainerConf(commands, "linux", filepath.Join(dir, "work"))
	require.NoError(t, err)
	cmd := exec.Command(entry[0], entry[1:]...)
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "CI_SCRIPT=" + env["CI_SCRIPT"]}, extraEnv...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestPosixEntrypointRuns: a step runs every command, however long its
// script and whatever reads stdin; a failing command or a script that does
// not decode fails it.
func TestPosixEntrypointRuns(t *testing.T) {
	// a script far past what a shell reads of a pipe at once, with commands
	// that read stdin early in it
	commands := []string{"echo BEFORE", "cat", "read -r line || true"}
	for i := range 300 {
		commands = append(commands, fmt.Sprintf(": padding command %03d ......................................................", i))
	}
	commands = append(commands, "echo AFTER")
	out, err := runPosix(t, commands)
	require.NoError(t, err, out)
	assert.Contains(t, out, "BEFORE")
	assert.Contains(t, out, CommandMarker+"echo AFTER")
	assert.True(t, strings.HasSuffix(out, "AFTER\n"), "the last command ran: %q", out[max(0, len(out)-200):])

	// exit codes, and the text of commands as the shell reads it
	for _, tt := range []struct {
		name     string
		commands []string
		code     int
		want     string // the output, the commands echoed left out
		// match: "" the whole output; "prefix": a shell's error follows;
		// "suffix": the echo of the command is not its text (dash's echo
		// reads its backslashes)
		match string
	}{
		{"a failing command stops the step", []string{"echo one", "false", "echo NOTRUN"}, 1, "one\n", ""},
		{"its exit code is the step's", []string{"echo one", "exit 3", "echo NOTRUN"}, 3, "one\n", ""},
		{"exit 0 ends it early", []string{"echo one", "exit 0", "echo NOTRUN"}, 0, "one\n", ""},
		{"a late syntax error", []string{"echo FIRST", "if then", "echo NOTRUN"}, 2, "FIRST\n", "prefix"},
		{"a last line continued onto nothing", []string{"echo a \\"}, 0, "a\n", ""},
		{"multi-line", []string{"if true; then\n  echo ML\nfi"}, 0, "ML\n", ""},
		{"a heredoc of its own", []string{"cat <<'X'\nhello $foo \\\nX"}, 0, "hello $foo \\\n", ""},
		{"quotes, backslashes and $", []string{`foo=bar; printf '%s|\n' 'p\tq' "r\\s" x$foo "$foo"`}, 0, "p\\tq|\nr\\s|\nxbar|\nbar|\n", "suffix"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			out, err := runPosix(t, tt.commands)
			code := 0
			if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
				code = exitErr.ExitCode()
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.code, code, out)
			got := withoutCommands(out, tt.commands)
			switch tt.match {
			case "prefix":
				assert.True(t, strings.HasPrefix(got, tt.want), "%q", got)
				assert.NotContains(t, got, "NOTRUN")
			case "suffix":
				assert.True(t, strings.HasSuffix(got, "\n"+tt.want), "%q", got)
			default:
				assert.Equal(t, tt.want, got)
			}
		})
	}

	// the netrc the script writes with a heredoc
	out, err = runPosix(t, []string{`cat "$HOME/.netrc"`, `echo "user=${CI_NETRC_USERNAME:-unset} pw=${CI_NETRC_PASSWORD:-unset} script=${CI_SCRIPT-unset}"`},
		"CI_NETRC_MACHINE=git.example.test", "CI_NETRC_USERNAME=u", "CI_NETRC_PASSWORD=p")
	require.NoError(t, err, out)
	assert.Contains(t, out, "machine git.example.test\nlogin u\npassword p\n")
	assert.Contains(t, out, "user=unset pw=unset script=unset", "the credentials and the script leave the environment")
}

// TestPosixEntrypointBadScript: a CI_SCRIPT that does not decode fails the
// step, and none of it runs; piped into the shell, the part decoded before
// the error ran and the step passed. (GNU base64; busybox's decodes what it
// can and ignores trailing junk.)
func TestPosixEntrypointBadScript(t *testing.T) {
	cmd := exec.Command(posixEntrypoint[0], posixEntrypoint[1:]...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "CI_SCRIPT=ZWNobyBvaw==!"} // "echo ok", then junk
	out, err := cmd.CombinedOutput()
	assert.Error(t, err, string(out))
	assert.NotContains(t, string(out), "ok\n")
}

// withoutCommands drops the echo of each command from out.
func withoutCommands(out string, commands []string) string {
	for _, c := range commands {
		out = strings.Replace(out, CommandMarker+c+"\n", "", 1)
	}
	return out
}
