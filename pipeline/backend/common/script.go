// Copyright 2022 Woodpecker Authors
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
	"encoding/base64"
	"slices"
)

// CommandMarker prefixes the echoed command line before each step command runs.
// The web UI (PipelineLog.vue) uses it to detect and collapse command output,
// and plugins are documented to print it too, so every backend must use it.
const CommandMarker = "▶  "

// posixEntrypoint runs CI_SCRIPT, base64, with /bin/sh -e. The script is
// decoded whole before it runs, not piped into the shell: a shell reads a
// pipe a buffer at a time, and a command reading stdin took the rest of a
// long script as its input, so the commands after it silently did not run.
// A script that does not decode fails the step. The "." keeps the newlines
// that end the script, which command substitution would strip, so that a
// last line ending in a backslash continues onto nothing, as it did piped.
var posixEntrypoint = []string{"/bin/sh", "-e", "-c", `CI_SCRIPT="$(echo "$CI_SCRIPT" | base64 -d && echo .)"; eval "${CI_SCRIPT%.}"`}

func GenerateContainerConf(commands []string, osType, workDir string) (env map[string]string, entry []string, err error) {
	env = make(map[string]string)
	if osType == "windows" {
		env["CI_SCRIPT"] = base64.StdEncoding.EncodeToString([]byte(generateScriptWindows(commands, workDir)))
		env["SHELL"] = "powershell.exe"
		// cspell:disable-next-line
		entry = []string{"powershell", "-noprofile", "-noninteractive", "-command", "[System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($Env:CI_SCRIPT)) | iex"}
	} else {
		env["CI_SCRIPT"] = base64.StdEncoding.EncodeToString([]byte(generateScriptPosix(commands, workDir)))
		env["SHELL"] = "/bin/sh"
		entry = slices.Clone(posixEntrypoint)
	}

	return env, entry, nil
}
