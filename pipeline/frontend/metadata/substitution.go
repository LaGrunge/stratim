// Copyright 2023 Woodpecker Authors
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
	"fmt"
	"regexp"
	"strings"

	"github.com/drone/envsubst"
)

func EnvVarSubst(yaml string, environ map[string]string) (string, error) {
	return envsubst.Eval(yaml, func(name string) string {
		env := environ[name]
		// Quote values containing a YAML line break so a user-controlled value
		// (commit message, branch name, PR title, …) cannot break out of its
		// scalar and inject new YAML structure. YAML 1.1 treats LF, CR, NEL,
		// LS and PS as line breaks, so all of them must trigger quoting.
		if strings.ContainsAny(env, "\n\r\u0085  ") {
			env = fmt.Sprintf("%q", env)
		}
		return env
	})
}

// Substitute substitutes environ into s, a value: ${NAME} and the string
// operations envsubst knows, the escapes $$, \\ and \/ outside of ${...}. A
// name not in environ is empty. Unlike EnvVarSubst, a value goes in as it
// is: s is not YAML text.
func Substitute(s string, environ map[string]string) (string, error) {
	if !strings.ContainsAny(s, `$\`) {
		return s, nil
	}
	return envsubst.Eval(s, func(name string) string { return environ[name] })
}

// Reference is a ${NAME...} expression in a string.
type Reference struct {
	Name string
	// HasDefault: the expression gives a default when NAME is empty or
	// unset (${NAME:-x}, ${NAME-x}, ${NAME:=x}, ${NAME=x}).
	HasDefault bool
}

// references finds ${NAME...} expressions, with the $ right before them:
// an odd number of those escapes the expression.
var references = regexp.MustCompile(`(\$*)\$\{#?([A-Za-z_][A-Za-z0-9_]*)(:?[-=])?`)

// References returns the ${NAME...} expressions s substitutes, leaving out
// those escaped as $${NAME}.
func References(s string) []Reference {
	var out []Reference
	for _, m := range references.FindAllStringSubmatch(s, -1) {
		if len(m[1])%2 == 1 { // an odd number of $ before: escaped
			continue
		}
		out = append(out, Reference{Name: m[2], HasDefault: m[3] != ""})
	}
	return out
}
