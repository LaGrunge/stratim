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
	"go.yaml.in/yaml/v4"
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

// Substitution is how a workflow config is substituted: its text, and the
// values (a parameter's default and options) read from it before.
type Substitution struct {
	Text  func(text string, environ map[string]string) (string, error)
	Value func(value string, environ map[string]string) (string, error)
}

// SubstitutionFor is the substitution of a YAML file written by hand
// (EnvVarSubst, Substitute), or, verbatim, of a workflow whose backslashes
// are all as meant, such as one a jsonnet config evaluated to
// (SubstituteValues, SubstituteVerbatim).
func SubstitutionFor(verbatim bool) Substitution {
	if verbatim {
		return Substitution{Text: SubstituteValues, Value: SubstituteVerbatim}
	}
	return Substitution{Text: EnvVarSubst, Value: Substitute}
}

// SubstituteValues substitutes environ into every string of yamlText, a
// YAML document whose backslashes are all as meant, such as a workflow a
// jsonnet config evaluated to: string by string (SubstituteVerbatim), after
// parsing, so that a value (a commit message with ": ", " #", quotes or
// line breaks) is part of its string and never YAML, and every backslash
// comes through as it is. ${...} and $$ work as in EnvVarSubst.
func SubstituteValues(yamlText string, environ map[string]string) (string, error) {
	if !strings.Contains(yamlText, "$") {
		return yamlText, nil // nothing to substitute: no parsing twice
	}
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(yamlText), &doc); err != nil {
		return "", err
	}
	if doc.Kind == 0 {
		return yamlText, nil
	}
	if err := SubstituteNode(&doc, environ); err != nil {
		return "", err
	}
	out, err := yaml.Dump(&doc, yaml.WithIndent(yamlIndent), yaml.WithLineWidth(-1))
	return string(out), err
}

// maxQuoted is how much of a string an error quotes.
const maxQuoted = 80

func truncated(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// yamlIndent is how far SubstituteValues indents a nested block.
const yamlIndent = 2

// SubstituteNode substitutes environ, in place, into every string of n, a
// parsed YAML document or part of one, as SubstituteValues does: string by
// string, a substituted value a string whatever it looks like, a broken
// ${...} reported with its string. An alias is left to name its anchor,
// which is substituted where it is.
func SubstituteNode(n *yaml.Node, environ map[string]string) error {
	if n.Kind == yaml.ScalarNode && strings.Contains(n.Value, "$") {
		v, err := SubstituteVerbatim(n.Value, environ)
		if err != nil {
			return fmt.Errorf("%q: %w", truncated(n.Value, maxQuoted), err)
		}
		if v != n.Value {
			// a substituted value is a string, whatever it looks like
			n.Value, n.Tag, n.Style = v, "!!str", 0
		}
	}
	for _, c := range n.Content {
		if err := SubstituteNode(c, environ); err != nil {
			return err
		}
	}
	return nil
}

// SubstituteVerbatim is Substitute for a value whose backslashes are all
// as meant: envsubst reads \\ as an escaped backslash and \/ as a slash,
// which a YAML file written by hand has always been subject to; here every
// backslash comes through as it is.
func SubstituteVerbatim(s string, environ map[string]string) (string, error) {
	return Substitute(escapeBackslashes(s), environ)
}

// escapeBackslashes doubles every backslash outside ${...}, which envsubst
// then halves; inside an expression envsubst takes backslashes as they are.
// $$ is copied as it is, so that $${...} stays an escaped dollar.
func escapeBackslashes(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + strings.Count(s, `\`))
	for i := 0; i < len(s); {
		switch {
		case strings.HasPrefix(s[i:], escapedDollar):
			b.WriteString(escapedDollar)
			i += len(escapedDollar)
		case strings.HasPrefix(s[i:], exprOpen):
			end := expressionEnd(s, i+len(exprOpen))
			b.WriteString(s[i:end])
			i = end
		case s[i] == '\\':
			b.WriteString(`\\`)
			i++
		default:
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String()
}

const (
	escapedDollar = "$$"
	exprOpen      = "${"
)

// expressionEnd is the index past the } that closes the ${ opened before
// start, nested ${...} included, or len(s) if none does.
func expressionEnd(s string, start int) int {
	depth := 1
	for i := start; i < len(s); i++ {
		switch {
		case strings.HasPrefix(s[i:], exprOpen):
			depth++
			i++
		case s[i] == '}':
			if depth--; depth == 0 {
				return i + 1
			}
		}
	}
	return len(s)
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
