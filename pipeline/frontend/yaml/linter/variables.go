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

package linter

import (
	"fmt"

	"go.uber.org/multierr"
	go_yaml "go.yaml.in/yaml/v4"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/matrix"
)

// lintParams reports what is wrong with the parameters the config
// declares, as far as it shows without a run.
func (l *Linter) lintParams(config *WorkflowConfig) error {
	if _, err := yaml.ParseParams([]byte(config.RawConfig)); err != nil {
		return newLinterError(err.Error(), config.File, "params", false)
	}
	return nil
}

// lintCommandVariables warns of a ${NAME} in a command that the server will
// substitute with nothing: the server substitutes commands too, before the
// shell runs, and a name it does not know comes out empty, without an
// error. The shell's own variables are written $${NAME}.
//
// The names the server knows are the built-in variables it sets when it
// reads a config, the parameters the config declares, the axes of its
// matrix, and, when the linter was given them, the variables of the run and
// the server's global environment. An expression with a default
// (${NAME:-x}) is left alone: it does not come out empty.
func (l *Linter) lintCommandVariables(config *WorkflowConfig) error {
	raw := []byte(config.RawConfig)
	var doc go_yaml.Node
	if err := go_yaml.Unmarshal(raw, &doc); err != nil {
		return nil //nolint:nilerr // lintSchema reports it
	}
	declared := make(map[string]bool)
	if params, err := yaml.ParseParams(raw); err == nil {
		for _, p := range params {
			declared[p.Name] = true
		}
	}
	if axes, err := matrix.Parse(raw); err == nil {
		for _, axis := range axes {
			for name := range axis {
				declared[name] = true
			}
		}
	}
	known := func(name string) bool {
		if declared[name] || metadata.IsConfigName(name) {
			return true
		}
		_, ok := l.variables[name]
		return ok
	}

	var linterErr error
	warned := make(map[string]bool)
	for _, cmd := range commandScalars(&doc, false, make(map[visit]bool)) {
		for _, ref := range metadata.References(cmd) {
			if ref.HasDefault || warned[ref.Name] || known(ref.Name) {
				continue
			}
			warned[ref.Name] = true
			linterErr = multierr.Append(linterErr, newLinterError(
				fmt.Sprintf("${%s} in a command: the server substitutes it before the shell runs and does not know %s, so it comes out empty; write $${%s} for the shell's variable", ref.Name, ref.Name, ref.Name),
				config.File, "commands", true,
			))
		}
	}
	return linterErr
}

type visit struct {
	node    *go_yaml.Node
	command bool
}

// commandScalars returns the scalars under the commands keys under n, but
// for those of plugin settings, through aliases too.
func commandScalars(n *go_yaml.Node, command bool, seen map[visit]bool) []string {
	if seen[visit{n, command}] {
		return nil
	}
	seen[visit{n, command}] = true
	switch n.Kind {
	case go_yaml.ScalarNode:
		if command {
			return []string{n.Value}
		}
		return nil
	case go_yaml.AliasNode:
		if n.Alias == nil {
			return nil
		}
		return commandScalars(n.Alias, command, seen)
	case go_yaml.MappingNode:
		var out []string
		for i := 0; i+1 < len(n.Content); i += 2 {
			switch n.Content[i].Value {
			case "settings":
			case "commands":
				out = append(out, commandScalars(n.Content[i+1], true, seen)...)
			default:
				out = append(out, commandScalars(n.Content[i+1], command, seen)...)
			}
		}
		return out
	default:
		var out []string
		for _, c := range n.Content {
			out = append(out, commandScalars(c, command, seen)...)
		}
		return out
	}
}
