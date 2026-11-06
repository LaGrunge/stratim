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
	"fmt"
	"regexp"
	"slices"
	"strings"

	"go.yaml.in/yaml/v4"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/types"
)

// ParseParams returns the parameters a workflow config declares, in the
// order declared, as written: nothing is substituted.
func ParseParams(raw []byte) ([]types.NamedParam, error) {
	var config struct {
		Params yaml.Node `yaml:"params"`
	}
	if err := Unmarshal(raw, &config); err != nil {
		return nil, err
	}
	declared := types.Resolve(&config.Params)
	if declared.Kind == 0 || declared.ShortTag() == "!!null" {
		return nil, nil
	}
	budget := maxParamEntries
	pairs, err := mappingPairs(declared, make(map[*yaml.Node]bool), &budget)
	if err != nil {
		return nil, fmt.Errorf("params: %w", err)
	}
	type entry struct {
		types.NamedParam
		merged bool
	}
	params := make([]entry, 0, len(pairs))
	index := make(map[string]int, len(pairs))
	decoded := make(map[*yaml.Node]types.Param) // a value aliased by many is read once
	size := 0
	for _, pair := range pairs {
		name := pair.key.Value
		param := entry{types.NamedParam{Name: name}, false}
		value := types.Resolve(pair.value)
		if p, ok := decoded[value]; ok {
			param.Param = p
		} else {
			if err := value.Decode(&param.Param); err != nil {
				return nil, fmt.Errorf("parameter %s: %w", name, err)
			}
			decoded[value] = param.Param
		}
		// every parameter costs its options and text, aliased or not: they
		// are substituted and compared for each
		if budget -= len(param.Options); budget < 0 {
			return nil, fmt.Errorf("params: more than %d parameters and options", maxParamEntries)
		}
		size += len(param.Default)
		for _, o := range param.Options {
			size += len(o)
		}
		if size > maxParamBytes {
			return nil, fmt.Errorf("params: defaults and options longer than %d bytes", maxParamBytes)
		}
		i, seen := index[name]
		switch {
		case !seen:
			param.merged = pair.merged
			index[name] = len(params)
			params = append(params, param)
		case pair.merged:
			// a key of the mapping itself overrides a merged one
		case params[i].merged:
			params[i] = param
		default:
			return nil, fmt.Errorf("params: line %d: parameter %s is declared twice", pair.key.Line, name)
		}
	}
	out := make([]types.NamedParam, len(params))
	for i, p := range params {
		out[i] = p.NamedParam
	}
	return out, checkParams(out)
}

var paramName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// maxParamEntries bounds the mappings and entries params reads, merge keys
// included: anchors merged into one another can multiply them without end.
const maxParamEntries = 1000

// maxParamBytes bounds the text of the defaults and options of params.
const maxParamBytes = 64 << 10

// ReservedParams are names a parameter cannot take, in any case, besides
// CI_*: the server's, and those every step's shell relies on.
var ReservedParams = []string{"CI", "PATH", "HOME", "SHELL", "PWD"}

// checkParams checks what can be checked of params without a run: names,
// that a default or option uses no parameter declared after it, and that
// options are not empty.
func checkParams(params []types.NamedParam) error {
	declaredAt := make(map[string]int, len(params))
	for i, p := range params {
		declaredAt[p.Name] = i
	}
	for i, p := range params {
		upper := strings.ToUpper(p.Name)
		switch {
		case !paramName.MatchString(p.Name):
			return fmt.Errorf("parameter %q: a name is letters, digits and _, and does not start with a digit", p.Name)
		case strings.HasPrefix(upper, "CI_") || slices.Contains(ReservedParams, upper):
			return fmt.Errorf("parameter %s: the name is reserved (CI_*, %s, in any case)", p.Name, strings.Join(ReservedParams, ", "))
		case p.Options != nil && len(p.Options) == 0:
			return fmt.Errorf("parameter %s: options is empty: list the values allowed, or leave options out", p.Name)
		}
		for _, text := range append([]string{p.Default}, p.Options...) {
			for _, ref := range metadata.References(text) {
				if j, ok := declaredAt[ref.Name]; ok && j > i {
					return fmt.Errorf("parameter %s: it uses %s, which is declared after it", p.Name, ref.Name)
				}
			}
		}
	}
	return nil
}

// pair is a key and its value; merged: it came from a merge key.
type pair struct {
	key, value *yaml.Node
	merged     bool
}

// mappingPairs returns the key-value pairs of a mapping, in order, those of
// merge keys (<<: *anchor) where they are.
// A mapping that merges itself, through any number of anchors, is an error,
// and so are more than budget mappings and entries.
func mappingPairs(n *yaml.Node, reading map[*yaml.Node]bool, budget *int) ([]pair, error) {
	if n.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("line %d: want a mapping of names to parameters", n.Line)
	}
	if *budget -= 1 + len(n.Content)/2; *budget < 0 { //nolint:mnd // key and value
		return nil, fmt.Errorf("more than %d parameters, mappings and merges counted", maxParamEntries)
	}
	if reading[n] {
		return nil, fmt.Errorf("line %d: the mapping merges itself", n.Line)
	}
	reading[n] = true
	defer delete(reading, n)
	var pairs []pair
	for i := 0; i+1 < len(n.Content); i += 2 {
		key, value := n.Content[i], n.Content[i+1]
		if key.Value != "<<" || key.Style != 0 {
			pairs = append(pairs, pair{key: key, value: value})
			continue
		}
		merged := []*yaml.Node{value}
		if value = types.Resolve(value); value.Kind == yaml.SequenceNode {
			merged = value.Content
		}
		for _, m := range merged {
			inner, err := mappingPairs(types.Resolve(m), reading, budget)
			if err != nil {
				return nil, err
			}
			for _, p := range inner {
				p.merged = true
				pairs = append(pairs, p)
			}
		}
	}
	return pairs, nil
}
