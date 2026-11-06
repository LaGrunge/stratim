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

package types

import (
	"fmt"

	"go.yaml.in/yaml/v4"
)

// Param is a parameter of a workflow: a variable with a default, which a
// manual run, a cron job or the API can set.
type Param struct {
	Default     string `yaml:"default,omitempty"`
	Description string `yaml:"description,omitempty"`
	// Options are the values allowed, if set.
	Options []string `yaml:"options,omitempty"`
}

// UnmarshalYAML takes a parameter as a mapping, or as its default alone.
// Numbers and booleans are taken as written, null as empty: a run gives
// strings.
func (p *Param) UnmarshalYAML(value *yaml.Node) error {
	value = Resolve(value)
	if value.Kind != yaml.MappingNode {
		v, err := scalar(value)
		p.Default = v
		return err
	}
	for i := 0; i+1 < len(value.Content); i += 2 {
		key, v := value.Content[i], value.Content[i+1]
		var err error
		switch key.Value {
		case "default":
			p.Default, err = scalar(v)
		case "description":
			p.Description, err = scalar(v)
		case "options":
			p.Options, err = scalars(v)
		case "<<":
			err = fmt.Errorf("line %d: a parameter cannot merge keys; share whole parameters with params: {<<: *anchor}", key.Line)
		default:
			err = fmt.Errorf("line %d: unknown key %q: a parameter has default, description and options", key.Line, key.Value)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// scalars is a list of scalars; an empty list is not nil.
func scalars(n *yaml.Node) ([]string, error) {
	n = Resolve(n)
	if n.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("line %d: options: want a list", n.Line)
	}
	out := make([]string, 0, len(n.Content))
	for _, item := range n.Content {
		v, err := scalar(item)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// scalar is a string, number or boolean as written; null is empty.
func scalar(n *yaml.Node) (string, error) {
	n = Resolve(n)
	if n.Kind != yaml.ScalarNode {
		return "", fmt.Errorf("line %d: want a string, number or boolean", n.Line)
	}
	if n.ShortTag() == "!!null" {
		return "", nil
	}
	return n.Value, nil
}

// NamedParam is a parameter with its name, in the order declared.
type NamedParam struct {
	Name string
	Param
}

// Resolve follows aliases to the node they name.
func Resolve(n *yaml.Node) *yaml.Node {
	for n.Kind == yaml.AliasNode && n.Alias != nil {
		n = n.Alias
	}
	return n
}
