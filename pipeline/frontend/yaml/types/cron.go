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

package types

import (
	"fmt"

	"go.yaml.in/yaml/v4"
)

// CronNamePattern is what the name of a cron in its workflow matches. The
// linter's schema.json says the same; a test holds them together.
const CronNamePattern = `^[A-Za-z0-9][A-Za-z0-9._-]*$`

// Cron is a schedule a workflow declares for itself: it runs the workflow,
// and the workflows it depends on, on the repository's default branch.
type Cron struct {
	// Schedule is a cron expression: "0 3 * * *", "@daily".
	Schedule string `yaml:"schedule"`
	// Timezone the schedule is read in; UTC if empty.
	Timezone string `yaml:"timezone,omitempty"`
	// Variables the run is given, as a manual run's: parameters read them.
	Variables map[string]string `yaml:"variables,omitempty"`
}

// UnmarshalYAML reads a cron strictly: a misspelled key or a variable with no
// value is an error, not a cron that runs otherwise than meant.
func (c *Cron) UnmarshalYAML(value *yaml.Node) error {
	value = Resolve(value)
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: a cron is a mapping of schedule, timezone and variables", value.Line)
	}
	seen := make(map[string]bool, 3)
	for i := 0; i+1 < len(value.Content); i += 2 {
		key, v := value.Content[i], Resolve(value.Content[i+1])
		if seen[key.Value] {
			return fmt.Errorf("line %d: key %q already defined", key.Line, key.Value)
		}
		seen[key.Value] = true
		var err error
		switch key.Value {
		case "schedule":
			c.Schedule, err = scalar(v)
		case "timezone":
			if c.Timezone, err = scalar(v); err == nil && c.Timezone == "Local" {
				err = fmt.Errorf("line %d: timezone Local is the server's: name the timezone", v.Line)
			}
		case "variables":
			c.Variables, err = cronVariables(v)
		default:
			err = fmt.Errorf("line %d: unknown key %q: a cron has schedule, timezone and variables", key.Line, key.Value)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func cronVariables(n *yaml.Node) (map[string]string, error) {
	if n.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("line %d: variables: want a mapping of names to values", n.Line)
	}
	vars := make(map[string]string, len(n.Content)/2)
	for i := 0; i+1 < len(n.Content); i += 2 {
		name, v := n.Content[i].Value, Resolve(n.Content[i+1])
		if _, ok := vars[name]; ok {
			return nil, fmt.Errorf("line %d: variable %s already defined", n.Content[i].Line, name)
		}
		if v.Kind != yaml.ScalarNode || v.ShortTag() == "!!null" {
			return nil, fmt.Errorf("line %d: variable %s: want a string, number or boolean", v.Line, name)
		}
		vars[name] = v.Value
	}
	return vars, nil
}
