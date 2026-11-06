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

package base

import (
	"fmt"
	"strconv"
	"time"

	"go.yaml.in/yaml/v4"
)

// Timeout is a timeout in a config: a Go duration ("90s", "1h30m"), or a
// whole number of minutes, as the timeout of a repository is; longer than
// zero.
type Timeout time.Duration

// maxMinutes is the most minutes a time.Duration holds.
const maxMinutes = int64(1<<63-1) / int64(time.Minute)

// UnmarshalYAML implements the Unmarshaler interface.
func (d *Timeout) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: want a duration such as 90s or 1h30m, or minutes", value.Line)
	}
	var parsed time.Duration
	if minutes, err := strconv.ParseInt(value.Value, 10, 64); err == nil {
		if minutes > maxMinutes {
			return fmt.Errorf("line %d: %d minutes is too long", value.Line, minutes)
		}
		parsed = time.Duration(minutes) * time.Minute
	} else if parsed, err = time.ParseDuration(value.Value); err != nil {
		return fmt.Errorf("line %d: %q: want a duration such as 90s or 1h30m, or minutes", value.Line, value.Value)
	}
	if parsed <= 0 {
		return fmt.Errorf("line %d: a timeout must be longer than zero", value.Line)
	}
	*d = Timeout(parsed)
	return nil
}

// MarshalYAML implements the Marshaler interface.
func (d Timeout) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}
