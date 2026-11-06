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

package jsonnet

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

// ambiguousValues are strings a YAML 1.1 reader would take, written plain,
// for a bool, a null or a number, by environment variable name.
func ambiguousValues() map[string]string {
	values := []string{"yes", "No", "on", "OFF", "y", "true", "null", "~", "", "0777", "0x1f", "1:20", "1_000", "1e3", ".inf", "3.14", "key: value", "plain"}
	out := make(map[string]string, len(values))
	for i, v := range values {
		out[fmt.Sprintf("V%02d", i)] = v
	}
	return out
}

// ambiguousStrings is the workflow a jsonnet config with ambiguousValues as
// a step's environment comes out as.
func ambiguousStrings(t *testing.T) []byte {
	t.Helper()
	source, err := json.Marshal(map[string]any{"steps": []map[string]any{{"name": "s", "image": "alpine", "environment": ambiguousValues()}}})
	require.NoError(t, err)
	out, err := Expand([]*File{{Name: ".woodpecker/w.jsonnet", Data: source}})
	require.NoError(t, err)
	return out[0].Data
}

// TestStringsStayStrings: such a string is quoted, so that a YAML 1.1
// reader (sigs.k8s.io/yaml here) sees the string the config wrote.
func TestStringsStayStrings(t *testing.T) {
	data := ambiguousStrings(t)
	var w struct {
		Steps []struct {
			Environment map[string]any
		}
	}
	require.NoError(t, yaml.Unmarshal(data, &w), string(data))
	for key, v := range ambiguousValues() {
		assert.Equal(t, v, w.Steps[0].Environment[key], "%q", v)
	}
}
