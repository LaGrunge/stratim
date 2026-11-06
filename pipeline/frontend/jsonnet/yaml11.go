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
	"strings"

	"go.yaml.in/yaml/v2"
)

// notPlainIn11 reports whether a YAML 1.1 reader takes s, written plain,
// for anything but the string s: yes, off, 0777, 1:20, an empty string.
// The server reads YAML 1.2; this is for the tools that read the workflows
// it writes with a 1.1 parser (sigs.k8s.io/yaml in tools/ciconfig, editors,
// linters a user runs on `cli lint` output).
func notPlainIn11(s string) bool {
	if strings.Contains(s, "\n") {
		return false // a literal block, never plain
	}
	var v any
	if err := yaml.Unmarshal([]byte(s), &v); err != nil {
		return true
	}
	str, ok := v.(string)
	return !ok || str != s
}
