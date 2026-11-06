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

package matrix

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml"
)

// TestParsePlainAsParse: the axes of a parsed document are those of its
// text, and so are its errors.
func TestParsePlainAsParse(t *testing.T) {
	deep := "matrix:\n  include: " + strings.Repeat("[", 14) + strings.Repeat("]", 14) + "\n"
	for _, text := range []string{fakeMatrix, fakeMatrixInclude, "steps: []\n", "matrix:\n", "matrix:\n  X: [a, '${Y}']\n", deep} {
		want, wantErr := parseString(text)
		doc, ok := yaml.ParsePlain([]byte(text))
		require.True(t, ok, "%q", text)
		got, gotErr := ParsePlain(doc)
		assert.Equal(t, wantErr, gotErr, "%q", text)
		assert.Equal(t, want, got, "%q", text)
	}
}
