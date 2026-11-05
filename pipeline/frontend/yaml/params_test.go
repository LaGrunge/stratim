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

package yaml

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseParamsMergeBomb: anchors merged into one another cannot make
// params read without end.
func TestParseParamsMergeBomb(t *testing.T) {
	var b strings.Builder
	b.WriteString("variables:\n  - &m0 {A: x}\n")
	for i := 1; i <= 30; i++ {
		fmt.Fprintf(&b, "  - &s%d [*m%d, *m%d]\n  - &m%d {<<: *s%d}\n", i, i-1, i-1, i, i)
	}
	b.WriteString("params:\n  <<: *m30\n")
	start := time.Now()
	_, err := ParseParams([]byte(b.String()))
	assert.ErrorContains(t, err, "more than 1000 parameters")
	assert.Less(t, time.Since(start), time.Second)
}

// TestParseParamsValueBomb: one big value aliased by many parameters is
// counted for each.
func TestParseParamsValueBomb(t *testing.T) {
	var b strings.Builder
	b.WriteString("variables:\n  - &big\n    options: [")
	for i := range 900 {
		fmt.Fprintf(&b, "o%d, ", i)
	}
	b.WriteString("x]\nparams:\n")
	for i := range 900 {
		fmt.Fprintf(&b, "  P%d: *big\n", i)
	}
	start := time.Now()
	_, err := ParseParams([]byte(b.String()))
	assert.ErrorContains(t, err, "more than 1000 parameters and options")
	assert.Less(t, time.Since(start), time.Second)

	_, err = ParseParams([]byte("params:\n  A: '" + strings.Repeat("x", 70<<10) + "'\n"))
	assert.ErrorContains(t, err, "longer than")
}

func TestParseParamsSelfMerge(t *testing.T) {
	_, err := ParseParams([]byte("params: &p\n  <<: *p\n"))
	assert.Error(t, err)
}

// TestParseParamsEscapedReference: an escaped ${...} in a default reads no
// parameter.
func TestParseParamsEscapedReference(t *testing.T) {
	params, err := ParseParams([]byte("params:\n  A: '$${B}'\n  B: x\n"))
	require.NoError(t, err)
	assert.Len(t, params, 2)
}

// TestParseParamsNoMergeInside: a parameter cannot merge keys; whole
// parameters can be shared.
func TestParseParamsNoMergeInside(t *testing.T) {
	_, err := ParseParams([]byte("variables:\n  - &o {options: [x]}\nparams:\n  A: {<<: *o, default: x}\n"))
	assert.ErrorContains(t, err, "a parameter cannot merge keys")
}
