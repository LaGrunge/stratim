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

package constraint

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	yaml_base_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/types/base"
	"go.woodpecker-ci.org/woodpecker/v3/shared/optional"
)

// TestWithoutTrigger: each condition is either on what started the
// pipeline, and dropped, or on how the workflow runs, and kept. A condition
// added to Constraint fails here until it is sorted into one of them.
func TestWithoutTrigger(t *testing.T) {
	kept := map[string]bool{"Repo": true, "Instance": true, "Platform": true, "Status": true, "Matrix": true, "Local": true}
	dropped := map[string]bool{"Ref": true, "Branch": true, "Cron": true, "Path": true, "Evaluate": true, "Event": true}

	list := List{Include: []string{"x"}}
	full := Constraint{
		Ref:      list,
		Repo:     list,
		Instance: list,
		Platform: list,
		Branch:   list,
		Cron:     list,
		Status:   yaml_base_types.StringOrSlice{"failure"},
		Matrix:   Map{Include: map[string]string{"GO": "1"}},
		Local:    optional.Some(true),
		Path:     Path{Include: []string{"x"}},
		Evaluate: "A == 1",
		Event:    yaml_base_types.StringOrSlice{"push"},
	}
	got := When{Constraints: []Constraint{full, full}}.WithoutTrigger()
	require.Len(t, got.Constraints, 2)

	in, out := reflect.ValueOf(full), reflect.ValueOf(got.Constraints[1])
	for i := range in.NumField() {
		name := in.Type().Field(i).Name
		require.True(t, kept[name] != dropped[name], "condition %s is neither kept nor dropped", name)
		require.False(t, in.Field(i).IsZero(), "the test sets %s", name)
		if kept[name] {
			assert.Equal(t, in.Field(i).Interface(), out.Field(i).Interface(), "%s is kept", name)
		} else {
			assert.True(t, out.Field(i).IsZero(), "%s is dropped", name)
		}
	}
}
