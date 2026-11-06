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
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/linter"
)

// checkWorkflow asserts that w parses and lints as a workflow.
func checkWorkflow(t *testing.T, w *File) {
	t.Helper()
	parsed, err := yaml.ParseBytes(w.Data)
	require.NoError(t, err, "%s parses as a workflow", w.Name)
	err = linter.New(linter.WithTrusted(linter.TrustedConfiguration{Network: true, Volumes: true, Security: true})).
		Lint([]*linter.WorkflowConfig{{File: path.Base(w.Name), RawConfig: string(w.Data), Workflow: parsed}})
	assert.False(t, pipeline_errors.HasBlockingErrors(err), "%s lints: %v", w.Name, err)
}
