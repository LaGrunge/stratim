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

package builder

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/matrix"
	yaml_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/types"
)

// workflowConfig is a workflow config read for a run.
type workflowConfig struct {
	parsed *yaml_types.Workflow
	// environ is what the config was substituted with, its parameters
	// included: what when sees
	environ map[string]string
	// params are the values of the parameters it declares
	params map[string]string
	// invalidParams is a value outside a parameter's options, which
	// matters only if the workflow runs
	invalidParams error
}

// parseWorkflow is how a workflow config is read for a run: the variables
// of the run and its parameters resolved, the config substituted with them
// and parsed.
func (b *PipelineBuilder) parseWorkflow(data []byte, axis matrix.Axis, md metadata.Metadata) (*workflowConfig, error) {
	environ := b.environmentVariables(md, axis)
	params, invalid, err := b.params(data, axis, environ)
	if err != nil {
		return nil, err
	}
	// over the server's global environment, so that substitution, when and
	// the steps see one value
	maps.Copy(environ, params)
	substituted, err := metadata.EnvVarSubst(string(data), environ)
	if err != nil {
		return nil, err
	}
	parsed, err := yaml.ParseString(substituted)
	if err != nil {
		return nil, &pipeline_errors.PipelineError{Message: err.Error(), Type: pipeline_errors.PipelineErrorTypeCompiler}
	}
	// a step that set a parameter's name would see one value in its
	// environment and the server's in its substituted commands
	for _, list := range []yaml_types.ContainerList{parsed.Clone, parsed.Steps, parsed.Services} {
		for _, c := range list.ContainerList {
			for name := range c.Environment {
				if _, ok := params[name]; ok {
					return nil, paramError("step %s: environment %s: %s is a parameter of the workflow; give the variable another name", c.Name, name, name)
				}
			}
		}
	}
	return &workflowConfig{parsed: parsed, environ: environ, params: params, invalidParams: invalid}, nil
}

// params returns the values of the parameters the workflow config
// declares, resolved in the order declared: the value the run was given (a
// manual run, a cron job, the API, a restart or deployment) unless empty, or else the default, with environ and the parameters before
// it substituted into it. A value outside a parameter's options comes back
// as invalid, apart from err.
func (b *PipelineBuilder) params(data []byte, axis matrix.Axis, environ map[string]string) (values map[string]string, invalid, err error) {
	declared, err := yaml.ParseParams(data)
	if err != nil {
		return nil, nil, paramError("%v", err)
	}
	if len(declared) == 0 {
		return nil, nil, nil
	}

	given := b.AdditionalEnvs
	env := maps.Clone(environ)
	values = make(map[string]string, len(declared))
	for _, param := range declared {
		name := param.Name
		if _, ok := axis[name]; ok {
			return nil, nil, paramError("parameter %s: the name is a matrix axis too", name)
		}
		value := given[name]
		if value == "" {
			if value, err = metadata.Substitute(param.Default, env); err != nil {
				return nil, nil, paramError("parameter %s: default: %v", name, err)
			}
		}
		if param.Options != nil {
			options := make([]string, len(param.Options))
			for k, o := range param.Options {
				if options[k], err = metadata.Substitute(o, env); err != nil {
					return nil, nil, paramError("parameter %s: options: %v", name, err)
				}
			}
			if !slices.Contains(options, value) && invalid == nil {
				invalid = paramError("parameter %s: %q is not one of %s", name, value, strings.Join(options, ", "))
			}
		}
		values[name] = value
		env[name] = value
	}
	return values, invalid, nil
}

func paramError(format string, args ...any) error {
	return &pipeline_errors.PipelineError{Message: fmt.Sprintf(format, args...), Type: pipeline_errors.PipelineErrorTypeCompiler}
}
