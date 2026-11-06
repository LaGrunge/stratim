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

package builder

import (
	"encoding/json"
	"errors"

	"go.uber.org/multierr"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/linter"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/matrix"
	yaml_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/types"
)

// workflowSource is a workflow file as the builder reads it for the axes
// of its matrix: a workflow of a jsonnet config parsed once (yaml.PlainDoc),
// each axis substituting into a copy of it, or else the text of the file,
// substituted and parsed for each axis, as a YAML file always was.
type workflowSource struct {
	file *YamlFile
	doc  yaml.PlainDoc
	// parsed: doc holds the file; a jsonnet workflow always is plain
	parsed bool
}

func newWorkflowSource(file *YamlFile) *workflowSource {
	s := &workflowSource{file: file}
	if file.FromJsonnet() {
		s.doc, s.parsed = yaml.ParsePlain(file.Data)
	}
	return s
}

// substitution is how the file is substituted: a jsonnet workflow string
// by string, its backslashes as the config wrote them.
func (s *workflowSource) substitution() metadata.Substitution {
	return metadata.SubstitutionFor(s.file.FromJsonnet())
}

// matrix returns the axes of the matrix the file declares.
func (s *workflowSource) matrix() ([]matrix.Axis, error) {
	if s.parsed {
		return matrix.ParsePlain(s.doc)
	}
	return matrix.Parse(s.file.Data)
}

// params returns the parameters the file declares, as written.
func (s *workflowSource) params() ([]yaml_types.NamedParam, error) {
	if s.parsed {
		return s.doc.Params()
	}
	return yaml.ParseParams(s.file.Data)
}

// workflow returns the file substituted with environ, parsed. The parsed
// document is substituted in a copy, for the next axis to have it as it
// was.
func (s *workflowSource) workflow(environ map[string]string) (*yaml_types.Workflow, error) {
	var parsed *yaml_types.Workflow
	if s.parsed {
		doc, err := s.doc.Substitute(environ)
		if err != nil {
			return nil, err
		}
		if parsed, err = doc.Workflow(); err != nil {
			return nil, compilerError(err)
		}
		return parsed, nil
	}
	text, err := s.substitution().Text(string(s.file.Data), environ)
	if err != nil {
		return nil, err
	}
	if parsed, err = yaml.ParseString(text); err != nil {
		return nil, compilerError(err)
	}
	return parsed, nil
}

func compilerError(err error) error {
	return &pipeline_errors.PipelineError{Message: err.Error(), Type: pipeline_errors.PipelineErrorTypeCompiler}
}

// buildState is what one Build keeps while it reads the workflow files,
// for the axes of a matrix to share: the sources of the files, what their
// raw text shows the linter, and the findings reported. A nil buildState
// keeps nothing.
type buildState struct {
	sources  map[*YamlFile]*workflowSource
	lint     *linter.Cache
	reported map[string]bool
}

func newBuildState() *buildState {
	return &buildState{
		sources:  make(map[*YamlFile]*workflowSource),
		lint:     linter.NewCache(),
		reported: make(map[string]bool),
	}
}

// source returns the source of file, read once per Build.
func (s *buildState) source(file *YamlFile) *workflowSource {
	if s == nil {
		return newWorkflowSource(file)
	}
	src, ok := s.sources[file]
	if !ok {
		src = newWorkflowSource(file)
		s.sources[file] = src
	}
	return src
}

// lintCache is the linter's cache, if any.
func (s *buildState) lintCache() *linter.Cache {
	if s == nil {
		return nil
	}
	return s.lint
}

// done drops what s keeps to read file, the workflow named workflow, once
// all its axes are read; what was reported of it is kept.
func (s *buildState) done(file *YamlFile, workflow string) {
	if s == nil {
		return
	}
	delete(s.sources, file)
	s.lint.Forget(workflow)
}

// reportOnce returns the findings of an axis of file that this Build has
// not reported for file yet: the axes of a matrix read one workflow, and
// what they find alike, such as the lint of its raw text, is reported
// once, with the first axis that is built or reports a finding.
func (s *buildState) reportOnce(file string, findings error) error {
	if s == nil {
		return findings
	}
	var out error
	for _, err := range multierr.Errors(findings) {
		key := file + "\x00" + findingKey(err)
		if s.reported[key] {
			continue
		}
		s.reported[key] = true
		out = multierr.Append(out, err)
	}
	return out
}

// findingKey tells a finding apart: what it says, of what, and its kind.
func findingKey(err error) string {
	if pe, ok := errors.AsType[*pipeline_errors.PipelineError](err); ok {
		if key, jsonErr := json.Marshal(pe); jsonErr == nil {
			return string(key)
		}
	}
	return err.Error()
}
