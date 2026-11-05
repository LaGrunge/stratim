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

// Package jsonnet turns pipeline configs written in jsonnet into workflows.
//
// A config file name.jsonnet evaluates to either
//
//   - one workflow, an object with steps (a list, or an object of steps by
//     name), named after the file (name); or
//   - an object of workflows by name: { build: {steps: [...]}, test: {...} }.
//
// Files ending in .libsonnet are libraries: imported, never run. A config
// imports stratim.libsonnet, which this package provides, and other files
// given with it, by a path relative to it; nothing else is readable.
//
// The workflows come out as JSON, which is YAML, so everything after this
// step (substitution, parsing, linting) treats them like YAML files.
package jsonnet

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/google/go-jsonnet"
)

const (
	// Ext marks a config written in jsonnet.
	Ext = ".jsonnet"
	// LibExt marks a jsonnet library.
	LibExt = ".libsonnet"
	// LibraryName is the name configs import the Stratim library by.
	LibraryName = "stratim.libsonnet"
	// VarsName is what the library imports the variables of the run as.
	VarsName = "stratim.vars"

	// The deepest jsonnet calls may nest.
	maxStack = 500
	// The most jsonnet configs in one set, and workflows they may yield.
	maxConfigs   = 100
	maxWorkflows = 500
)

// maxOutput is the most JSON one config may evaluate to, in bytes.
var maxOutput = 16 << 20

// Library is stratim.libsonnet, as configs import it.
//
//go:embed stratim.libsonnet
var Library string

// workflowName is what a workflow may be called: it becomes a file name and
// is referred to in depends_on.
var workflowName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// File is a config file, by its path in the repository.
type File struct {
	Name string
	Data []byte
}

// IsConfig reports whether name is a config written in jsonnet.
func IsConfig(name string) bool { return strings.HasSuffix(name, Ext) }

// IsLibrary reports whether name is a jsonnet library.
func IsLibrary(name string) bool { return strings.HasSuffix(name, LibExt) }

// WorkflowName is the name of the workflow a config file holds: the file
// name without its extension or a leading dot (.woodpecker.yaml is
// "woodpecker").
func WorkflowName(file string) string {
	name := path.Base(filepath.ToSlash(file))
	name = strings.TrimSuffix(name, ".yml")
	name = strings.TrimSuffix(name, ".yaml")
	return strings.TrimPrefix(name, ".")
}

// Expand returns files with every jsonnet config replaced by the workflows it
// evaluates to, and every library left out; other files are kept as they
// are. A workflow comes out as a file named after it, next to its config,
// with a .yaml extension, so that it is named the way YAML files are.
//
// Expand evaluates with the Default renderer.
func Expand(files []*File) ([]*File, error) {
	return Default.Expand(files)
}

// ExpandWith is Expand for a run given vars: the variables of a manual
// run, a cron job, the API, a restart or deployment, which configs read as
// (import 'stratim.libsonnet').vars.
func ExpandWith(files []*File, vars map[string]string) ([]*File, error) {
	return Default.ExpandWith(files, vars)
}

// Error is a config that could not be turned into workflows.
type Error struct {
	File string
	Err  error
}

func (e *Error) Error() string { return e.File + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

// expand does Expand with evaluate. It gives up on the set at the first
// config that fails for reasons of the moment (time, memory), so that a set
// of hanging configs takes one timeout, not one each.
func expand(files []*File, vars map[string]string, evaluate evaluator) ([]*File, error) {
	sources := map[string]string{VarsName: varsJSON(vars)}
	configs := 0
	for _, f := range files {
		if IsConfig(f.Name) || IsLibrary(f.Name) {
			sources[clean(f.Name)] = string(f.Data)
		}
		if IsConfig(f.Name) {
			configs++
		}
	}
	if configs > maxConfigs {
		return nil, fmt.Errorf("%d jsonnet configs, more than the %d allowed", configs, maxConfigs)
	}

	var out []*File
	var errs []error
	rendered := make(map[string]bool)
	for _, f := range files {
		switch {
		case IsLibrary(f.Name):
			continue
		case !IsConfig(f.Name):
			out = append(out, f)
			continue
		}
		json, err := evaluate(clean(f.Name), sources)
		if err == nil {
			var workflows []*File
			if workflows, err = split(clean(f.Name), []byte(json)); err == nil {
				for _, w := range workflows {
					rendered[w.Name] = true
				}
				if len(rendered) > maxWorkflows {
					return nil, fmt.Errorf("jsonnet configs yield more than the %d workflows allowed", maxWorkflows)
				}
				out = append(out, workflows...)
				continue
			}
		}
		errs = append(errs, &Error{File: f.Name, Err: err})
		if isTransient(err) {
			break
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return out, duplicates(out, rendered)
}

// split turns what a config evaluated to into workflow files.
func split(config string, data []byte) ([]*File, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return nil, errors.New("must evaluate to an object: a workflow, or workflows by name")
	}
	file := func(name string, workflow []byte) (*File, error) {
		if !workflowName.MatchString(name) {
			return nil, fmt.Errorf("workflow name %q: use letters, digits, '.', '_' and '-', starting with a letter or digit", name)
		}
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, workflow, "", "  "); err != nil {
			return nil, err
		}
		return &File{Name: path.Join(path.Dir(config), name+".yaml"), Data: pretty.Bytes()}, nil
	}

	if areSteps(fields["steps"]) {
		f, err := file(WorkflowName(strings.TrimSuffix(config, Ext)), data)
		if err != nil {
			return nil, err
		}
		return []*File{f}, nil
	}

	if len(fields) == 0 {
		return nil, errors.New("declares no workflows")
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	slices.Sort(names)
	workflows := make([]*File, 0, len(names))
	for _, name := range names {
		var workflow map[string]json.RawMessage
		if err := json.Unmarshal(fields[name], &workflow); err != nil || workflow == nil {
			return nil, fmt.Errorf("workflow %q is not an object", name)
		}
		if !areSteps(workflow["steps"]) {
			return nil, fmt.Errorf("workflow %q has no steps", name)
		}
		f, err := file(name, fields[name])
		if err != nil {
			return nil, err
		}
		workflows = append(workflows, f)
	}
	return workflows, nil
}

// areSteps reports whether v is the steps of a workflow: a list of steps, or
// an object of steps by name, none of which has steps itself (that would be
// an object of workflows, one of them named "steps").
func areSteps(v json.RawMessage) bool {
	v = bytes.TrimSpace(v)
	if len(v) == 0 {
		return false
	}
	if v[0] == '[' {
		return true
	}
	var byName map[string]map[string]json.RawMessage
	if json.Unmarshal(v, &byName) != nil || byName == nil {
		return false
	}
	for _, step := range byName {
		if _, ok := step["steps"]; ok {
			return false
		}
	}
	return true
}

// duplicates reports a workflow from jsonnet that gets the same name as
// another. YAML files among themselves are left as they always were.
func duplicates(files []*File, rendered map[string]bool) error {
	seen := make(map[string]string, len(files))
	var errs []error
	for _, f := range files {
		name := WorkflowName(f.Name)
		if other, ok := seen[name]; ok && (rendered[f.Name] || rendered[other]) {
			errs = append(errs, fmt.Errorf("two workflows are named %q: %s and %s", name, other, f.Name))
			continue
		}
		seen[name] = f.Name
	}
	return errors.Join(errs...)
}

// evaluateHere evaluates config among sources in this process: the VM,
// the importer and the limits every evaluator shares.
func evaluateHere(config string, sources map[string]string) (string, error) {
	vm := jsonnet.MakeVM()
	vm.Importer(newImporter(sources))
	vm.MaxStack = maxStack
	vm.SetTraceOut(io.Discard) // std.trace is the config author's, not ours to log
	out, err := vm.EvaluateFile(config)
	if err != nil {
		return "", err
	}
	if len(out) > maxOutput {
		return "", fmt.Errorf("evaluates to %d bytes, more than the %d allowed", len(out), maxOutput)
	}
	return out, nil
}

// importer resolves imports: stratim.libsonnet, and the given sources by a
// path relative to the importing file. It hands out one Contents per file,
// as go-jsonnet requires.
type importer struct {
	files   map[string]jsonnet.Contents
	library jsonnet.Contents
}

func newImporter(sources map[string]string) *importer {
	files := make(map[string]jsonnet.Contents, len(sources))
	for name, data := range sources {
		files[name] = jsonnet.MakeContents(data)
	}
	return &importer{files: files, library: jsonnet.MakeContents(Library)}
}

func (i *importer) Import(importedFrom, importedPath string) (jsonnet.Contents, string, error) {
	if importedPath == LibraryName {
		return i.library, LibraryName, nil
	}
	if importedPath == VarsName {
		return i.files[VarsName], VarsName, nil
	}
	resolved := clean(importedPath) // the config itself
	if importedFrom != "" {
		resolved = clean(path.Join(path.Dir(importedFrom), importedPath))
	}
	if contents, ok := i.files[resolved]; ok {
		return contents, resolved, nil
	}
	return jsonnet.Contents{}, "", fmt.Errorf("%s not found: a config can import %s and the .jsonnet and .libsonnet files next to it", importedPath, LibraryName)
}

// varsJSON is vars as a jsonnet object; none is {}.
func varsJSON(vars map[string]string) string {
	if vars == nil {
		vars = make(map[string]string)
	}
	data, _ := json.Marshal(vars) //nolint:errchkjson // a map of strings always marshals
	return string(data)
}

// clean is a file name as configs and imports refer to it.
func clean(name string) string { return path.Clean(filepath.ToSlash(name)) }
