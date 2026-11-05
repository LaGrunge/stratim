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

package common

import (
	"os"
	"path/filepath"
	"slices"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/jsonnet"
	"go.woodpecker-ci.org/woodpecker/v3/shared/constant"
)

// IsConfigFile reports whether name is part of a pipeline config: a YAML
// workflow, a jsonnet config or a jsonnet library.
func IsConfigFile(name string) bool {
	return slices.Contains(constant.DefaultConfigExtensions, filepath.Ext(name))
}

// ReadConfigFile reads file, and for a jsonnet config in a config folder
// also the libraries next to it, which it may import, as the server does.
func ReadConfigFile(file string) ([]*jsonnet.File, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	files := []*jsonnet.File{{Name: file, Data: data}}
	// a config folder is read as a whole, a single config at the root alone
	if !jsonnet.IsConfig(file) || filepath.Base(file) == ".woodpecker"+jsonnet.Ext {
		return files, nil
	}
	libs, err := filepath.Glob(filepath.Join(filepath.Dir(file), "*"+jsonnet.LibExt))
	if err != nil {
		return nil, err
	}
	for _, lib := range libs {
		data, err := os.ReadFile(lib)
		if err != nil {
			return nil, err
		}
		files = append(files, &jsonnet.File{Name: lib, Data: data})
	}
	return files, nil
}

// ReadConfigDir reads every file of a pipeline config under dir. Jsonnet
// files count only right in dir, as the server fetches a config folder.
func ReadConfigDir(dir string) ([]*jsonnet.File, error) {
	var files []*jsonnet.File
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || !IsConfigFile(info.Name()) {
			return nil
		}
		if (jsonnet.IsConfig(path) || jsonnet.IsLibrary(path)) && filepath.Dir(path) != filepath.Clean(dir) {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files = append(files, &jsonnet.File{Name: path, Data: data})
		return nil
	})
	return files, err
}
