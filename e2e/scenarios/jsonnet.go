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

//go:build test

package scenarios

import (
	"path"
	"testing"

	"github.com/google/go-jsonnet"
	"github.com/stretchr/testify/require"
)

// libDir holds the .libsonnet files every jsonnet fixture may import by bare
// name, e.g. import 'steps.libsonnet'. It is not a scenario.
const libDir = "_lib"

// TODO(S0-4): delete this evaluator once the server evaluates jsonnet itself;
// the loader then serves the .jsonnet fixtures unchanged.

// evaluateJsonnet evaluates a jsonnet fixture, resolving imports against
// fixtures/_lib/ only.
func evaluateJsonnet(t *testing.T, location string, source []byte) []byte {
	t.Helper()

	libs, err := fixtureFS.ReadDir(path.Join("fixtures", libDir))
	require.NoError(t, err, "read jsonnet library dir")
	importer := &jsonnet.MemoryImporter{Data: make(map[string]jsonnet.Contents, len(libs))}
	for _, lib := range libs {
		data, err := fixtureFS.ReadFile(path.Join("fixtures", libDir, lib.Name()))
		require.NoError(t, err, "read %s/%s", libDir, lib.Name())
		importer.Data[lib.Name()] = jsonnet.MakeContents(string(data))
	}

	vm := jsonnet.MakeVM()
	vm.Importer(importer)
	out, err := vm.EvaluateAnonymousSnippet(location, string(source))
	require.NoError(t, err, "evaluate %s", location)
	return []byte(out)
}
