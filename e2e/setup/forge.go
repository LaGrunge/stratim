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

package setup

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"

	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// Files are the config files the mock forge serves; a test changes them
// between pushes with Set.
type Files struct {
	mu    sync.Mutex
	files []*forge_types.FileMeta
}

// Set makes files the config the next pushes fetch; none is no config.
func (f *Files) Set(files []*forge_types.FileMeta) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files = files
}

func (f *Files) get() []*forge_types.FileMeta {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.files
}

// newMockForge builds a MockForge that serves the given files for any
// config-fetch call, records status reporting, and stubs all other methods safely.
func newMockForge(t *testing.T, files *Files, statuses *Statuses) *forge_mocks.MockForge {
	t.Helper()
	m := forge_mocks.NewMockForge(t)

	// Identity.
	m.On("Name").Return("mock").Maybe()
	m.On("URL").Return("https://forge.example.test").Maybe()

	// we just use multi workflows
	m.On(
		"File",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(nil, nil).Maybe()

	m.EXPECT().Dir(mock.Anything, mock.Anything, mock.Anything, mock.Anything, ".woodpecker").
		RunAndReturn(func(context.Context, *model.User, *model.Repo, *model.Pipeline, string) ([]*forge_types.FileMeta, error) {
			return files.get(), nil
		}).Maybe()

	// Status reporting back to forge, recorded.
	m.On(
		"Status",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(statuses.status).Maybe()

	// Netrc for clone steps.
	m.On(
		"Netrc",
		mock.Anything, mock.Anything,
	).Return(&model.Netrc{}, nil).Maybe()

	// The head of a branch, for crons.
	m.On(
		"BranchHead",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return(&model.Commit{SHA: "deadbeef"}, nil).Maybe()

	return m
}
