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

//go:build test

package setup

import (
	"sync"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// FaultyStore is a store whose writes fail when told, as a database that
// goes away for a while.
type FaultyStore struct {
	store.Store

	mu         sync.Mutex
	finishErr  error
	finishFail int
	// finishFailed counts the writes of a workflow's end failed so far.
	finishFailed int
}

func (f *FaultyStore) failWorkflowFinish(n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finishFail, f.finishErr = n, err
}

func (f *FaultyStore) workflowFinishFailed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.finishFailed
}

func (f *FaultyStore) WorkflowFinish(workflow *model.Workflow) (bool, error) {
	f.mu.Lock()
	if f.finishFail > 0 {
		f.finishFail--
		f.finishFailed++
		err := f.finishErr
		f.mu.Unlock()
		return false, err
	}
	f.mu.Unlock()
	return f.Store.WorkflowFinish(workflow)
}
