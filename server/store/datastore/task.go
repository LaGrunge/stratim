// Copyright 2021 Woodpecker Authors
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

package datastore

import (
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

func (s storage) TaskList() ([]*model.Task, error) {
	tasks := make([]*model.Task, 0, perPage)
	return tasks, s.engine.Find(&tasks)
}

func (s storage) TaskInsert(task *model.Task) error {
	// only Insert set auto created ID back to object
	return wrapInsert(s.engine.Insert(task))
}

// TaskSetBoost sets the boost of the stored tasks of ids that have a lower
// one: a task already handed out or canceled (deleted) stays deleted, and a
// later move of the same task, written first, is not undone.
func (s storage) TaskSetBoost(ids []string, boost int64) error {
	if len(ids) == 0 {
		return nil
	}
	return retryWrite(func() error {
		_, err := s.engine.In("id", ids).And("boost < ?", boost).Cols("boost").Update(&model.Task{Boost: boost})
		return err
	})
}

// TaskSetQueued sets when the stored task of id was queued, if it was not
// set yet: a task deleted meanwhile stays deleted.
func (s storage) TaskSetQueued(id string, queued int64) error {
	return retryWrite(func() error {
		_, err := s.engine.Where("id = ? AND queued = 0", id).Cols("queued").Update(&model.Task{Queued: queued})
		return err
	})
}

func (s storage) TaskDelete(id string) error {
	return retryWrite(func() error {
		return wrapDelete(s.engine.Where("id = ?", id).Delete(new(model.Task)))
	})
}
