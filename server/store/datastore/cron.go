// Copyright 2022 Woodpecker Authors
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
	"errors"
	"fmt"

	"xorm.io/builder"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func (s storage) CronCreate(cron *model.Cron) error {
	if err := cron.Validate(); err != nil {
		return err
	}
	err := wrapInsert(s.engine.Insert(cron))
	if errors.Is(err, types.ErrInsertDuplicateDetected) {
		return fmt.Errorf("create cron failed, duplicate detected: %w", err)
	}
	return err
}

func (s storage) CronFind(repo *model.Repo, id int64) (*model.Cron, error) {
	cron := new(model.Cron)
	return cron, wrapGet(s.engine.ID(id).Where("repo_id = ?", repo.ID).Get(cron))
}

func (s storage) CronList(repo *model.Repo, p *model.ListOptionsWithAll) ([]*model.Cron, error) {
	var crons []*model.Cron
	return crons, s.paginate(p).Where("repo_id = ?", repo.ID).OrderBy("name").Find(&crons)
}

func (s storage) CronUpdate(_ *model.Repo, cron *model.Cron) error {
	return retryWrite(func() error {
		return wrapUpdate(s.engine.ID(cron.ID).AllCols().Update(cron))
	})
}

func (s storage) CronDelete(repo *model.Repo, id int64) error {
	return retryWrite(func() error {
		return wrapDelete(s.engine.ID(id).Where("repo_id = ?", repo.ID).Delete(new(model.Cron)))
	})
}

// CronListNextExecute returns limited number of jobs with NextExec being less or equal to the provided unix timestamp.
func (s storage) CronListNextExecute(nextExec, limit int64) ([]*model.Cron, error) {
	crons := make([]*model.Cron, 0, limit)
	return crons, s.engine.Join("INNER", "repos", "repos.id = crons.repo_id").Where(builder.Lte{"next_exec": nextExec}).And(builder.Eq{"repos.active": true, "enabled": true}).Limit(int(limit)).Find(&crons)
}

// CronGetLock try to get a lock by updating NextExec.
func (s storage) CronGetLock(cron *model.Cron, newNextExec int64) (bool, error) {
	var gotLock bool
	err := retryWrite(func() error {
		cols, e := s.engine.ID(cron.ID).Where(builder.Eq{"next_exec": cron.NextExec}).
			Cols("next_exec").Update(&model.Cron{NextExec: newNextExec})
		gotLock = cols != 0
		return wrapUpdate(cols, e)
	})
	if err == nil && gotLock {
		cron.NextExec = newNextExec
	}
	return gotLock, err
}

func (s storage) CronSetEnabled(repo *model.Repo, cron *model.Cron) error {
	// enabled, the next run was worked out from the schedule read: only
	// while the config has not changed it since
	cond := builder.Eq{"id": cron.ID, "repo_id": repo.ID, "managed": true}
	cols := []string{"enabled", "creator_id"}
	if cron.Enabled {
		cond["schedule"], cond["timezone"] = cron.Schedule, cron.Timezone
		cols = append(cols, "next_exec")
	}
	return retryWrite(func() error {
		n, err := s.engine.Where(cond).Cols(cols...).Update(cron)
		if err != nil {
			return wrapUpdate(n, err)
		}
		if n == 0 {
			// changed or gone, or a value as it was: tell them apart
			if exist, err := s.engine.Where(cond).Exist(new(model.Cron)); err != nil || !exist {
				return wrapGet(exist, err)
			}
		}
		return nil
	})
}

func (s storage) CronApplyConfig(repo *model.Repo, pipelineID int64, plan func([]*model.Cron) (*model.CronPlan, error)) (bool, error) {
	applied := false
	err := retryWrite(func() error {
		applied = false
		sess := s.engine.NewSession()
		defer sess.Close()
		if err := sess.Begin(); err != nil {
			return txErr(err)
		}

		// only a newer pipeline's config: pipeline IDs, unlike their
		// numbers, are never handed out twice. The row lock this takes
		// keeps other applies for the repository waiting until commit.
		n, err := sess.Table(new(model.Repo)).Where("id = ? AND (crons_from IS NULL OR crons_from < ?)", repo.ID, pipelineID).
			Cols("crons_from").Update(map[string]any{"crons_from": pipelineID})
		if err != nil {
			return txErr(err)
		}
		if n == 0 {
			return sess.Rollback()
		}

		var existing []*model.Cron
		if err := sess.Where("repo_id = ?", repo.ID).Find(&existing); err != nil {
			return txErr(err)
		}
		p, err := plan(existing)
		if err != nil {
			return err
		}
		for _, id := range p.Delete {
			if _, err := sess.ID(id).Where("repo_id = ? AND managed = ?", repo.ID, true).Delete(new(model.Cron)); err != nil {
				return txErr(err)
			}
		}
		for _, c := range p.Update {
			cols := []string{"name", "schedule", "timezone", "workflow", "branch", "variables"}
			if p.Rescheduled[c.ID] {
				cols = append(cols, "next_exec")
			}
			if _, err := sess.ID(c.ID).Where("repo_id = ? AND managed = ?", repo.ID, true).Cols(cols...).Update(c); err != nil {
				return txErr(err)
			}
		}
		for _, c := range p.Create {
			c.ID, c.RepoID = 0, repo.ID // a try rolled back may have set the ID
			if _, err := sess.Insert(c); err != nil {
				return txErr(err)
			}
		}
		if err := sess.Commit(); err != nil {
			return txErr(err)
		}
		applied = true
		return nil
	})
	return applied, err
}

// txErr marks the errors after which CronApplyConfig is tried again from
// the start, keeping their text: a lock SQLite gave up waiting for, or a
// cron another apply inserted first. A MariaDB deadlock retryWrite knows by
// itself.
func txErr(err error) error {
	switch {
	case err == nil:
		return nil
	case isErrDatabaseLocked(err):
		return fmt.Errorf("%w: %w", types.ErrDatabaseLocked, err)
	case errors.Is(wrapInsert(0, err), types.ErrInsertDuplicateDetected):
		return fmt.Errorf("%w: %w", types.ErrInsertDuplicateDetected, err)
	}
	return err
}
