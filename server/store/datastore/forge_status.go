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

package datastore

import (
	"database/sql"

	"xorm.io/xorm"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// ForgeStatusSet records the status the forge of the workflow took.
func (s storage) ForgeStatusSet(status *model.ForgeStatus) error {
	return retryWrite(func() error {
		sess := s.engine.NewSession()
		defer sess.Close()
		if err := sess.Begin(); err != nil {
			return lockErr(err)
		}

		exist, err := sess.ID(status.WorkflowID).Exist(new(model.ForgeStatus))
		if err != nil {
			return lockErr(err)
		}
		if exist {
			err = wrapUpdate(sess.ID(status.WorkflowID).AllCols().Update(status))
		} else {
			// not for a workflow deleted already, which would leave the row
			// behind. Not locking the workflow: on MariaDB its state changes
			// would make this fail as often (1020); a row left by a deletion
			// committed meanwhile is never read, the scan going by workflows.
			_, err = sess.Exec(`INSERT INTO forge_statuses (workflow_id, pipeline_id, value)
SELECT ?, ?, ? FROM workflows WHERE id = ?`, status.WorkflowID, status.PipelineID, status.Value, status.WorkflowID)
			err = wrapInsert(0, err)
		}
		if err != nil {
			return err
		}

		return lockErr(sess.Commit())
	})
}

// ForgeStatusCandidates returns the workflows of the pipelines created since
// then (unix seconds), with the status their forge took, newest first: at
// most limit of them, with an ID below before unless that is 0.
func (s storage) ForgeStatusCandidates(since, before int64, limit int) ([]*model.ForgeStatusCandidate, error) {
	sess := s.engine.NewSession()
	defer sess.Close()
	first, err := firstPipelineSince(sess, since)
	if err != nil || first == 0 {
		return nil, err
	}
	low, _, err := workflowsFrom(sess, first)
	if err != nil || low == 0 {
		return nil, err
	}

	q := sess.Table("workflows").Alias("w").
		Join("INNER", []string{"pipelines", "p"}, "p.id = w.pipeline_id").
		Join("LEFT", []string{"forge_statuses", "f"}, "f.workflow_id = w.id").
		Select("w.id AS workflow_id, w.pipeline_id, w.state, p.event AS pipeline_event, p.status AS pipeline_status, COALESCE(f.value, '') AS value").
		Where("w.id >= ? AND w.pipeline_id >= ?", low, first)
	if before > 0 {
		q = q.And("w.id < ?", before)
	}
	var candidates []*model.ForgeStatusCandidate
	return candidates, q.Desc("w.id").Limit(limit).Find(&candidates)
}

// firstPipelineSince is the lowest ID of the pipelines created since then
// (unix seconds), 0 if none was; pipelines are indexed by creation.
func firstPipelineSince(sess *xorm.Session, since int64) (int64, error) {
	var first sql.NullInt64
	if _, err := sess.SQL("SELECT MIN(id) FROM pipelines WHERE created >= ?", since).Get(&first); err != nil {
		return 0, err
	}
	return first.Int64, nil
}

// workflowsFrom are the lowest and highest IDs of the workflows of the
// pipelines from that ID on, 0 if there are none: they bound a scan of
// recent workflows by primary key.
func workflowsFrom(sess *xorm.Session, pipelineID int64) (low, high int64, err error) {
	var bounds struct {
		Low  sql.NullInt64 `xorm:"'low'"`
		High sql.NullInt64 `xorm:"'high'"`
	}
	if _, err := sess.SQL("SELECT MIN(id) AS low, MAX(id) AS high FROM workflows WHERE pipeline_id >= ?", pipelineID).Get(&bounds); err != nil {
		return 0, 0, err
	}
	return bounds.Low.Int64, bounds.High.Int64, nil
}

// forgeStatusesDelete forgets the statuses taken of the workflows of the
// pipeline.
func forgeStatusesDelete(sess *xorm.Session, pipelineID int64) error {
	_, err := sess.Where("pipeline_id = ?", pipelineID).Delete(new(model.ForgeStatus))
	return err
}

// lockErr is err, or ErrDatabaseLocked for an SQLite lock, so that
// retryWrite tries again; it does on MariaDB deadlocks as they are.
func lockErr(err error) error {
	if isErrDatabaseLocked(err) {
		return types.ErrDatabaseLocked
	}
	return err
}
