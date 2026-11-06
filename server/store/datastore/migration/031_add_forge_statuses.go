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

package migration

import (
	"database/sql"
	"time"

	"github.com/rs/zerolog/log"
	"src.techknowlogick.com/xormigrate"
	"xorm.io/xorm"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

const (
	// How far back the statuses already sent are taken as taken.
	forgeStatusesSeeded = 30 * 24 * time.Hour
	// How long ago a pipeline must have finished for its statuses to have
	// had their chance: the window the server tries a status for.
	forgeStatusesSettled = 10 * time.Minute
	seedBatch            = 1000
)

type forgeStatusesV031 struct {
	WorkflowID int64  `xorm:"pk 'workflow_id'"`
	PipelineID int64  `xorm:"INDEX 'pipeline_id'"`
	Value      string `xorm:"'value'"`
}

func (forgeStatusesV031) TableName() string { return "forge_statuses" }

type pipelinesV031 struct {
	ID      int64 `xorm:"pk autoincr 'id'"`
	Created int64 `xorm:"'created' NOT NULL DEFAULT 0 created INDEX"`
}

func (pipelinesV031) TableName() string { return "pipelines" }

// addForgeStatuses adds the table of the statuses each forge took, and an
// index on when pipelines were created, which bounds the scan for the ones
// to send again on start. The statuses of the workflows that finished, in
// pipelines that finished long enough ago, are taken as taken, so that the
// first start does not send them all again; those still in flight are sent.
var addForgeStatuses = xormigrate.Migration{
	ID: "add-forge-statuses",
	MigrateSession: func(sess *xorm.Session) error {
		if err := addForgeStatusesSchema(sess); err != nil {
			return err
		}
		now := time.Now()
		n, err := seedForgeStatuses(sess, now.Add(-forgeStatusesSeeded).Unix(), now.Add(-forgeStatusesSettled).Unix())
		if err != nil {
			return err
		}
		log.Info().Msgf("forge statuses: %d of finished workflows taken as taken", n)
		return nil
	},
}

// addForgeStatusesSchema adds the index on pipelines.created and the table.
// The index alone: a plain Sync of the partial pipelines struct would drop
// every other index of the table, its unique one included.
func addForgeStatusesSchema(sess *xorm.Session) error {
	if _, err := sess.SyncWithOptions(xorm.SyncOptions{IgnoreDropIndices: true, IgnoreConstrains: true}, new(pipelinesV031)); err != nil {
		return err
	}
	return sess.Sync(new(forgeStatusesV031))
}

type seedRowV031 struct {
	WorkflowID     int64              `xorm:"'workflow_id'"`
	PipelineID     int64              `xorm:"'pipeline_id'"`
	State          model.StatusValue  `xorm:"'state'"`
	PipelineEvent  model.WebhookEvent `xorm:"'pipeline_event'"`
	PipelineStatus model.StatusValue  `xorm:"'pipeline_status'"`
}

// seedForgeStatuses records as taken the statuses of the finished workflows
// of the pipelines created since then that finished by settled (unix
// seconds), but for those recorded already: a DDL statement ends the
// transaction on MariaDB, so a migration that failed halfway may run again.
// It returns how many it recorded.
func seedForgeStatuses(sess *xorm.Session, since, settled int64) (int, error) {
	var first sql.NullInt64
	if _, err := sess.SQL("SELECT MIN(id) FROM pipelines WHERE created >= ?", since).Get(&first); err != nil {
		return 0, err
	}
	if !first.Valid {
		return 0, nil
	}
	// the workflows of those pipelines, by primary key
	var bounds struct {
		Low  sql.NullInt64 `xorm:"'low'"`
		High sql.NullInt64 `xorm:"'high'"`
	}
	if _, err := sess.SQL("SELECT MIN(id) AS low, MAX(id) AS high FROM workflows WHERE pipeline_id >= ?", first.Int64).Get(&bounds); err != nil {
		return 0, err
	}
	if !bounds.Low.Valid {
		return 0, nil
	}

	seeded := 0
	after := bounds.Low.Int64 - 1
	for {
		var rows []*seedRowV031
		err := sess.Table("workflows").Alias("w").
			Join("INNER", []string{"pipelines", "p"}, "p.id = w.pipeline_id").
			Join("LEFT", []string{"forge_statuses", "f"}, "f.workflow_id = w.id").
			Select("w.id AS workflow_id, w.pipeline_id, w.state, p.event AS pipeline_event, p.status AS pipeline_status").
			Where("w.id > ? AND w.id <= ? AND p.id >= ? AND p.finished > 0 AND p.finished <= ? AND f.workflow_id IS NULL",
				after, bounds.High.Int64, first.Int64, settled).
			Asc("w.id").Limit(seedBatch).Find(&rows)
		if err != nil {
			return seeded, err
		}
		if len(rows) == 0 {
			return seeded, nil
		}
		after = rows[len(rows)-1].WorkflowID

		statuses := make([]*forgeStatusesV031, 0, len(rows))
		for _, r := range rows {
			if r.State.IsDone() && r.PipelineStatus.IsDone() {
				statuses = append(statuses, &forgeStatusesV031{
					WorkflowID: r.WorkflowID, PipelineID: r.PipelineID, Value: model.ForgeStatusValue(r.PipelineEvent, r.State, r.PipelineStatus),
				})
			}
		}
		if len(statuses) > 0 {
			if _, err := sess.Insert(statuses); err != nil {
				return seeded, err
			}
		}
		seeded += len(statuses)
	}
}
