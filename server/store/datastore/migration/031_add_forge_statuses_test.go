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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"xorm.io/xorm"
	"xorm.io/xorm/schemas"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

type pipelineV031 struct {
	ID       int64  `xorm:"pk autoincr 'id'"`
	Status   string `xorm:"'status'"`
	Event    string `xorm:"'event'"`
	Created  int64  `xorm:"'created'"`
	Finished int64  `xorm:"'finished'"`
}

func (pipelineV031) TableName() string { return "pipelines" }

type workflowV031 struct {
	ID         int64  `xorm:"pk autoincr 'id'"`
	PipelineID int64  `xorm:"'pipeline_id'"`
	State      string `xorm:"'state'"`
}

func (workflowV031) TableName() string { return "workflows" }

// TestAddForgeStatuses: the statuses of the finished workflows of recent
// pipelines that finished long enough ago are taken as taken; those in
// flight, and older pipelines, are left out.
func TestAddForgeStatuses(t *testing.T) {
	engine, closeDB := testDB(t, true)
	defer closeDB()

	require.NoError(t, engine.Sync(new(pipelineV031), new(workflowV031)))
	const since, settled = 1_700_000_000, 1_700_100_000
	_, err := engine.Insert(
		&pipelineV031{ID: 31000, Status: "failure", Created: since - 1, Finished: since},                            // too old
		&pipelineV031{ID: 31001, Status: "success", Created: since, Finished: settled},                              // just in
		&pipelineV031{ID: 31002, Status: "running", Created: since + 1},                                             // still running
		&pipelineV031{ID: 31003, Status: "failure", Created: since + 2, Finished: settled + 1},                      // finished just now
		&pipelineV031{ID: 31004, Status: "failure", Event: "deployment", Created: since + 3, Finished: settled - 1}, // done
		&pipelineV031{ID: 31005, Status: "killed", Created: since + 4, Finished: settled - 1},                       // a workflow left running
	)
	require.NoError(t, err)
	_, err = engine.Insert(
		&workflowV031{ID: 31100, PipelineID: 31000, State: "failure"},
		&workflowV031{ID: 31101, PipelineID: 31001, State: "success"},
		&workflowV031{ID: 31102, PipelineID: 31001, State: "skipped"},
		&workflowV031{ID: 31103, PipelineID: 31002, State: "running"},
		&workflowV031{ID: 31104, PipelineID: 31003, State: "failure"},
		&workflowV031{ID: 31105, PipelineID: 31004, State: "failure"},
		&workflowV031{ID: 31106, PipelineID: 31005, State: "running"},
	)
	require.NoError(t, err)

	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, addForgeStatusesSchema(sess))
	n, err := seedForgeStatuses(sess, since, settled)
	require.NoError(t, err)
	require.NoError(t, sess.Commit())
	assert.Equal(t, 3, n)

	var seeded []*forgeStatusesV031
	require.NoError(t, engine.Asc("workflow_id").Find(&seeded))
	assert.Equal(t, []*forgeStatusesV031{
		{WorkflowID: 31101, PipelineID: 31001, Value: "success"},
		{WorkflowID: 31102, PipelineID: 31001, Value: "skipped"},
		{WorkflowID: 31105, PipelineID: 31004, Value: "failure failure"},
	}, seeded)
}

// TestAddForgeStatusesBatches: more workflows than a batch are all seeded,
// and an empty range seeds none.
func TestAddForgeStatusesBatches(t *testing.T) {
	engine, closeDB := testDB(t, true)
	defer closeDB()

	require.NoError(t, engine.Sync(new(pipelineV031), new(workflowV031)))
	now := time.Now().Unix()
	_, err := engine.Insert(&pipelineV031{ID: 32001, Status: "success", Created: now, Finished: now})
	require.NoError(t, err)
	workflows := make([]*workflowV031, 0, seedBatch+5)
	for i := range seedBatch + 5 {
		workflows = append(workflows, &workflowV031{ID: 32100 + int64(i), PipelineID: 32001, State: "success"})
	}
	_, err = engine.Insert(workflows)
	require.NoError(t, err)

	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, addForgeStatusesSchema(sess))
	n, err := seedForgeStatuses(sess, now+1, now)
	require.NoError(t, err)
	assert.Zero(t, n, "no pipeline created since")
	n, err = seedForgeStatuses(sess, now, now)
	require.NoError(t, err)
	assert.Equal(t, seedBatch+5, n)

	// run again, as after a migration that failed halfway: nothing twice
	n, err = seedForgeStatuses(sess, now, now)
	require.NoError(t, err)
	require.NoError(t, sess.Commit())
	assert.Zero(t, n)
	count, err := engine.Count(new(forgeStatusesV031))
	require.NoError(t, err)
	assert.EqualValues(t, seedBatch+5, count)
}

// TestAddForgeStatusesKeepsIndexes: adding the index on pipelines.created
// leaves the other indexes of the table, its unique one included.
func TestAddForgeStatusesKeepsIndexes(t *testing.T) {
	engine, closeDB := testDB(t, true)
	defer closeDB()

	require.NoError(t, engine.Sync(new(model.Pipeline), new(model.Workflow)))
	created := schemas.NewIndex("created", schemas.IndexType)
	created.AddColumn("created")
	_, err := engine.Exec(engine.Dialect().DropIndexSQL("pipelines", created))
	require.NoError(t, err)
	before := pipelineIndexes(t, engine)
	require.NotContains(t, before, "created")

	sess := engine.NewSession()
	defer sess.Close()
	require.NoError(t, addForgeStatuses.MigrateSession(sess))
	require.NoError(t, sess.Commit())

	after := pipelineIndexes(t, engine)
	for name, typ := range before {
		assert.Equal(t, typ, after[name], "index %s", name)
	}
	assert.Equal(t, schemas.UniqueType, after["s"], "UQE_pipelines_s")
	assert.Equal(t, schemas.IndexType, after["repo_id"], "IDX_pipelines_repo_id")
	assert.Equal(t, schemas.IndexType, after["created"], "IDX_pipelines_created")
}

// pipelineIndexes are the indexes of pipelines, by the name xorm gives them
// in the model.
func pipelineIndexes(t *testing.T, engine *xorm.Engine) map[string]int {
	t.Helper()
	tables, err := engine.DBMetas()
	require.NoError(t, err)
	for _, table := range tables {
		if table.Name == "pipelines" {
			out := make(map[string]int, len(table.Indexes))
			for name, index := range table.Indexes {
				out[name] = index.Type
			}
			return out
		}
	}
	t.Fatal("no pipelines table")
	return nil
}
