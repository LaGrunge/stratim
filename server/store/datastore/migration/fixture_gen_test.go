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

//go:build fixturegen

package migration

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"xorm.io/xorm"
)

// TestGenerateMariaDBFixture migrates a Postgres database holding
// test-files/postgres.sql and an empty MariaDB database to the schema of the
// checkout it runs in, then copies the rows across. Dumping the MariaDB
// database gives test-files/mariadb.sql.
//
// The script scripts/gen-mariadb-fixture.sh runs it inside a worktree of an
// older release, so that the fixture is an old database the current
// migrations have to upgrade. It never runs in CI: it needs the fixturegen tag and two
// databases of its own. It depends on nothing but Migrate, whose signature has
// not changed since v3.0, so it also compiles in those old worktrees.
func TestGenerateMariaDBFixture(t *testing.T) {
	pgConfig := os.Getenv("FIXTURE_POSTGRES_DATASOURCE")
	myConfig := os.Getenv("FIXTURE_MARIADB_DATASOURCE")
	if pgConfig == "" || myConfig == "" {
		t.Skip("set FIXTURE_POSTGRES_DATASOURCE and FIXTURE_MARIADB_DATASOURCE")
	}

	pg, err := xorm.NewEngine("postgres", pgConfig)
	require.NoError(t, err)
	defer pg.Close()
	require.NoError(t, Migrate(t.Context(), pg, true))

	my, err := xorm.NewEngine("mysql", myConfig)
	require.NoError(t, err)
	defer my.Close()
	require.NoError(t, Migrate(t.Context(), my, true))

	pgTables, err := pg.DBMetas()
	require.NoError(t, err)
	pgColumns := make(map[string][]string, len(pgTables))
	for _, table := range pgTables {
		// xorm's Postgres metadata can list a column twice (crons.id); keep
		// each name once.
		var columns []string
		for _, column := range table.ColumnsSeq() {
			if !slices.Contains(columns, column) {
				columns = append(columns, column)
			}
		}
		pgColumns[table.Name] = columns
	}

	myTables, err := my.DBMetas()
	require.NoError(t, err)
	for _, table := range myTables {
		// xormigrate's record of applied migrations: MariaDB has written its
		// own, and the table has no key to stop a second copy.
		if table.Name == "migration" {
			continue
		}
		have, ok := pgColumns[table.Name]
		require.Truef(t, ok, "table %s is missing on the Postgres side", table.Name)

		dst := table.ColumnsSeq()
		src := make([]string, len(dst))
		for i, column := range dst {
			src[i] = column
			if from, ok := renamedInFixture[table.Name][column]; ok {
				src[i] = from
			}
		}
		// Every Postgres column has to land somewhere, or its data would be
		// lost without a trace. The release's own columns that the renames
		// replace are empty there: Migrate only just added them.
		for _, column := range have {
			_, replaced := renamedInFixture[table.Name][column]
			assert.Truef(t, slices.Contains(src, column) || replaced ||
				slices.Contains(leftoverInFixture[table.Name], column),
				"column %s.%s has no place in the MariaDB schema", table.Name, column)
		}
		copyRows(t, pg, my, table.Name, src, dst)
	}
}

// renamedInFixture maps a column of the release's schema to the column of
// postgres.sql holding its data, where upstream rewrote that dump to a later
// schema: forges already has the client columns of migration fix-forge-columns
// (v3.10), whose data an older database keeps in the o_auth ones.
var renamedInFixture = map[string]map[string]string{
	"forges": {
		"o_auth_client_i_d":    "client",
		"o_auth_client_secret": "client_secret",
	},
}

// leftoverInFixture lists columns of postgres.sql that the release's schema
// no longer has and no migration reads; they are left behind.
var leftoverInFixture = map[string][]string{
	"repos": {"repo_allow_push"},
}

func copyRows(t *testing.T, from, to *xorm.Engine, table string, src, dst []string) {
	t.Helper()
	// Some column names are reserved words in one of the two dialects
	// (pipelines.release in MariaDB), so each side quotes in its own way.
	quoted := func(e *xorm.Engine, columns []string) string {
		names := make([]string, len(columns))
		for i, column := range columns {
			names[i] = e.Quote(column)
		}
		return strings.Join(names, ", ")
	}
	rows, err := from.DB().Query(fmt.Sprintf("SELECT %s FROM %s ORDER BY 1", quoted(from, src), from.Quote(table)))
	require.NoError(t, err)
	defer rows.Close()

	columns := dst
	insert := fmt.Sprintf("INSERT INTO %s (%s) VALUES (?%s)",
		to.Quote(table), quoted(to, dst), strings.Repeat(", ?", len(dst)-1))
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		require.NoError(t, rows.Scan(pointers...))
		_, err := to.DB().Exec(insert, values...)
		require.NoErrorf(t, err, "copy a row of %s", table)
	}
	require.NoError(t, rows.Err())

	want, err := from.Table(table).Count()
	require.NoError(t, err)
	got, err := to.Table(table).Count()
	require.NoError(t, err)
	assert.Equalf(t, want, got, "rows of %s", table)
	t.Logf("%s: %d rows", table, got)
}
