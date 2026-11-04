// Copyright 2023 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	// Blank imports to register the sql drivers.
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"xorm.io/xorm"
	"xorm.io/xorm/schemas"

	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore/testdb"
)

const (
	sqliteDB     = "./test-files/sqlite.db"
	postgresDump = "./test-files/postgres.sql"
	mariadbDump  = "./test-files/mariadb.sql"
)

func testDriver() string {
	driver := os.Getenv("WOODPECKER_DATABASE_DRIVER")
	if len(driver) == 0 {
		return "sqlite3"
	}
	return driver
}

func createSQLiteDB(t *testing.T) string {
	tmpF, err := os.CreateTemp("./test-files", "tmp_")
	require.NoError(t, err)
	dbF, err := os.ReadFile(sqliteDB)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(tmpF.Name(), dbF, 0o644))
	return tmpF.Name()
}

func testDB(t *testing.T, initNewDB bool) (engine *xorm.Engine, closeDB func()) {
	driver := testDriver()
	var err error
	closeDB = func() {}
	switch driver {
	case "sqlite3":
		config := ":memory:"
		if !initNewDB {
			config = createSQLiteDB(t)
			closeDB = func() {
				_ = os.Remove(config)
			}
		}
		engine, err = xorm.NewEngine(driver, config)
		require.NoError(t, err)
		return engine, closeDB
	case "mysql":
		config := os.Getenv("WOODPECKER_DATABASE_DATASOURCE")
		cleanMySQLDB(t, config)
		closeDB = func() {
			// close the pool first, its connections would hold the database
			_ = engine.Close()
			cleanMySQLDB(t, config)
		}
		if !initNewDB {
			restoreMySQLDump(t, config)
		}
		engine, err = xorm.NewEngine(driver, config)
		require.NoError(t, err)
		return engine, closeDB
	case "postgres":
		config := os.Getenv("WOODPECKER_DATABASE_DATASOURCE")
		closeDB = func() {
			cleanPostgresDB(t, config)
		}
		if !initNewDB {
			restorePostgresDump(t, config)
		}
		engine, err = xorm.NewEngine(driver, config)
		require.NoError(t, err)
		return engine, closeDB
	default:
		t.Errorf("unsupported driver: %s", driver)
		t.FailNow()
	}
	return engine, closeDB
}

// restorePostgresDump only supports dumps generated with `pg_dump --inserts`.
func restorePostgresDump(t *testing.T, config string) {
	dump, err := os.ReadFile(postgresDump)
	require.NoError(t, err)

	db, err := sql.Open("postgres", config)
	require.NoError(t, err)
	defer db.Close()

	// clean dump
	lines := strings.Split(string(dump), "\n")
	newLines := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		switch {
		case line == "",
			strings.HasPrefix(line, "\\"),
			strings.HasPrefix(line, "--"):
			continue
		}
		newLines = append(newLines, line)
	}

	for stmt := range strings.SplitSeq(strings.Join(newLines, "\n"), ";") {
		if stmt == "" {
			continue
		}

		_, err = db.Exec(stmt)
		if err != nil {
			t.Logf("Failed to execute statement: %s", stmt[:min(len(stmt), 100)])
			require.NoErrorf(t, err, "could not load postgres dump")
		}
	}
}

// restoreMySQLDump loads a mariadb-dump file in one round trip; the dump's
// conditional comments and statements need a multi-statement connection.
func restoreMySQLDump(t *testing.T, config string) {
	dump, err := os.ReadFile(mariadbDump)
	require.NoError(t, err)

	cfg, err := mysql.ParseDSN(config)
	require.NoError(t, err)
	cfg.MultiStatements = true

	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(string(dump))
	require.NoError(t, err, "could not load mariadb dump")
}

// cleanMySQLDB recreates the test database, so that a dump loads into an
// empty one and nothing of it leaks into the next test.
func cleanMySQLDB(t *testing.T, config string) {
	testdb.RecreateMySQL(t, config)
}

// rowCounts counts the rows of every table but xormigrate's own.
func rowCounts(t *testing.T, engine *xorm.Engine) map[string]int64 {
	tables, err := engine.DBMetas()
	require.NoError(t, err)
	counts := make(map[string]int64, len(tables))
	for _, table := range tables {
		if table.Name == "migration" {
			continue
		}
		counts[table.Name], err = engine.Table(table.Name).Count()
		require.NoError(t, err)
	}
	return counts
}

// appliedMigrations is how many migrations xormigrate has recorded; an old
// database from before xormigrate has none.
func appliedMigrations(t *testing.T, engine *xorm.Engine) int64 {
	exists, err := engine.IsTableExist("migration")
	require.NoError(t, err)
	if !exists {
		return 0
	}
	n, err := engine.Table("migration").Count()
	require.NoError(t, err)
	return n
}

func cleanPostgresDB(t *testing.T, config string) {
	db, err := sql.Open("postgres", config)
	require.NoError(t, err)
	defer db.Close()

	// Drop and recreate the public schema
	// This removes all tables, indexes, constraints, sequences, etc.
	_, err = db.Exec(`
		DROP SCHEMA public CASCADE;
		CREATE SCHEMA public;
		GRANT ALL ON SCHEMA public TO postgres;
		GRANT ALL ON SCHEMA public TO public;
	`)
	require.NoError(t, err)
}

func TestMigrate(t *testing.T) {
	// init new db
	engine, closeDB := testDB(t, true)
	assert.NoError(t, Migrate(t.Context(), engine, true))
	closeDB()

	dbType := engine.Dialect().URI().DBType
	if dbType == schemas.MYSQL || dbType == schemas.POSTGRES {
		// wait for mysql/postgres to sync ...
		time.Sleep(100 * time.Millisecond)
	}

	// migrate old db
	engine, closeDB = testDB(t, false)
	appliedBefore := appliedMigrations(t, engine)
	var rowsBefore map[string]int64
	if testDriver() == "mysql" {
		rowsBefore = rowCounts(t, engine)
	}
	assert.NoError(t, Migrate(t.Context(), engine, true))

	// Every fixture predates the newest migration; if none ran, the fixture
	// went stale and this case tests nothing.
	assert.Greater(t, appliedMigrations(t, engine), appliedBefore, "the upgrade ran no migration")
	// every fixture holds repositories; an empty table would mean the
	// fixture never loaded and the case above proved nothing
	repos, err := engine.Table("repos").Count()
	assert.NoError(t, err)
	assert.Positive(t, repos, "the existing database lost its repositories")
	// The MariaDB fixture has no duplicates for a migration to clean up, so
	// the upgrade must keep every row of every table it had.
	for table, before := range rowsBefore {
		after, err := engine.Table(table).Count()
		assert.NoError(t, err)
		assert.Equalf(t, before, after, "rows of %s after the upgrade", table)
	}
	closeDB()
}
