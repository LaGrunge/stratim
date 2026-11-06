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

// Package testdb prepares MariaDB databases for tests that run against a real
// server: the datastore migration tests and the e2e harness. It is test-only
// and must keep the test build tag, so that no production binary can drop a
// database through it.
package testdb

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// IsDisposable reports whether a database may be dropped by a test: only
// "test" and names ending in "_test" are, so that a datasource exported for a
// real server is never wiped by running the tests.
func IsDisposable(name string) bool {
	return name == "test" || strings.HasSuffix(name, "_test")
}

// disposable parses the datasource and returns it without its database,
// together with the database name, or an error when that name is not
// IsDisposable.
func disposable(datasource string) (*mysql.Config, string, error) {
	cfg, err := mysql.ParseDSN(datasource)
	if err != nil {
		return nil, "", err
	}
	name := cfg.DBName
	if !IsDisposable(name) {
		return nil, "", fmt.Errorf("refusing to drop database %q: name it test or *_test", name)
	}
	// connect without the database, it may not exist yet
	cfg.DBName = ""
	return cfg, name, nil
}

// RecreateMySQL drops and recreates the database the datasource names,
// leaving it empty. It refuses any name IsDisposable rejects.
func RecreateMySQL(t testing.TB, datasource string) {
	t.Helper()

	cfg, name, err := disposable(datasource)
	require.NoError(t, err)

	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer db.Close()

	quoted := "`" + strings.ReplaceAll(name, "`", "``") + "`"
	_, err = db.Exec("DROP DATABASE IF EXISTS " + quoted)
	require.NoError(t, err)
	_, err = db.Exec("CREATE DATABASE " + quoted + " CHARACTER SET utf8mb4")
	require.NoError(t, err)
}
