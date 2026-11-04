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
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore/testdb"
)

// DatasourceEnv names the variable that switches the harness from in-memory
// sqlite to MariaDB. It holds a go-sql-driver datasource whose database is
// disposable (named test or *_test): every scenario recreates it. All
// scenarios share that one database, so none may call t.Parallel, and nothing
// else may use the database while the e2e tests run.
//
//	STRATIM_E2E_DATASOURCE='root@tcp(127.0.0.1:3306)/e2e_test?parseTime=true'
const DatasourceEnv = "STRATIM_E2E_DATASOURCE"

// The MariaDB pool: the scheduler, gRPC handlers and agents' updates query
// the store concurrently.
const (
	mariadbMaxOpenConns = 10
	mariadbMaxIdleConns = 2
)

// Fixtures holds the pre-seeded database records shared across all tests.
type Fixtures struct {
	Forge *model.Forge
	Org   *model.Org
	Owner *model.User
	Repo  *model.Repo
}

// newStore creates a fully-migrated store: in-memory sqlite by default, or a
// freshly recreated MariaDB database when DatasourceEnv is set.
func newStore(ctx context.Context, t *testing.T) store.Store {
	t.Helper()

	opts := &store.Opts{
		Driver: "sqlite3",
		Config: ":memory:",
		// MaxOpenConns=1 and MaxIdleConns=1 are required for in-memory sqlite:
		// without them the pool drops idle connections, destroying the in-memory
		// schema between calls and breaking migrations.
		XORM: store.XORM{
			MaxOpenConns: 1,
			MaxIdleConns: 1,
		},
	}
	if datasource := os.Getenv(DatasourceEnv); datasource != "" {
		testdb.RecreateMySQL(t, datasource)
		opts = &store.Opts{
			Driver: "mysql",
			Config: datasource,
			XORM: store.XORM{
				MaxOpenConns: mariadbMaxOpenConns,
				MaxIdleConns: mariadbMaxIdleConns,
			},
		}
	}

	s, err := datastore.NewEngine(opts)
	require.NoError(t, err, "create %s store", opts.Driver)

	require.NoError(t, s.Ping(), "ping store")
	require.NoError(t, s.Migrate(ctx, true), "migrate store")

	t.Cleanup(func() { _ = s.Close() })
	return s
}

// seedFixtures creates the minimal set of DB records every test needs:
// one Forge, one owner User with its personal Org, one Repo linked to them.
func seedFixtures(t *testing.T, s store.Store) *Fixtures {
	t.Helper()

	forge := &model.Forge{
		Type: TestForgeType,
		URL:  "https://forge.example.test",
	}
	require.NoError(t, s.ForgeCreate(forge), "seed forge")

	org := &model.Org{
		ForgeID: forge.ID,
		Name:    "test-owner",
		IsUser:  true,
	}
	require.NoError(t, s.OrgCreate(org), "seed org")

	owner := &model.User{
		ForgeID:       forge.ID,
		OrgID:         org.ID,
		ForgeRemoteID: "1",
		Login:         "test-owner",
		Email:         "owner@example.test",
	}
	require.NoError(t, s.CreateUser(owner), "seed user")

	repo := &model.Repo{
		ForgeID:       forge.ID,
		ForgeRemoteID: "1",
		UserID:        owner.ID,
		OrgID:         org.ID,
		FullName:      "test-owner/test-repo",
		Owner:         "test-owner",
		Name:          "test-repo",
		Clone:         "https://forge.example.test/test-owner/test-repo.git",
		Branch:        "main",
		IsActive:      true,
		AllowPull:     true,
	}
	require.NoError(t, s.CreateRepo(repo), "seed repo")

	return &Fixtures{
		Forge: forge,
		Org:   org,
		Owner: owner,
		Repo:  repo,
	}
}
