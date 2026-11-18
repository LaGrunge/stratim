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

package transient

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"syscall"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func TestIs(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"bad connection", driver.ErrBadConn, true},
		{"invalid connection", mysql.ErrInvalidConn, true},
		{"connection done", sql.ErrConnDone, true},
		{"unexpected EOF elsewhere", io.ErrUnexpectedEOF, false},
		{"HTTP call", &url.Error{Op: "Get", URL: "https://forge", Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}, false},
		{"read only", &mysql.MySQLError{Number: 1290}, true},
		{"read only mode", &mysql.MySQLError{Number: 1836}, true},
		{"galera not ready", &mysql.MySQLError{Number: 1047}, true},
		{"postgres shutdown", &pq.Error{Code: "57P01"}, true},
		{"postgres starting", &pq.Error{Code: "57P03"}, true},
		{"postgres deadlock", &pq.Error{Code: "40P01"}, true},
		{"postgres connection", &pq.Error{Code: "08006"}, true},
		{"postgres unique", &pq.Error{Code: "23505"}, false},
		{"wrapped", fmt.Errorf("load workflow: %w", mysql.ErrInvalidConn), true},
		{"connection refused", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, true},
		{"too many connections", &mysql.MySQLError{Number: 1040}, true},
		{"shutdown in progress", &mysql.MySQLError{Number: 1053}, true},
		{"deadlock", &mysql.MySQLError{Number: 1213}, true},
		{"lock wait timeout", &mysql.MySQLError{Number: 1205}, true},
		{"sqlite locked", types.ErrDatabaseLocked, true},
		{"sqlite locked text", errors.New("database is locked (5) (SQLITE_BUSY)"), true},
		{"duplicate entry", &mysql.MySQLError{Number: 1062}, false},
		{"syntax", &mysql.MySQLError{Number: 1064}, false},
		{"not found", types.ErrRecordNotExist, false},
		{"other", errors.New("workflow was already marked as finished"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Is(tc.err))
		})
	}
}
