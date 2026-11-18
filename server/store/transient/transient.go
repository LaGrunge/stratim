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

// Package transient tells the errors of a store that is unavailable for a
// while from the others, for each driver the server supports. It is the
// only package outside the datastore that knows the drivers' errors.
package transient

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	"github.com/lib/pq/pqerror"

	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// MariaDB errors a statement tried again later can get past: the server is
// busy, going away, read-only during a primary switch, or chose the
// statement as a lock's victim. The datastore's own retry (retryWrite) tries
// again at once only the lock conflicts that roll back; a caller backing off
// for longer, as an agent does, gets past a lock wait timeout too.
var mysqlErrors = map[uint16]bool{
	1020: true, // a row changed since the transaction's snapshot
	1040: true, // too many connections
	1047: true, // Galera node not ready
	1053: true, // server shutdown in progress
	1205: true, // lock wait timeout
	1213: true, // deadlock
	1290: true, // running with read_only (a primary being switched)
	1836: true, // read-only mode
	1927: true, // connection killed
	4031: true, // client disconnected by the server for inactivity
}

// Postgres errors of the same kinds, by SQLSTATE; the whole class 08
// (connection exception) is too.
var pqErrors = map[pqerror.Code]bool{
	"57P01": true, // admin shutdown
	"57P02": true, // crash shutdown
	"57P03": true, // cannot connect now (starting)
	"40001": true, // serialization failure
	"40P01": true, // deadlock
	"25006": true, // read-only transaction
}

// Is reports whether err is the store being unavailable for a while (a
// connection lost or refused, the database restarting or switching
// primaries, a lock conflict) rather than the request being wrong: the
// same request may succeed once tried again.
func Is(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, types.ErrDatabaseLocked) ||
		errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, mysql.ErrInvalidConn) ||
		errors.Is(err, sql.ErrConnDone) {
		return true
	}
	if myErr, ok := errors.AsType[*mysql.MySQLError](err); ok {
		return mysqlErrors[myErr.Number]
	}
	if pqErr, ok := errors.AsType[*pq.Error](err); ok {
		return pqErrors[pqErr.Code] || pqErr.Code.Class() == "08"
	}
	// an HTTP call failing (a forge, an addon) is not the store's
	if _, ok := errors.AsType[*url.Error](err); ok {
		return false
	}
	if _, ok := errors.AsType[*net.OpError](err); ok {
		return true
	}
	// SQLite reports its locks only as text
	msg := err.Error()
	return strings.Contains(msg, "database is locked") || strings.Contains(msg, "database table is locked")
}
