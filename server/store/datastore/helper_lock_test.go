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

package datastore

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// TestLockConflict: a MariaDB deadlock is tried again, wrapped or not; a
// lock wait timeout and other errors are not.
func TestLockConflict(t *testing.T) {
	deadlock := &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock; try restarting transaction"}
	timeout := &mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded; try restarting transaction"}
	for _, tt := range []struct {
		err   error
		retry bool
	}{
		{deadlock, true},
		{fmt.Errorf("CronApplyConfig: %w", deadlock), true},
		{timeout, false},
		{errors.New("Deadlock found when trying to get lock"), false}, // the text alone is no driver's error
		{errors.New("syntax error"), false},
		{nil, false},
	} {
		assert.Equal(t, tt.retry, isErrLockConflict(tt.err), "%v", tt.err)
	}

	for _, tt := range []struct {
		err   error
		tries int
	}{
		{deadlock, maxRetries},
		{timeout, 1},
		{types.ErrRecordNotExist, 1},
	} {
		tries := 0
		err := retryWrite(func() error { tries++; return tt.err })
		assert.ErrorIs(t, err, tt.err)
		assert.Equal(t, tt.tries, tries, "%v", tt.err)
	}

	// a deadlocked delete is the deadlock, not a missing row
	assert.ErrorIs(t, wrapDelete(0, deadlock), deadlock)
	assert.ErrorIs(t, wrapDelete(0, nil), types.ErrRecordNotExist)
}
