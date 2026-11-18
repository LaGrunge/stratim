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

package rpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/rpc/proto"
)

// The store failing for a while reaches agents as codes.Unavailable, which
// they try again; other errors as they were.
func TestUnavailableOnTransientStoreError(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: proto.Woodpecker_Done_FullMethodName}
	call := func(err error) error {
		_, got := unavailableOnTransientStoreError(t.Context(), nil, info, func(context.Context, any) (any, error) {
			return nil, err
		})
		return got
	}

	assert.NoError(t, call(nil))
	assert.Equal(t, codes.Unavailable, status.Code(call(fmt.Errorf("load workflow: %w", mysql.ErrInvalidConn))))
	assert.Equal(t, codes.Unavailable, status.Code(call(&mysql.MySQLError{Number: 1213})))

	illegal := call(ErrAgentIllegalWorkflowReRunStateChange)
	assert.ErrorIs(t, illegal, ErrAgentIllegalWorkflowReRunStateChange, "not the store's: as it was")
	assert.Equal(t, codes.Unknown, status.Code(illegal))

	denied := status.Error(codes.PermissionDenied, "no")
	assert.Equal(t, denied, call(denied), "a status: as it was")
	assert.Equal(t, codes.Unknown, status.Code(call(errors.New("database"))))
}

// A batch of several steps' lines whose write fails for one step: the
// agent is told, to send the batch again; the other steps are written.
func TestWriteLogBatchFirstError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fail    string
		written []string
	}{
		{"first step fails", "a", []string{"b:0"}},
		{"last step fails", "b", []string{"a:0", "a:1"}},
		{"none fails", "", []string{"a:0", "a:1", "b:0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			written := []string{}
			err := writeLogBatch(&proto.LogRequest{LogEntries: []*proto.LogEntry{
				{StepUuid: "a", Line: 0}, {StepUuid: "a", Line: 1}, {StepUuid: "b", Line: 0},
			}}, func(step string, entries []*rpc.LogEntry) error {
				if step == tc.fail {
					return mysql.ErrInvalidConn
				}
				for _, e := range entries {
					written = append(written, fmt.Sprintf("%s:%d", step, e.Line))
				}
				return nil
			})
			if tc.fail == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, mysql.ErrInvalidConn)
			}
			assert.Equal(t, tc.written, written)
		})
	}
}
