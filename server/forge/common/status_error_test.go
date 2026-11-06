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

package common

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
)

func TestStatusError(t *testing.T) {
	errFailed := errors.New("failed")
	header := func(retryAfter string) http.Header {
		h := http.Header{}
		h.Set("Retry-After", retryAfter)
		return h
	}
	tests := []struct {
		name      string
		code      int
		header    http.Header
		plain     bool // not a StatusError
		final     bool
		auth      bool
		wantAfter time.Duration // at least
	}{
		{name: "no answer", plain: true},
		{name: "unreadable answer", code: http.StatusCreated, plain: true},
		{name: "bad gateway", code: http.StatusBadGateway},
		{name: "unavailable, retry after seconds", code: http.StatusServiceUnavailable, header: header("42"), wantAfter: 42 * time.Second},
		{name: "rate limited, retry after a date", code: http.StatusTooManyRequests, header: header(time.Now().Add(time.Minute).UTC().Format(http.TimeFormat)), wantAfter: 50 * time.Second},
		{name: "rate limited, garbled retry after", code: http.StatusTooManyRequests, header: header("soon")},
		{name: "rate limited for ages", code: http.StatusTooManyRequests, header: header("99999999999999999"), wantAfter: maxRetryAfter},
		{name: "rate limited till year 9999", code: http.StatusTooManyRequests, header: header("Fri, 31 Dec 9999 23:59:59 GMT"), wantAfter: maxRetryAfter},
		{name: "rate limited, negative retry after", code: http.StatusTooManyRequests, header: header("-5")},
		{name: "request timeout", code: http.StatusRequestTimeout},
		{name: "not found", code: http.StatusNotFound, final: true},
		{name: "unprocessable", code: http.StatusUnprocessableEntity, final: true},
		{name: "unauthorized", code: http.StatusUnauthorized, final: true, auth: true},
		{name: "forbidden", code: http.StatusForbidden, final: true, auth: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := StatusError(tt.code, tt.header, errFailed)
			require.ErrorIs(t, err, errFailed)
			var se *types.StatusError
			if tt.plain {
				assert.False(t, errors.As(err, &se))
				return
			}
			require.ErrorAs(t, err, &se)
			assert.Equal(t, tt.final, se.Final)
			assert.Equal(t, tt.auth, se.Auth)
			assert.GreaterOrEqual(t, se.RetryAfter, tt.wantAfter)
			assert.LessOrEqual(t, se.RetryAfter, maxRetryAfter)
			if tt.wantAfter == 0 {
				assert.Zero(t, se.RetryAfter)
			}
		})
	}
	assert.NoError(t, StatusError(http.StatusNotFound, nil, nil))
}
