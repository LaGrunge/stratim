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
	"net/http"
	"strconv"
	"time"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
)

// StatusError tells the server whether a status request that failed with err
// may pass if sent again, from the HTTP status code the forge answered with
// (0 if none) and the headers (nil if not known): a 5xx, a 429, a 408 or no
// answer at all may, waiting as long as Retry-After asks; another 4xx will
// not, but a 401 or 403 may once the user logs in again.
func StatusError(code int, header http.Header, err error) error {
	switch {
	case err == nil:
		return nil
	case code < http.StatusBadRequest:
		return err // no answer, or an answer the client could not read
	case code >= http.StatusInternalServerError, code == http.StatusTooManyRequests, code == http.StatusRequestTimeout:
		return &types.StatusError{Err: err, RetryAfter: retryAfter(header)}
	case code == http.StatusUnauthorized, code == http.StatusForbidden:
		return &types.StatusError{Err: err, Final: true, Auth: true}
	default:
		return &types.StatusError{Err: err, Final: true}
	}
}

// maxRetryAfter bounds the wait a forge may ask for.
const maxRetryAfter = time.Hour

// ClampRetryAfter is the wait a forge asked for, within 0 and an hour.
func ClampRetryAfter(d time.Duration) time.Duration {
	return min(max(d, 0), maxRetryAfter)
}

// retryAfter is the wait a Retry-After header asks for, in seconds or until
// a date, up to maxRetryAfter; 0 if it asks for none.
func retryAfter(header http.Header) time.Duration {
	v := header.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		return time.Duration(min(max(secs, 0), int64(maxRetryAfter/time.Second))) * time.Second
	}
	if at, err := http.ParseTime(v); err == nil {
		return ClampRetryAfter(time.Until(at))
	}
	return 0
}
