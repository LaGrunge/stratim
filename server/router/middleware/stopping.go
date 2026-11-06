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

package middleware

import (
	"context"

	"github.com/gin-gonic/gin"
)

const stoppingKey = "stopping"

// Stopping attaches to every request a channel that is closed once the
// server stops, from the context given: a long poll or a stream ends on it,
// so that the server's shutdown does not wait for it.
func Stopping(ctx context.Context) gin.HandlerFunc {
	done := ctx.Done()
	return func(c *gin.Context) {
		c.Set(stoppingKey, done)
		c.Next()
	}
}

// StoppingOf is the channel closed once the server stops; nil, never ready,
// when the request has none.
func StoppingOf(c *gin.Context) <-chan struct{} {
	if v, ok := c.Get(stoppingKey); ok {
		if done, ok := v.(<-chan struct{}); ok {
			return done
		}
	}
	return nil
}
