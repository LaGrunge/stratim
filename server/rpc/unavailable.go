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

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.woodpecker-ci.org/woodpecker/v3/server/store/transient"
)

// errStoreUnavailable is what an agent is told when the store failed for a
// while: it tries the same call again.
const errStoreUnavailable = "the store is unavailable for now, try again"

// unavailableOnTransientStoreError answers codes.Unavailable for a call
// that failed on the store being unavailable for a while (see
// transient.Is), which agents try again until it succeeds;
// any other error would reach them as codes.Unknown, which they give up on,
// losing the step's or the workflow's end it reported.
func unavailableOnTransientStoreError(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err == nil || !transient.Is(err) {
		return resp, err
	}
	if _, isStatus := status.FromError(err); isStatus {
		return resp, err
	}
	log.Warn().Err(err).Str("method", info.FullMethod).Msg("store unavailable: the agent tries again")
	return nil, status.Error(codes.Unavailable, errStoreUnavailable)
}
