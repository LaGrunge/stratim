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

package scaleway

import (
	"errors"

	"github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"
)

var (
	ErrInvalidZone          = errors.New("invalid zone")
	ErrServerTypeNotFound   = errors.New("server type not found")
	ErrImageNotFound        = errors.New("no configured image resolves for server type arch")
	ErrNoMatchingServerType = errors.New("no configured server type matches requested capability")
	ErrInstanceNotFound     = errors.New("instance not found")
	ErrReservedTagPrefix    = errors.New("reserved tag prefix")
)

// deployCandidate is a fully-resolved server type + image pair ready for use
// in CreateServer. Candidates are built once in New() and filtered at deploy
// time by the requested Capability arch.
type deployCandidate struct {
	rawType    string
	zone       scw.Zone
	serverType *instance.ServerType
	// imageID is the zone- and arch-specific ID resolved from the configured
	// image name list. The first name that resolves for the server type's
	// architecture wins.
	imageID   string
	imageName string
}
