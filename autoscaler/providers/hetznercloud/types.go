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

package hetznercloud

import (
	"errors"

	"github.com/hetznercloud/hcloud-go/v2/hcloud"
)

var (
	ErrIllegalLabelPrefix   = errors.New("illegal label prefix")
	ErrImageNotFound        = errors.New("image not found")
	ErrSSHKeyNotFound       = errors.New("SSH key not found")
	ErrNetworkNotFound      = errors.New("network not found")
	ErrFirewallNotFound     = errors.New("firewall not found")
	ErrServerTypeNotFound   = errors.New("server type not found")
	ErrLocationNotSupported = errors.New("location not available for server type")
	ErrNoMatchingServerType = errors.New("no configured server type matches requested capability")
)

type deployCandidate struct {
	location   *hcloud.Location
	serverType *hcloud.ServerType
	image      *hcloud.Image
}
