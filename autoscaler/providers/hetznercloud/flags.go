// Copyright 2023 Woodpecker Authors
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
	"os"

	"github.com/urfave/cli/v3"
)

const category = "Hetzner Cloud"

var ProviderFlags = []cli.Flag{
	&cli.StringFlag{
		Name:  "hetznercloud-api-token",
		Usage: "hetzner cloud api token",
		Sources: cli.NewValueSourceChain(
			cli.EnvVar("WOODPECKER_HETZNERCLOUD_API_TOKEN"),
			cli.File(os.Getenv("WOODPECKER_HETZNERCLOUD_API_TOKEN_FILE")),
		),
		Category: category,
	},
	&cli.StringSliceFlag{
		Name:     "hetznercloud-server-type",
		Value:    []string{"cx11:nbg1"},
		Usage:    "ordered list of hetzner cloud server types as 'type' or 'type:location'; if a type is unavailable, the next one of the same architecture is tried",
		Sources:  cli.EnvVars("WOODPECKER_HETZNERCLOUD_SERVER_TYPE"),
		Category: category,
	},
	&cli.StringSliceFlag{
		Name:     "hetznercloud-ssh-keys",
		Usage:    "names of hetzner cloud ssh keys",
		Sources:  cli.EnvVars("WOODPECKER_HETZNERCLOUD_SSH_KEYS"),
		Category: category,
	},
	&cli.StringFlag{
		Name:     "hetznercloud-image",
		Value:    "ubuntu-24.04",
		Usage:    "hetzner cloud image",
		Sources:  cli.EnvVars("WOODPECKER_HETZNERCLOUD_IMAGE"),
		Category: category,
	},
	&cli.StringSliceFlag{
		Name:     "hetznercloud-labels",
		Usage:    "hetzner cloud server labels",
		Sources:  cli.EnvVars("WOODPECKER_HETZNERCLOUD_LABELS"),
		Category: category,
	},
	&cli.StringSliceFlag{
		Name:     "hetznercloud-firewalls",
		Usage:    "names of hetzner cloud firewalls",
		Sources:  cli.EnvVars("WOODPECKER_HETZNERCLOUD_FIREWALLS"),
		Category: category,
	},
	&cli.StringSliceFlag{
		Name:     "hetznercloud-networks",
		Usage:    "names of hetzner cloud networks",
		Sources:  cli.EnvVars("WOODPECKER_HETZNERCLOUD_NETWORKS"),
		Category: category,
	},
	&cli.BoolFlag{
		Name:     "hetznercloud-public-ipv4-enable",
		Value:    true,
		Usage:    "enables public ipv4 network for agents",
		Sources:  cli.EnvVars("WOODPECKER_HETZNERCLOUD_PUBLIC_IPV4_ENABLE"),
		Category: category,
	},
	&cli.BoolFlag{
		Name:     "hetznercloud-public-ipv6-enable",
		Value:    true,
		Usage:    "enables public ipv6 network for agents",
		Sources:  cli.EnvVars("WOODPECKER_HETZNERCLOUD_PUBLIC_IPV6_ENABLE"),
		Category: category,
	},
}
