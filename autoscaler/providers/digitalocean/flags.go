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

package digitalocean

import (
	"os"

	"github.com/urfave/cli/v3"
)

const category = "DigitalOcean"

var ProviderFlags = []cli.Flag{
	&cli.StringFlag{
		Name:  "digitalocean-api-token",
		Usage: "DigitalOcean API token",
		Sources: cli.NewValueSourceChain(
			cli.EnvVar("WOODPECKER_DIGITALOCEAN_API_TOKEN"),
			cli.File(os.Getenv("WOODPECKER_DIGITALOCEAN_API_TOKEN_FILE")),
		),
		Category: category,
	},
	&cli.StringFlag{
		Name:     "digitalocean-region",
		Usage:    "DigitalOcean region slug",
		Value:    "nyc1",
		Sources:  cli.EnvVars("WOODPECKER_DIGITALOCEAN_REGION"),
		Category: category,
	},
	&cli.StringFlag{
		Name:     "digitalocean-size",
		Usage:    "DigitalOcean droplet size slug",
		Value:    "s-1vcpu-1gb",
		Sources:  cli.EnvVars("WOODPECKER_DIGITALOCEAN_SIZE"),
		Category: category,
	},
	&cli.StringSliceFlag{
		Name:     "digitalocean-ssh-keys",
		Usage:    "names or fingerprints of DigitalOcean SSH keys",
		Sources:  cli.EnvVars("WOODPECKER_DIGITALOCEAN_SSH_KEYS"),
		Category: category,
	},
	&cli.StringFlag{
		Name:     "digitalocean-image",
		Usage:    "DigitalOcean image slug or name",
		Value:    "ubuntu-24-04-x64",
		Sources:  cli.EnvVars("WOODPECKER_DIGITALOCEAN_IMAGE"),
		Category: category,
	},
	&cli.StringSliceFlag{
		Name:     "digitalocean-tags",
		Usage:    "additional DigitalOcean droplet tags",
		Sources:  cli.EnvVars("WOODPECKER_DIGITALOCEAN_TAGS"),
		Category: category,
	},
	&cli.BoolFlag{
		Name:     "digitalocean-public-ipv4-enable",
		Usage:    "enable access to internet via IPv4 for agents; when disabled digitalocean-nat-gateway is required so agents can reach the server",
		Value:    true,
		Sources:  cli.EnvVars("WOODPECKER_DIGITALOCEAN_PUBLIC_IPV4_ENABLE"),
		Category: category,
	},
	&cli.StringFlag{
		Name:     "digitalocean-nat-gateway",
		Usage:    "name or ID of an existing VPC NAT gateway (paid); required when public IPv4 is disabled, agents are placed in the gateway's VPC",
		Sources:  cli.EnvVars("WOODPECKER_DIGITALOCEAN_NAT_GATEWAY"),
		Category: category,
	},
	&cli.BoolFlag{
		Name:     "digitalocean-public-ipv6-enable",
		Usage:    "enable public IPv6 on the agents' public interface (requires public IPv4, DigitalOcean has no IPv6-only droplets)",
		Value:    true,
		Sources:  cli.EnvVars("WOODPECKER_DIGITALOCEAN_PUBLIC_IPV6_ENABLE"),
		Category: category,
	},
}
