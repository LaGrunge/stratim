// Copyright 2025 Woodpecker Authors
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
	"os"

	"github.com/urfave/cli/v3"
)

const category = "Scaleway"

//nolint:mnd
var ProviderFlags = []cli.Flag{
	&cli.StringFlag{
		Name:  "scaleway-access-key",
		Usage: "Scaleway IAM API Token Access Key",
		Sources: cli.NewValueSourceChain(
			cli.EnvVar("WOODPECKER_SCALEWAY_ACCESS_KEY"),
			cli.EnvVar("SCW_ACCESS_KEY"), // scaleway official naming
			cli.File(os.Getenv("WOODPECKER_SCALEWAY_ACCESS_KEY_FILE")),
		),
		Category: category,
	},
	&cli.StringFlag{
		Name:  "scaleway-secret-key",
		Usage: "Scaleway IAM API Token Secret Key",
		Sources: cli.NewValueSourceChain(
			cli.EnvVar("WOODPECKER_SCALEWAY_SECRET_KEY"),
			cli.EnvVar("SCW_SECRET_KEY"), // scaleway official naming
			cli.File(os.Getenv("WOODPECKER_SCALEWAY_SECRET_KEY_FILE")),
		),
		Category: category,
	},
	&cli.StringSliceFlag{
		Name: "scaleway-server-types",
		Usage: "Ordered list of server types to deploy, in \"type:zone\" format " +
			"(e.g. \"PRO2-XXS:fr-par-1\", \"COPARM1-2C-8G:fr-par-2\"). " +
			"Architecture is inferred from the server type. " +
			"On resource unavailability the next entry of the same architecture is tried.",
		Sources:  cli.EnvVars("WOODPECKER_SCALEWAY_SERVER_TYPES"),
		Category: category,
	},
	&cli.StringSliceFlag{
		Name: "scaleway-images",
		Usage: "Ordered list of image names (e.g. \"ubuntu_noble\", \"ubuntu_jammy\"). " +
			"The first image that resolves for the server type's architecture wins.",
		Sources:  cli.EnvVars("WOODPECKER_SCALEWAY_IMAGES"),
		Value:    []string{"ubuntu_noble"},
		Category: category,
	},
	&cli.StringFlag{
		Name:  "scaleway-project",
		Usage: "Scaleway Project ID in which to spawn the instances",
		Sources: cli.NewValueSourceChain(
			cli.EnvVar("WOODPECKER_SCALEWAY_PROJECT"),
			cli.EnvVar("SCW_DEFAULT_PROJECT_ID"), // scaleway official naming
		),
		Category: category,
	},
	&cli.StringSliceFlag{
		Name:     "scaleway-tags",
		Usage:    "Comma separated list of tags to uniquely identify the instances spawned",
		Sources:  cli.EnvVars("WOODPECKER_SCALEWAY_TAGS"),
		Category: category,
	},
	&cli.StringFlag{
		Name:     "scaleway-prefix",
		Usage:    "Prefix prepended before any Scaleway resource name",
		Sources:  cli.EnvVars("WOODPECKER_SCALEWAY_PREFIX"),
		Value:    "woodpecker-autoscaler",
		Category: category,
	},
	&cli.BoolFlag{
		Name:     "scaleway-enable-ipv6",
		Usage:    "Enable IPv6 for the instances",
		Sources:  cli.EnvVars("WOODPECKER_SCALEWAY_ENABLE_IPV6"),
		Category: category,
	},
	&cli.Uint64Flag{
		Name:     "scaleway-storage-size",
		Usage:    "How much storage to provision for your agents in GB",
		Sources:  cli.EnvVars("WOODPECKER_SCALEWAY_STORAGE_SIZE"),
		Value:    25,
		Category: category,
	},
	&cli.StringFlag{
		Name:     "scaleway-storage-type",
		Usage:    "The storage type to provision",
		Sources:  cli.EnvVars("WOODPECKER_SCALEWAY_STORAGE_TYPE"),
		Value:    "l_ssd",
		Category: category,
	},
}
