// Copyright 2023 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lint

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/urfave/cli/v3"

	"go.woodpecker-ci.org/woodpecker/v3/cli/common"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/jsonnet"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/linter"
	"go.woodpecker-ci.org/woodpecker/v3/shared/constant"
)

// Command exports the info command.
var Command = &cli.Command{
	Name:      "lint",
	Usage:     "lint a pipeline configuration file",
	ArgsUsage: "[path/to/.woodpecker.yaml]",
	Action:    lint,
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Sources: cli.EnvVars("WOODPECKER_PLUGINS_PRIVILEGED"),
			Name:    "plugins-privileged",
			Usage:   "allow plugins to run in privileged mode, if set empty, there is no",
			Config: cli.StringConfig{
				TrimSpace: true,
			},
		},
		&cli.StringSliceFlag{
			Sources: cli.EnvVars("WOODPECKER_PLUGINS_TRUSTED_CLONE"),
			Name:    "plugins-trusted-clone",
			Usage:   "plugins that are trusted to handle Git credentials in cloning steps",
			Value:   constant.TrustedClonePlugins,
			Config: cli.StringConfig{
				TrimSpace: true,
			},
		},
		&cli.BoolFlag{
			Sources: cli.EnvVars("WOODPECKER_LINT_STRICT"),
			Name:    "strict",
			Usage:   "treat warnings as errors",
		},
	},
}

func lint(ctx context.Context, c *cli.Command) error {
	return common.RunPipelineFunc(ctx, c, lintFile, lintDir)
}

func lintDir(_ context.Context, c *cli.Command, dir string) error {
	files, err := common.ReadConfigDir(dir)
	if err != nil {
		return err
	}
	workflows, err := jsonnet.Expand(files)
	if err != nil {
		return err
	}

	var errorStrings []string
	for _, w := range workflows {
		fmt.Println("#", path.Base(w.Name))
		if err := lintWorkflow(c, w.Name, w.Data); err != nil {
			errorStrings = append(errorStrings, err.Error())
		}
		fmt.Println("")
	}

	if len(errorStrings) != 0 {
		return fmt.Errorf("ERRORS: %s", strings.Join(errorStrings, "; "))
	}
	return nil
}

func lintFile(_ context.Context, c *cli.Command, file string) error {
	files, err := common.ReadConfigFile(file)
	if err != nil {
		return err
	}
	workflows, err := jsonnet.Expand(files)
	if err != nil {
		return err
	}

	var errs []error
	for _, w := range workflows {
		if len(workflows) > 1 {
			fmt.Println("#", path.Base(w.Name))
		}
		if err := lintWorkflow(c, w.Name, w.Data); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// lintWorkflow lints one workflow; a jsonnet config yields it as YAML.
func lintWorkflow(c *cli.Command, file string, buf []byte) error {
	rawConfig := string(buf)

	parsedConfig, err := yaml.ParseString(rawConfig)
	if err != nil {
		return err
	}

	config := &linter.WorkflowConfig{
		File:      path.Base(file),
		RawConfig: rawConfig,
		Workflow:  parsedConfig,
	}

	// TODO: lint multiple files at once to allow checks for sth like "depends_on" to work
	err = linter.New(
		linter.WithTrusted(linter.TrustedConfiguration{
			Network:  true,
			Volumes:  true,
			Security: true,
		}),
		linter.PrivilegedPlugins(c.StringSlice("plugins-privileged")),
		linter.WithTrustedClonePlugins(c.StringSlice("plugins-trusted-clone")),
	).Lint([]*linter.WorkflowConfig{config})
	if err != nil {
		str, err := FormatLintError(config.File, err, c.Bool("strict"))

		if str != "" {
			fmt.Print(str)
		}

		return err
	}

	fmt.Println("✅ Config is valid")
	return nil
}
