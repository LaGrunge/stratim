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

package watch

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v3"

	"go.woodpecker-ci.org/woodpecker/v3/cli/internal"
	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// defaultCauseWait is how long to wait for the cause of a failure by
// default.
const defaultCauseWait = 10 * time.Second

// maxArgs are the repository and the pipeline.
const maxArgs = 2

// Command is "pipeline watch".
var Command = &cli.Command{
	Name:      "watch",
	Usage:     "follow a pipeline, one line per event, and exit with how it went",
	ArgsUsage: "<repo-id|repo-full-name> [pipeline-number|last]",
	Description: `Follows the events of a pipeline over /api/v2 until the condition of --exit-on is met.

Exit codes: 0 the pipeline succeeded, or ended without failing with --exit-on failed;
1 it failed or timed out, or with --exit-on failed a step or workflow failed before its end;
2 it ended killed, canceled, declined or in an error; 3 a wrong usage, a server that
cannot be reached, does not serve /api/v2 or refuses the login; 4 --timeout passed;
130 interrupted.`,
	Action: run,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "exit-on",
			Usage: "exit when the pipeline finished, or on the first failure that is not ignored (finished|failed)",
			Value: ExitOnFinished,
		},
		&cli.StringFlag{
			Name:  "level",
			Usage: "a line per event, or the summary of the pipeline when it changed (line|summary)",
			Value: LevelLine,
		},
		&cli.StringFlag{
			Name:  "severity",
			Usage: "the least severity of the events printed (debug|info|notice|warning|error)",
			Value: woodpecker.SeverityNotice.String(),
		},
		&cli.DurationFlag{
			Name:  "timeout",
			Usage: "how long to watch at most, 0 for no limit",
		},
		&cli.DurationFlag{
			Name:  "cause-wait",
			Usage: "with --exit-on failed, how long to wait for the cause of the failure found in the log",
			Value: defaultCauseWait,
		},
	},
}

func run(ctx context.Context, c *cli.Command) error {
	opt, err := parseOptions(c)
	if err != nil {
		return cli.Exit(err.Error(), ExitError)
	}
	// the timeout and Ctrl-C bound the lookups too
	ctx, cancel := withTimeout(ctx, opt.Timeout)
	defer cancel()
	client, err := internal.NewClient(ctx, c)
	if err != nil {
		return cli.Exit(describe(err), ExitError)
	}
	target, err := resolveContext(ctx, client, c.Args().First(), c.Args().Get(1))
	if err != nil {
		code, err := exitFor(ctx, opt.Timeout, err)
		return cli.Exit(err.Error(), code)
	}

	opt.Out, opt.Err = c.Root().Writer, c.Root().ErrWriter
	opt.Location = time.Local
	code, err := Watch(ctx, client, target, opt)
	if err != nil {
		return cli.Exit(err.Error(), code)
	}
	if code != ExitSuccess {
		return cli.Exit("", code)
	}
	return nil
}

// resolveContext is resolve, given up on when the context ends: the
// lookups of the client take no context.
func resolveContext(ctx context.Context, client woodpecker.Client, repoArg, pipelineArg string) (Target, error) {
	type resolved struct {
		target Target
		err    error
	}
	done := make(chan resolved, 1)
	go func() {
		target, err := resolve(client, repoArg, pipelineArg)
		done <- resolved{target, err}
	}()
	select {
	case <-ctx.Done():
		return Target{}, ctx.Err()
	case got := <-done:
		return got.target, got.err
	}
}

func parseOptions(c *cli.Command) (Options, error) {
	opt := Options{
		ExitOn:    c.String("exit-on"),
		Level:     c.String("level"),
		Timeout:   c.Duration("timeout"),
		CauseWait: c.Duration("cause-wait"),
	}
	if opt.ExitOn != ExitOnFinished && opt.ExitOn != ExitOnFailed {
		return opt, fmt.Errorf("--exit-on: want %s or %s, not %q", ExitOnFinished, ExitOnFailed, opt.ExitOn)
	}
	if opt.Level != LevelLine && opt.Level != LevelSummary {
		return opt, fmt.Errorf("--level: want %s or %s, not %q", LevelLine, LevelSummary, opt.Level)
	}
	severity, err := woodpecker.ParseEventSeverity(c.String("severity"))
	if err != nil {
		return opt, fmt.Errorf("--severity: %w", err)
	}
	opt.Severity = severity
	if opt.Timeout < 0 || opt.CauseWait < 0 {
		return opt, errors.New("--timeout and --cause-wait cannot be negative")
	}
	if c.Args().Len() > maxArgs {
		return opt, errors.New("want a repository and a pipeline number or last")
	}
	return opt, nil
}

// resolve looks up the pipeline of a repository by its number, the latest
// one with "last" or none.
func resolve(client woodpecker.Client, repoArg, pipelineArg string) (Target, error) {
	repoID, err := internal.ParseRepo(client, repoArg)
	if err != nil {
		return Target{}, err
	}
	var number int64
	if pipelineArg == "" || pipelineArg == "last" {
		last, err := client.PipelineLast(repoID, woodpecker.PipelineLastOptions{})
		if err != nil {
			return Target{}, fmt.Errorf("cannot find the latest pipeline: %w", err)
		}
		number = last.Number
	} else {
		number, err = strconv.ParseInt(pipelineArg, 10, 64)
		if err != nil || number <= 0 {
			return Target{}, fmt.Errorf("want a pipeline number or last, not %q", pipelineArg)
		}
	}
	pipeline, err := client.Pipeline(repoID, number)
	if err != nil {
		return Target{}, fmt.Errorf("cannot find pipeline %d: %w", number, err)
	}

	name := repoArg
	if !strings.Contains(name, "/") {
		name = strconv.FormatInt(repoID, 10)
		if repo, err := client.Repo(repoID); err == nil {
			name = repo.FullName
		}
	}
	return Target{
		PipelineID: pipeline.ID,
		Name:       fmt.Sprintf("%s #%d", name, pipeline.Number),
		Status:     pipeline.Status,
	}, nil
}
