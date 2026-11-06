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

// Command synthetic-check is the last step of the synthetic pipeline
// (.woodpecker/synthetic.jsonnet): it reads that pipeline from the Stratim API
// and fails when a workflow broke its budget.
//
// In the pipeline it takes the API from STRATIM_API_URL and STRATIM_API_TOKEN
// and the pipeline from CI_REPO_ID and CI_PIPELINE_NUMBER. With -pipeline it
// reads a saved API response instead, to replay a run locally; the logs are
// then not checked.
//
// Exit codes: 0 within budgets, 1 a budget broken, 2 the check could not run.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/oauth2"

	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// The Stratim API, as the checker reads it in the pipeline.

const (
	// The time reading the pipeline may take.
	requestTimeout = 30 * time.Second
	// The time reading one step's log may take: the big one is hundreds of
	// megabytes.
	logRequestTimeout = 5 * time.Minute
)

type apiConfig struct {
	URL, Token   string
	Repo, Number int64
}

// httpClient returns a client that authenticates as cfg's token and gives up
// on a request after timeout.
func (cfg apiConfig) httpClient(timeout time.Duration) *http.Client {
	c := oauth2.NewClient(context.Background(), oauth2.StaticTokenSource(&oauth2.Token{AccessToken: cfg.Token}))
	c.Timeout = timeout
	return c
}

func apiConfigFromEnv(getenv func(string) string) (apiConfig, error) {
	var missing []string
	get := func(name string) string {
		v := getenv(name)
		if v == "" {
			missing = append(missing, name)
		}
		return v
	}
	cfg := apiConfig{URL: get("STRATIM_API_URL"), Token: get("STRATIM_API_TOKEN")}
	repo, number := get("CI_REPO_ID"), get("CI_PIPELINE_NUMBER")
	if len(missing) > 0 {
		return apiConfig{}, fmt.Errorf("missing %s", strings.Join(missing, ", "))
	}
	var err error
	if cfg.Repo, err = strconv.ParseInt(repo, 10, 64); err != nil {
		return apiConfig{}, fmt.Errorf("CI_REPO_ID: %w", err)
	}
	if cfg.Number, err = strconv.ParseInt(number, 10, 64); err != nil {
		return apiConfig{}, fmt.Errorf("CI_PIPELINE_NUMBER: %w", err)
	}
	return cfg, nil
}

func apiSource(cfg apiConfig) source {
	return func() (*woodpecker.Pipeline, error) {
		p, err := woodpecker.NewClient(cfg.URL, cfg.httpClient(requestTimeout)).Pipeline(cfg.Repo, cfg.Number)
		if err != nil {
			return nil, fmt.Errorf("get pipeline %d of repository %d: %w", cfg.Number, cfg.Repo, err)
		}
		return p, nil
	}
}

// apiLogSource counts the numbered lines of step logs from the Stratim API.
func apiLogSource(cfg apiConfig) logSource {
	return func(stepID int64) (count LogCount, err error) {
		err = readLog(cfg, stepID, func(r io.Reader) (err error) {
			count, err = CountLines(r)
			return err
		})
		return count, err
	}
}

// apiStatusSource reads what step logs from the Stratim API say
// STRATIM_PIPELINE_STATUS was.
func apiStatusSource(cfg apiConfig) statusSource {
	return func(stepID int64) (status string, err error) {
		err = readLog(cfg, stepID, func(r io.Reader) (err error) {
			status, err = PipelineStatus(r)
			return err
		})
		return status, err
	}
}

// readLog has read read the log of a step from the Stratim API, as the API
// returns it.
func readLog(cfg apiConfig, stepID int64, read func(io.Reader) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), logRequestTimeout)
	defer cancel()
	url := fmt.Sprintf("%s/api/repos/%d/logs/%d/%d", trimSlash(cfg.URL), cfg.Repo, cfg.Number, stepID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := cfg.httpClient(logRequestTimeout).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512)) //nolint:mnd
		return fmt.Errorf("%s: %s", resp.Status, body)
	}
	err = read(resp.Body)
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("reading the log took longer than %s", logRequestTimeout)
	}
	return err
}

func trimSlash(s string) string { return strings.TrimRight(s, "/") }
