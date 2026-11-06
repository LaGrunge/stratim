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

package linter

import "crypto/sha256"

// Cache keeps what linters find in the raw text of a config, which is the
// same for every axis of a matrix: the schema, the parameters, the
// deprecations and bad habits, and the ${NAME} in commands. Linters given
// one cache (WithCache) read each config's text once; what depends on the
// parsed workflow, the options of the linter or the variables of the run
// is linted every time. A Cache is not safe for concurrent use.
type Cache struct {
	raw map[rawKey]*rawFindings
	// reads is how many configs it read (readRaw)
	reads int
}

// NewCache returns an empty Cache.
func NewCache() *Cache { return &Cache{raw: make(map[rawKey]*rawFindings)} }

// WithCache has the linter keep what it finds in raw config text in c, and
// read it from there.
func WithCache(c *Cache) Option {
	return func(linter *Linter) {
		linter.cache = c
	}
}

// Forget drops what c keeps of the configs of file, once no linter is to
// read them again.
func (c *Cache) Forget(file string) {
	if c == nil {
		return
	}
	for key := range c.raw {
		if key.file == file {
			delete(c.raw, key)
		}
	}
}

// rawKey is a config by its file and the hash of its text, so that the
// cache does not hold the text.
type rawKey struct {
	file string
	sum  [sha256.Size]byte
}

// rawFindings is what a config's raw text shows, whatever the run.
type rawFindings struct {
	// err: the findings of lintSchema, lintDeprecations, lintBadHabits and
	// lintParams, in this order
	err error
	// commandNames are the names ${NAME} in commands refers to without a
	// default, each once, in order; declared are the names of the
	// parameters and matrix axes (see lintCommandVariables)
	commandNames []string
	declared     map[string]bool
}

// findings returns what config's raw text shows (readRaw), read once per
// cache; a nil cache reads it every time.
func (c *Cache) findings(config *WorkflowConfig) *rawFindings {
	if c == nil {
		return readRaw(config)
	}
	key := rawKey{config.File, sha256.Sum256([]byte(config.RawConfig))}
	if found, ok := c.raw[key]; ok {
		return found
	}
	found := readRaw(config)
	c.raw[key] = found
	c.reads++
	return found
}
