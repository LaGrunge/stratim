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

package yaml

import (
	"errors"
	"strings"

	"codeberg.org/6543/xyaml/v2"
	"go.yaml.in/yaml/v4"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/types"
)

// PlainDoc is a YAML document parsed once with no anchor, alias or merge
// key, such as a workflow a jsonnet config evaluated to: Unmarshal would
// merge no sequence of it, so it can be decoded as it is, as often as
// needed, and its text need not be parsed again. Only ParsePlain makes
// one; a PlainDoc is never changed.
type PlainDoc struct {
	root *yaml.Node
}

// errNoPlainDoc is the error of a PlainDoc that ParsePlain did not make.
var errNoPlainDoc = errors.New("no parsed document")

// ParsePlain parses data, if it is a plain document: not empty, with no
// anchor, alias or merge key. Otherwise ok is false, and data is to be
// read as text (Unmarshal, ParseBytes), which reports what is wrong.
func ParsePlain(data []byte) (doc PlainDoc, ok bool) {
	root := new(yaml.Node)
	if err := yaml.Unmarshal(data, root); err != nil {
		return PlainDoc{}, false
	}
	if root.Kind != yaml.DocumentNode || len(root.Content) != 1 || !plain(root) {
		return PlainDoc{}, false
	}
	return PlainDoc{root: root}, true
}

func plain(n *yaml.Node) bool {
	if n.Kind == yaml.AliasNode || n.Anchor != "" || n.Tag == mergeTag {
		return false
	}
	for _, c := range n.Content {
		if !plain(c) {
			return false
		}
	}
	return true
}

// mergeTag is the tag of a merge key (<<), as xyaml reads it.
const mergeTag = "!!merge"

// Decode is Unmarshal for the document: it fails where Unmarshal would on
// its text (a document nested deeper than Woodpecker supports) and decodes
// it into out.
func (d PlainDoc) Decode(out any) error {
	if d.root == nil {
		return errNoPlainDoc
	}
	if err := checkDepth(d.root, 0); err != nil {
		return err
	}
	return d.root.Decode(out)
}

// Workflow is ParseBytes for the document.
func (d PlainDoc) Workflow() (*types.Workflow, error) {
	out := new(types.Workflow)
	if err := d.Decode(out); err != nil {
		return nil, err
	}
	return out, nil
}

// Substitute returns a copy of the document with environ substituted into
// every string, as metadata.SubstituteValues does to its text; the
// document itself stays as it is, so that one serves the axes of a matrix,
// each with a copy of its own. The copy shares with the document what
// nothing was substituted into: neither is ever changed.
func (d PlainDoc) Substitute(environ map[string]string) (PlainDoc, error) {
	if d.root == nil {
		return PlainDoc{}, errNoPlainDoc
	}
	root, err := substituteShared(d.root, environ)
	if err != nil {
		return PlainDoc{}, err
	}
	return PlainDoc{root: root}, nil
}

// substituteShared returns n with environ substituted into its strings: n
// itself if nothing changed under it, or else a copy of n, and of the
// nodes from it down to what changed. With no alias in a plain tree, a
// node is reached only from its parent.
func substituteShared(n *yaml.Node, environ map[string]string) (*yaml.Node, error) {
	if n.Kind == yaml.ScalarNode {
		if !strings.Contains(n.Value, "$") {
			return n, nil
		}
		cp := *n
		if err := metadata.SubstituteNode(&cp, environ); err != nil {
			return nil, err
		}
		if cp.Value == n.Value {
			return n, nil
		}
		return &cp, nil
	}
	var content []*yaml.Node // the copy's, once a child changed
	for i, c := range n.Content {
		sc, err := substituteShared(c, environ)
		if err != nil {
			return nil, err
		}
		if sc != c && content == nil {
			content = make([]*yaml.Node, len(n.Content))
			copy(content, n.Content)
		}
		if content != nil {
			content[i] = sc
		}
	}
	if content == nil {
		return n, nil
	}
	cp := *n
	cp.Content = content
	return &cp, nil
}

// checkDepth fails as xyaml's sequence merging does on a plain tree, which
// it walks to a limited depth: a document, the values of a mapping and the
// items of a sequence each a level deeper. It mirrors mergeSequences of
// codeberg.org/6543/xyaml/v2 v2.0.1 (merge.go); TestDecodeDepth checks
// that they agree, so a change there shows.
func checkDepth(n *yaml.Node, depth uint16) error {
	if depth == maxMergeDepth {
		return xyaml.ErrMaxDepth
	}
	switch n.Kind {
	case yaml.DocumentNode:
		return checkDepth(n.Content[0], depth+1)
	case yaml.MappingNode:
		if len(n.Content)%2 != 0 {
			return xyaml.ErrBrokenMappingNode
		}
		for i := 1; i < len(n.Content); i += 2 {
			if err := checkDepth(n.Content[i], depth+1); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		for _, c := range n.Content {
			if err := checkDepth(c, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}
