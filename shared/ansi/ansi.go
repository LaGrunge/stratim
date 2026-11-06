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

// Package ansi turns a terminal log line into the plain text a terminal
// would leave on screen.
package ansi

import "unicode/utf8"

const (
	esc = 0x1b
	bel = 0x07
	del = 0x7f
)

// StripLine appends to dst the visible text of one log line and returns the
// extended slice. It removes
//   - CSI sequences (ESC [ parameters intermediates final), colors and
//     cursor moves included;
//   - OSC, DCS, SOS, PM and APC strings (ESC ], ESC P, …) up to BEL, ESC \
//     or the ESC of the next sequence, hyperlinks and window titles included;
//   - the other escape sequences (ESC 7, ESC ( B, ESC =, …);
//   - control characters other than \t, and DEL.
//
// A carriage return, a cursor move to column 1 (ESC [ G, ESC [ 1 G) and an
// erase of the whole line (ESC [ 2 K) rewrite the line on a terminal, so of
// the parts they cut the line into only the last one with visible text
// survives: a progress bar " 10%\r 20%\r" keeps its final state " 20%",
// "ERROR: disk full\r   " keeps the error, and a CRLF ending is dropped. A line
// without visible text keeps its last part as is. A backspace deletes the rune
// before it within its part, so a spinner "|\b/\b-\bdone" leaves "done".
//
// A sequence cut off by the end of the line is dropped. Every other byte is
// copied untouched, so valid UTF-8 stays valid.
func StripLine(dst, line []byte) []byte {
	s := lineState{dst: dst, base: len(dst), segStart: len(dst)}
	for i := 0; i < len(line); {
		b := line[i]
		switch {
		case b == esc:
			var reset bool
			i, reset = skipEscape(line, i)
			if reset {
				s.cut()
			}
		case b == '\r':
			s.cut()
			i++
		case b == '\b':
			if len(s.dst) > s.segStart {
				_, size := utf8.DecodeLastRune(s.dst[s.segStart:])
				s.dst = s.dst[:len(s.dst)-size]
			}
			i++
		case b == '\t' || (b >= ' ' && b != del):
			s.dst = append(s.dst, b)
			i++
		default:
			i++
		}
	}
	return s.finish()
}

// lineState collects the parts of a line in dst[base:].
type lineState struct {
	dst                []byte
	base               int
	segStart           int // where the current part begins
	keepStart, keepEnd int // the last finished part with visible text
	kept               bool
}

// cut ends the current part and starts a new one.
func (s *lineState) cut() {
	if hasText(s.dst[s.segStart:]) {
		s.keepStart, s.keepEnd, s.kept = s.segStart, len(s.dst), true
	}
	s.segStart = len(s.dst)
}

// finish leaves only the part to keep after base.
func (s *lineState) finish() []byte {
	if hasText(s.dst[s.segStart:]) || !s.kept {
		s.keepStart, s.keepEnd = s.segStart, len(s.dst)
	}
	n := copy(s.dst[s.base:], s.dst[s.keepStart:s.keepEnd])
	return s.dst[:s.base+n]
}

func hasText(b []byte) bool {
	for _, c := range b {
		if c != ' ' && c != '\t' {
			return true
		}
	}
	return false
}

// skipEscape returns the index right after the escape sequence that starts
// with the ESC at line[i], and whether the sequence rewrites the line.
func skipEscape(line []byte, i int) (int, bool) {
	i++
	if i >= len(line) {
		return i, false
	}
	switch b := line[i]; {
	case b == '[':
		return skipCSI(line, i+1)
	case b == ']' || b == 'P' || b == 'X' || b == '^' || b == '_':
		return skipString(line, i+1), false
	case b >= 0x20 && b <= 0x2f:
		// intermediates, then a final byte
		for i < len(line) && line[i] >= 0x20 && line[i] <= 0x2f {
			i++
		}
		if i < len(line) && line[i] >= 0x30 && line[i] <= 0x7e {
			i++
		}
		return i, false
	case b >= 0x30 && b <= 0x7e:
		return i + 1, false
	default:
		// not a sequence, only the ESC goes
		return i, false
	}
}

// skipCSI skips the parameter, intermediate and final bytes of a CSI
// sequence; a byte outside those ranges ends it and is kept. It reports
// whether the sequence moves to column 1 or erases the whole line.
func skipCSI(line []byte, i int) (int, bool) {
	start := i
	for i < len(line) && line[i] >= 0x30 && line[i] <= 0x3f {
		i++
	}
	params := line[start:i]
	inter := i
	for i < len(line) && line[i] >= 0x20 && line[i] <= 0x2f {
		i++
	}
	if i >= len(line) || line[i] < 0x40 || line[i] > 0x7e {
		return i, false
	}
	final := line[i]
	reset := inter == i &&
		(final == 'G' && (len(params) == 0 || isParam(params, '1')) || final == 'K' && isParam(params, '2'))
	return i + 1, reset
}

func isParam(params []byte, p byte) bool {
	return len(params) == 1 && params[0] == p
}

// skipString skips a control string up to and including its terminator, BEL
// or ESC \. Any other ESC ends the string and starts the next sequence.
func skipString(line []byte, i int) int {
	for ; i < len(line); i++ {
		switch line[i] {
		case bel:
			return i + 1
		case esc:
			if i+1 < len(line) && line[i+1] == '\\' {
				return i + 1 + 1
			}
			return i
		}
	}
	return i
}
