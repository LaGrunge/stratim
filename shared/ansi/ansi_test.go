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

package ansi

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestStripLine(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "hello world", "hello world"},
		{"empty", "", ""},
		{"tab kept", "a\tb", "a\tb"},
		{"sgr color", "\x1b[31merror\x1b[0m: boom", "error: boom"},
		{"sgr reset short", "\x1b[mtext\x1b[1;4m!", "text!"},
		{"256 color", "\x1b[38;5;208morange\x1b[39m", "orange"},
		{"truecolor", "\x1b[38;2;255;100;0mrgb\x1b[48;2;0;0;0m bg\x1b[0m", "rgb bg"},
		{"cursor moves", "\x1b[2K\x1b[1A\x1b[10Gdone\x1b[?25h", "done"},
		{"private mode", "\x1b[?1049hscreen\x1b[?1049l", "screen"},
		{"osc hyperlink st", "see \x1b]8;;https://example.com\x1b\\the docs\x1b]8;;\x1b\\ now", "see the docs now"},
		{"osc title bel", "\x1b]0;my title\x07prompt$", "prompt$"},
		{"dcs string", "a\x1bPq#0;2;0;0;0\x1b\\b", "ab"},
		{"two-byte escapes", "\x1b7saved\x1b8\x1b=\x1bM", "saved"},
		{"charset designation", "\x1b(Bascii\x1b)0", "ascii"},
		{"truncated csi at end", "text\x1b[38;5", "text"},
		{"truncated osc at end", "text\x1b]8;;https://exa", "text"},
		{"lone esc at end", "text\x1b", "text"},
		{"csi broken by esc", "a\x1b[31\x1b[0mb", "ab"},
		{"control chars dropped", "be\x07ll\x00 \x01text\x7f", "bell text"},
		{"progress bar", "  10%\r  20%\r 100%", " 100%"},
		{"progress trailing cr", "  10%\r  20%\r", "  20%"},
		{"crlf", "windows line\r", "windows line"},
		{"cr then erase line", "Receiving 50%\r\x1b[KReceiving 100%, done.", "Receiving 100%, done."},
		{"only cr", "\r\r", ""},
		{"whitespace after cr", "ERROR: disk full\r   ", "ERROR: disk full"},
		{"tab after cr", "ERROR\r\t\r", "ERROR"},
		{"whitespace only line", "   ", "   "},
		{"whitespace only parts", " \r  ", "  "},
		{"column 1 resets", "50%\x1b[1G100%", "100%"},
		{"column reset no param", "50%\x1b[G100%", "100%"},
		{"erase line resets", "50%\x1b[2K\x1b[1G100%", "100%"},
		{"erase line alone resets", "50%\x1b[2K100%", "100%"},
		{"erase to end after column reset", "downloading 50%\x1b[1G\x1b[0Kdone", "done"},
		{"erase to end alone", "abc\x1b[Kdef", "abcdef"},
		{"column 5 does not reset", "abc\x1b[5Gdef", "abcdef"},
		{"osc ended by csi", "\x1b]0;t\x1b[31mred", "red"},
		{"osc ended by other esc", "\x1b]8;;x\x1b7text", "text"},
		{"backspace spinner", "|\b/\b-\bdone", "done"},
		{"backspace deletes a rune", "日本語\b!", "日本!"},
		{"backspace at part start", "\bab\r\bcd", "cd"},
		{"backspace keeps earlier part", "ok\r\b", "ok"},
		{"utf8 next to escapes", "\x1b[32m✓\x1b[0m тест 日本\x1b[1m語\x1b[0m 🚀", "✓ тест 日本語 🚀"},
		{"utf8 after cr", "загрузка 1%\rзагрузка 100%", "загрузка 100%"},
		{"invalid utf8 untouched", "a\xffb", "a\xffb"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(StripLine(nil, []byte(tt.in)))
			assert.Equal(t, tt.want, got)
			if utf8.ValidString(tt.in) {
				assert.True(t, utf8.ValidString(got))
			}
		})
	}
}

func TestStripLineAppends(t *testing.T) {
	dst := []byte("   1\t")
	dst = StripLine(dst, []byte("old\r\x1b[1mnew\x1b[0m"))
	assert.Equal(t, "   1\tnew", string(dst))

	dst = StripLine(dst[:0], []byte("\r"))
	assert.Empty(t, dst)
}
