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

package api

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/shared/ansi"
)

// downloadBufferSize is how much of a downloaded log is buffered before it goes to the client.
const downloadBufferSize = 64 * 1024

// downloadTruncated ends a download that lost its log store midway and could
// not be cut off.
const downloadTruncated = "--- log truncated: the log could not be read to the end\n"

// appendDownloadLine appends a log entry to dst as a line of a downloaded log:
// with its number as the UI shows it if lineNumbers, as stored if raw and as
// a terminal shows it otherwise.
func appendDownloadLine(dst []byte, entry *model.LogEntry, lineNumbers, raw bool) []byte {
	if lineNumbers {
		dst = fmt.Appendf(dst, "%6d\t", entry.Line+1)
	}
	if raw {
		dst = append(dst, entry.Data...)
	} else {
		dst = ansi.StripLine(dst, entry.Data)
	}
	return append(dst, '\n')
}

// clientError is a failed write to the client.
type clientError struct{ error }

func (e clientError) Unwrap() error { return e.error }

// queryBool reads an optional boolean query parameter.
func queryBool(c *gin.Context, name string) (bool, error) {
	v := c.Query(name)
	if v == "" {
		return false, nil
	}
	return strconv.ParseBool(v)
}

// abortDownload ends a download whose log store failed after part of the log
// was sent. The client must not take the part for the whole log, so the
// connection is closed before the chunked body ends; where the connection
// cannot be taken over (HTTP/2), or closing it is how an HTTP/1.0 body ends
// anyway, a last line says the log is truncated.
func abortDownload(c *gin.Context, w *bufio.Writer) {
	if !c.Request.ProtoAtLeast(1, 1) {
		// the body ends where the connection does
		_, _ = w.WriteString(downloadTruncated)
		_ = w.Flush()
		return
	}
	if w.Flush() != nil {
		return
	}
	c.Writer.Flush()

	// gin refuses to give up a connection it wrote a body to, net/http does not
	var rw http.ResponseWriter = c.Writer
	if u, ok := rw.(interface{ Unwrap() http.ResponseWriter }); ok {
		rw = u.Unwrap()
	}
	if conn, _, err := http.NewResponseController(rw).Hijack(); err == nil {
		if err := conn.Close(); err != nil {
			log.Debug().Err(err).Msg("could not close the connection of a truncated log download")
		}
		return
	}
	_, _ = io.WriteString(c.Writer, downloadTruncated)
}
