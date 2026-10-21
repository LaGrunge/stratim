package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeObject is an object stored by fakeS3, with the headers it was uploaded with.
type fakeObject struct {
	body   []byte
	header http.Header
}

// fakeS3 is a path-style S3 endpoint that answers HeadObject and PutObject.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]fakeObject
	puts    int
	// putFailures is how many PutObject calls fail with 400 before one succeeds.
	putFailures int
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := strings.TrimPrefix(r.URL.Path, "/")
	switch r.Method {
	case http.MethodHead:
		if _, ok := f.objects[key]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		f.puts++
		if f.putFailures > 0 {
			f.putFailures--
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `<Error><Code>InvalidRequest</Code><Message>injected</Message></Error>`)
			return
		}
		body, err := readPayload(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.objects[key] = fakeObject{body: body, header: r.Header.Clone()}
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// readPayload returns the request body, decoding the aws-chunked framing the SDK
// uses when it sends a trailing checksum.
func readPayload(r *http.Request) ([]byte, error) {
	if !strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		return io.ReadAll(r.Body)
	}
	var out bytes.Buffer
	br := bufio.NewReader(r.Body)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.ParseInt(strings.SplitN(strings.TrimSpace(line), ";", 2)[0], 16, 64)
		if err != nil {
			return nil, err
		}
		if size == 0 {
			return out.Bytes(), nil
		}
		if _, err := io.CopyN(&out, br, size); err != nil {
			return nil, err
		}
		if _, err := br.ReadString('\n'); err != nil {
			return nil, err
		}
	}
}

func (f *fakeS3) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (f *fakeS3) object(t *testing.T, key string) fakeObject {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[key]
	require.True(t, ok, "object %q was not uploaded", key)
	return obj
}

func startFakeS3(t *testing.T) (*fakeS3, *Plugin) {
	t.Helper()
	// keep the developer's ~/.aws and environment out of the SDK config
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "")

	fake := &fakeS3{objects: map[string]fakeObject{}}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	return fake, &Plugin{
		Endpoint:  srv.URL,
		Key:       "key",
		Secret:    "secret",
		Bucket:    "bucket",
		Region:    "us-east-1",
		PathStyle: true,
	}
}

// writeTree creates the files under dir, keyed by slash-separated relative path.
func writeTree(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
	}
}

func TestMatches(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{
		"a.txt":       "a",
		"b.log":       "b",
		"sub/c.txt":   "c",
		"sub/d.debug": "d",
	})

	got, err := matches(filepath.Join(dir, "**", "*"), nil)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		filepath.Join(dir, "a.txt"),
		filepath.Join(dir, "b.log"),
		filepath.Join(dir, "sub"),
		filepath.Join(dir, "sub", "c.txt"),
		filepath.Join(dir, "sub", "d.debug"),
	}, got)

	got, err = matches(filepath.Join(dir, "**", "*"), []string{
		filepath.Join(dir, "*.log"),
		filepath.Join(dir, "**", "*.debug"),
	})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		filepath.Join(dir, "a.txt"),
		filepath.Join(dir, "sub"),
		filepath.Join(dir, "sub", "c.txt"),
	}, got)

	got, err = matches(filepath.Join(dir, "*.txt"), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{filepath.Join(dir, "a.txt")}, got)
}

func TestMatchExtension(t *testing.T) {
	m := map[string]string{`\.js$`: "application/javascript"}
	assert.Equal(t, "application/javascript", matchExtension("dist/app.js", m))
	assert.Empty(t, matchExtension("dist/app.json", m))
	assert.Empty(t, matchExtension("dist/app.js", nil))
}

func TestExecUploadsMatchedFiles(t *testing.T) {
	fake, p := startFakeS3(t)
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{
		"index.html":    "<html></html>",
		"app.js":        "console.log(1)",
		"assets/a.css":  "body{}",
		"assets/blob":   "raw",
		"skip/skip.txt": "no",
	})

	p.Source = filepath.Join(dir, "**", "*")
	p.Exclude = []string{filepath.Join(dir, "skip", "**")}
	p.StripPrefix = dir + string(filepath.Separator)
	p.Target = "/builds/42"
	p.Access = "public-read"
	p.ContentType = map[string]string{`\.js$`: "text/x-custom-js"}
	p.CacheControl = map[string]string{`\.css$`: "max-age=3600"}
	p.ContentEncoding = map[string]string{`\.html$`: "identity"}
	p.Encryption = "aws:kms"
	p.EncryptionKey = "kms-key"
	p.StorageClass = "STANDARD_IA"

	require.NoError(t, p.Exec(context.Background()))

	assert.Equal(t, []string{
		"bucket/builds/42/app.js",
		"bucket/builds/42/assets/a.css",
		"bucket/builds/42/assets/blob",
		"bucket/builds/42/index.html",
	}, fake.keys())

	html := fake.object(t, "bucket/builds/42/index.html")
	assert.Equal(t, "<html></html>", string(html.body))
	assert.Equal(t, "text/html; charset=utf-8", html.header.Get("Content-Type"))
	assert.Contains(t, html.header.Get("Content-Encoding"), "identity")
	assert.Equal(t, "public-read", html.header.Get("X-Amz-Acl"))
	assert.Equal(t, "aws:kms", html.header.Get("X-Amz-Server-Side-Encryption"))
	assert.Equal(t, "kms-key", html.header.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"))
	assert.Equal(t, "STANDARD_IA", html.header.Get("X-Amz-Storage-Class"))

	assert.Equal(t, "text/x-custom-js", fake.object(t, "bucket/builds/42/app.js").header.Get("Content-Type"))
	assert.Equal(t, "max-age=3600", fake.object(t, "bucket/builds/42/assets/a.css").header.Get("Cache-Control"))
	assert.Equal(t, "application/octet-stream", fake.object(t, "bucket/builds/42/assets/blob").header.Get("Content-Type"))
}

func TestExecDryRunUploadsNothing(t *testing.T) {
	fake, p := startFakeS3(t)
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"a.txt": "a"})

	p.Source = filepath.Join(dir, "*")
	p.DryRun = true

	require.NoError(t, p.Exec(context.Background()))
	assert.Empty(t, fake.keys())
	assert.Zero(t, fake.puts)
}

func TestExecOverwrite(t *testing.T) {
	fake, p := startFakeS3(t)
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"a.txt": "new"})
	fake.objects["bucket/a.txt"] = fakeObject{body: []byte("old")}

	p.Source = filepath.Join(dir, "a.txt")
	p.StripPrefix = dir + string(filepath.Separator)

	require.NoError(t, p.Exec(context.Background()))
	assert.Equal(t, "old", string(fake.object(t, "bucket/a.txt").body), "an existing object must be kept without overwrite")
	assert.Zero(t, fake.puts)

	p.Overwrite = true
	require.NoError(t, p.Exec(context.Background()))
	assert.Equal(t, "new", string(fake.object(t, "bucket/a.txt").body))
}

func TestExecRetriesFailedUpload(t *testing.T) {
	fake, p := startFakeS3(t)
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"a.txt": "a"})

	p.Source = filepath.Join(dir, "a.txt")
	p.StripPrefix = dir + string(filepath.Separator)
	p.RetryCount = 2

	// a 400 is not retried by the SDK, so each failure costs one plugin retry
	fake.putFailures = 2
	require.NoError(t, p.Exec(context.Background()))
	assert.Equal(t, 3, fake.puts)
	assert.Equal(t, "a", string(fake.object(t, "bucket/a.txt").body))

	fake.objects = map[string]fakeObject{}
	fake.puts = 0
	fake.putFailures = 3
	require.Error(t, p.Exec(context.Background()))
	assert.Equal(t, 3, fake.puts)
	assert.Empty(t, fake.keys())
}

func TestExecCompress(t *testing.T) {
	fake, p := startFakeS3(t)
	dir := t.TempDir()
	content := strings.Repeat("compress me ", 1000)
	writeTree(t, dir, map[string]string{"report.txt": content})

	p.Source = filepath.Join(dir, "report.txt")
	p.StripPrefix = dir + string(filepath.Separator)
	p.Compress = true

	require.NoError(t, p.Exec(context.Background()))

	obj := fake.object(t, "bucket/report.txt")
	assert.Contains(t, obj.header.Get("Content-Encoding"), "gzip")
	assert.Less(t, len(obj.body), len(content), "the uploaded body must be compressed")

	zr, err := gzip.NewReader(bytes.NewReader(obj.body))
	require.NoError(t, err)
	got, err := io.ReadAll(zr)
	require.NoError(t, err)
	assert.Equal(t, content, string(got))
}
