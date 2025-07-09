package main

import (
	"compress/gzip"
	"errors"
	"io"
	"log/slog"
	"mime"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/mattn/go-zglob"
)

// Plugin defines the S3 plugin parameters.
type Plugin struct {
	Endpoint              string
	Key                   string
	Secret                string
	AssumeRole            string
	AssumeRoleSessionName string
	Bucket                string

	// if not "", enable server-side encryption
	// valid values are:
	//     AES256
	//     aws:kms
	Encryption string

	// us-east-1
	// us-west-1
	// us-west-2
	// eu-west-1
	// ap-southeast-1
	// ap-southeast-2
	// ap-northeast-1
	// sa-east-1
	Region string

	// Indicates the files ACL, which should be one
	// of the following:
	//     private
	//     public-read
	//     public-read-write
	//     authenticated-read
	//     bucket-owner-read
	//     bucket-owner-full-control
	Access string

	// Sets the content type on each uploaded object based on a extension map
	ContentType map[string]string

	// Sets the content encoding on each uploaded object based on a extension map
	ContentEncoding map[string]string

	// Sets the Cache-Control header on each uploaded object based on a extension map
	CacheControl map[string]string

	// Sets the storage class, affects the storage backend costs
	StorageClass string

	// Copies the files from the specified directory.
	// Regexp matching will apply to match multiple
	// files
	//
	// Examples:
	//    /path/to/file
	//    /path/to/*.txt
	//    /path/to/*/*.txt
	//    /path/to/**
	Source string
	Target string

	// Strip the prefix from the target path
	StripPrefix string

	// Exclude files matching this pattern.
	Exclude []string

	// Use path style instead of domain style.
	//
	// Should be true for minio and false for AWS.
	PathStyle bool
	// Dry run without uploading/
	DryRun bool
	// Compress objects and upload with Content-Encoding: gzip
	Compress bool
	// Overwrite existing files
	Overwrite bool
	// Number of time to retry upload
	RetryCount int
	// Time to sleep between retries in milliseconds
	RetryDelay time.Duration
}

// Exec runs the plugin
func (p *Plugin) Exec() error {
	// normalize the target URL
	p.Target = strings.TrimPrefix(p.Target, "/")

	// create the client
	conf := &aws.Config{
		Region:           aws.String(p.Region),
		Endpoint:         &p.Endpoint,
		DisableSSL:       aws.Bool(strings.HasPrefix(p.Endpoint, "http://")),
		S3ForcePathStyle: aws.Bool(p.PathStyle),
	}

	if p.Key != "" && p.Secret != "" {
		conf.Credentials = credentials.NewStaticCredentials(p.Key, p.Secret, "")
	} else if p.AssumeRole != "" {
		conf.Credentials = assumeRole(p.AssumeRole, p.AssumeRoleSessionName)
	} else {
		slog.Warn("AWS Key and/or Secret not provided (falling back to ec2 instance profile)")
	}

	sess, err := session.NewSession(conf)
	if err != nil {
		slog.Error("Failed to create AWS session", slog.String("err", err.Error()))
		return err
	}
	client := s3.New(sess)

	// find the bucket
	slog.Info("Attempting to upload",
		slog.String("region", p.Region),
		slog.String("endpoint", p.Endpoint),
		slog.String("bucket", p.Bucket))

	matches, err := matches(p.Source, p.Exclude)
	if err != nil {
		slog.Error("Could not match files", slog.String("err", err.Error()))
		return err
	}

	for _, file := range matches {
		stat, err := os.Stat(file)
		if err != nil {
			continue // should never happen
		}

		// skip directories
		if stat.IsDir() {
			continue
		}

		target := filepath.Join(p.Target, strings.TrimPrefix(file, p.StripPrefix))
		if !strings.HasPrefix(target, "/") {
			target = "/" + target
		}

		contentType := matchExtension(file, p.ContentType)
		contentEncoding := matchExtension(file, p.ContentEncoding)
		cacheControl := matchExtension(file, p.CacheControl)

		if contentType == "" {
			contentType = mime.TypeByExtension(filepath.Ext(file))

			if contentType == "" {
				contentType = "application/octet-stream"
			}
		}

		// when executing a dry-run we exit because we don't actually want to
		// upload the file to S3.
		if p.DryRun {
			continue
		}

		f, err := os.Open(file)
		if err != nil {
			slog.Error("Problem opening file", slog.String("file", file), slog.String("err", err.Error()))
			return err
		}
		defer f.Close()

		putObjectInput := &s3.PutObjectInput{
			Body:   f,
			Bucket: &(p.Bucket),
			Key:    &target,
			ACL:    &(p.Access),
		}
		if contentType != "" {
			putObjectInput.ContentType = aws.String(contentType)
		}

		if contentEncoding != "" {
			putObjectInput.ContentEncoding = aws.String(contentEncoding)
		}

		if cacheControl != "" {
			putObjectInput.CacheControl = aws.String(cacheControl)
		}

		if p.Encryption != "" {
			putObjectInput.ServerSideEncryption = aws.String(p.Encryption)
		}

		if p.StorageClass != "" {
			putObjectInput.StorageClass = &(p.StorageClass)
		}

		// Check if the object exists
		headObjectInput := &s3.HeadObjectInput{
			Bucket: &(p.Bucket),
			Key:    &target,
		}

		_, err = client.HeadObject(headObjectInput)
		if err != nil || p.Overwrite {
			// If the error indicates the object does not exist, upload the file
			p.doUpload(client, putObjectInput, f, file)
		} else if !p.Overwrite {
			// Object exists, write log message
			slog.Info("File already exists, skipping upload due to overwrite=false",
				slog.String("file", file))
		}

		f.Close()
	}

	return nil
}

func (p *Plugin) doUpload(client *s3.S3, putObjectInput *s3.PutObjectInput, f *os.File, file string) error {
	// optionally compress
	if p.Compress {
		gr, err := gzip.NewReader(f)
		if err != nil {
			slog.Error("Problem gzipping file", slog.String("file", file), slog.String("err", err.Error()))
			return err
		}
		// wrap with gzip
		putObjectInput.Body = &gzipReadSeeker{rs: f, z: gr}
		// set encoding
		putObjectInput.ContentEncoding = aws.String("gzip")
	} else {
		putObjectInput.Body = f
	}

	slog.Info("Uploading file",
		slog.String("file", file),
		slog.String("target", *putObjectInput.Key))

	// do upload
	retryIndex := 0
	var err error
	for {
		_, err = client.PutObject(putObjectInput)
		retryIndex++
		if err == nil || retryIndex > p.RetryCount {
			break
		} else {
			slog.Error("Could not upload file, retrying...",
				slog.String("file", file),
				slog.String("target", *putObjectInput.Key),
				slog.String("err", err.Error()),
				slog.Int("retry_index", retryIndex))
			time.Sleep(p.RetryDelay)
		}
	}

	if err != nil {
		slog.Error("Could not upload file",
			slog.String("file", file),
			slog.String("target", *putObjectInput.Key),
			slog.String("err", err.Error()))

		return err
	}
	return nil
}

// gzipReadSeeker implements Seek over gzip.Reader
type gzipReadSeeker struct {
	rs io.ReadSeeker
	z  *gzip.Reader
}

func (grs *gzipReadSeeker) Read(p []byte) (n int, err error) {
	return grs.z.Read(p)
}

func (grs *gzipReadSeeker) Seek(offset int64, whence int) (int64, error) {
	// only zero (reset) seeks are supported, in this case, this should be fine
	// since AWS will rarely seek mid-file
	if offset == 0 && whence == 0 {
		if err := grs.z.Reset(grs.rs); err != nil {
			return 0, err
		}
		return grs.rs.Seek(0, 0)
	}
	return 0, errors.New("Non-zero seek not supported")
}

// matches is a helper function that returns a list of all files matching the
// included Glob pattern, while excluding all files that matche the exclusion
// Glob pattners.
func matches(include string, exclude []string) ([]string, error) {
	matches, err := zglob.Glob(include)
	if err != nil {
		return nil, err
	}
	if len(exclude) == 0 {
		return matches, nil
	}

	// find all files that are excluded and load into a map. we can verify
	// each file in the list is not a member of the exclusion list.
	excludem := map[string]bool{}
	for _, pattern := range exclude {
		excludes, err := zglob.Glob(pattern)
		if err != nil {
			return nil, err
		}
		for _, match := range excludes {
			excludem[match] = true
		}
	}

	var included []string
	for _, include := range matches {
		_, ok := excludem[include]
		if ok {
			continue
		}
		included = append(included, include)
	}
	return included, nil
}

func matchExtension(match string, stringMap map[string]string) string {
	for pattern := range stringMap {
		matched, err := regexp.MatchString(pattern, match)

		if err != nil {
			panic(err)
		}

		if matched {
			return stringMap[pattern]
		}
	}

	return ""
}

func assumeRole(roleArn, roleSessionName string) *credentials.Credentials {
	sess, err := session.NewSession()
	if err != nil {
		slog.Error("Failed to create AWS session for assume role", slog.String("err", err.Error()))
	}
	client := sts.New(sess)
	duration := time.Hour * 1
	stsProvider := &stscreds.AssumeRoleProvider{
		Client:          client,
		Duration:        duration,
		RoleARN:         roleArn,
		RoleSessionName: roleSessionName,
	}

	return credentials.NewCredentials(stsProvider)
}
