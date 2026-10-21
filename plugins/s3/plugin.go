package main

import (
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"mime"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
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

	// if not "" and encryption aws:kms
	// pass key to aws
	EncryptionKey string

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
func (p *Plugin) Exec(ctx context.Context) error {
	// normalize the target URL
	p.Target = strings.TrimPrefix(p.Target, "/")

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(p.Region), config.WithBaseEndpoint(p.Endpoint))
	if err != nil {
		slog.Error("Failed to create AWS session", slog.String("err", err.Error()))
		return err
	}

	switch {
	case p.Key != "" && p.Secret != "":
		cfg.Credentials = credentials.NewStaticCredentialsProvider(p.Key, p.Secret, "")
	case p.AssumeRole != "":
		client := sts.NewFromConfig(cfg)
		cfg.Credentials = stscreds.NewAssumeRoleProvider(client, p.AssumeRole, func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = p.AssumeRoleSessionName
		})
	default:
		slog.Warn("AWS Key and/or Secret not provided (falling back to ec2 instance profile)")
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = p.PathStyle
		o.EndpointOptions.DisableHTTPS = strings.HasPrefix(p.Endpoint, "http://")
	})

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

		target := path.Join(p.Target, strings.TrimPrefix(file, p.StripPrefix))

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
			Bucket: aws.String(p.Bucket),
			Key:    aws.String(target),
			ACL:    types.ObjectCannedACL(p.Access),
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
			putObjectInput.ServerSideEncryption = types.ServerSideEncryption(p.Encryption)
			if p.Encryption == "aws:kms" && p.EncryptionKey != "" {
				putObjectInput.SSEKMSKeyId = aws.String(p.EncryptionKey)
			}
		}

		if p.StorageClass != "" {
			putObjectInput.StorageClass = types.StorageClass(p.StorageClass)
		}

		// Check if the object exists
		headObjectInput := &s3.HeadObjectInput{
			Bucket: &p.Bucket,
			Key:    &target,
		}

		_, err = client.HeadObject(ctx, headObjectInput)
		if err != nil || p.Overwrite {
			// If the error indicates the object does not exist, upload the file
			if err := p.doUpload(ctx, client, putObjectInput, f, file); err != nil {
				return err
			}
		} else if !p.Overwrite {
			// Object exists, write log message
			slog.Info("File already exists, skipping upload due to overwrite=false",
				slog.String("file", file))
		}

		f.Close()
	}

	return nil
}

func (p *Plugin) doUpload(ctx context.Context, client *s3.Client, putObjectInput *s3.PutObjectInput, f *os.File, file string) error {
	// optionally compress
	if p.Compress {
		gz, err := gzipToTemp(f)
		if err != nil {
			slog.Error("Problem gzipping file", slog.String("file", file), slog.String("err", err.Error()))
			return err
		}
		defer func() {
			gz.Close()
			os.Remove(gz.Name())
		}()
		putObjectInput.Body = gz
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
		// a failed attempt has consumed the body, rewind it so that a retry
		// does not upload an empty object
		if _, err = putObjectInput.Body.(io.Seeker).Seek(0, io.SeekStart); err != nil {
			break
		}
		_, err = client.PutObject(ctx, putObjectInput)
		retryIndex++
		if err == nil || retryIndex > p.RetryCount {
			break
		}

		slog.Error("Could not upload file, retrying...",
			slog.String("file", file),
			slog.String("target", *putObjectInput.Key),
			slog.String("err", err.Error()),
			slog.Int("retry_index", retryIndex))
		time.Sleep(p.RetryDelay)
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

// gzipToTemp compresses src into a temporary file and returns it rewound, so
// that the SDK can seek it to sign the request and to retry. The caller closes
// and removes the file.
func gzipToTemp(src io.Reader) (*os.File, error) {
	tmp, err := os.CreateTemp("", "plugin-s3-*.gz")
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*os.File, error) {
		tmp.Close()
		os.Remove(tmp.Name())
		return nil, err
	}

	zw := gzip.NewWriter(tmp)
	if _, err := io.Copy(zw, src); err != nil {
		return fail(err)
	}
	if err := zw.Close(); err != nil {
		return fail(err)
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	return tmp, nil
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
			slog.Error(err.Error())
			os.Exit(1)
		}

		if matched {
			return stringMap[pattern]
		}
	}

	return ""
}
