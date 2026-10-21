package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/urfave/cli/v3"
)

var version = "0.0.0+0"

func main() {
	app := cli.Command{
		Name:    "s3 plugin",
		Usage:   "s3 plugin",
		Version: version,
		Action:  run,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "endpoint",
				Usage:   "endpoint for the s3 connection",
				Sources: cli.EnvVars("PLUGIN_ENDPOINT", "S3_ENDPOINT"),
			},
			&cli.StringFlag{
				Name:    "access-key",
				Usage:   "aws access key",
				Sources: cli.EnvVars("PLUGIN_ACCESS_KEY", "AWS_ACCESS_KEY_ID"),
			},
			&cli.StringFlag{
				Name:    "secret-key",
				Usage:   "aws secret key",
				Sources: cli.EnvVars("PLUGIN_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"),
			},
			&cli.StringFlag{
				Name:    "assume-role",
				Usage:   "aws iam role to assume",
				Sources: cli.EnvVars("PLUGIN_ASSUME_ROLE", "ASSUME_ROLE"),
			},
			&cli.StringFlag{
				Name:    "assume-role-session-name",
				Usage:   "aws iam role session name to assume",
				Value:   "woodpecker-s3",
				Sources: cli.EnvVars("PLUGIN_ASSUME_ROLE_SESSION_NAME", "ASSUME_ROLE_SESSION_NAME"),
			},
			&cli.StringFlag{
				Name:    "bucket",
				Usage:   "aws bucket",
				Value:   "us-east-1",
				Sources: cli.EnvVars("PLUGIN_BUCKET", "S3_BUCKET"),
			},
			&cli.StringFlag{
				Name:    "region",
				Usage:   "aws region",
				Value:   "us-east-1",
				Sources: cli.EnvVars("PLUGIN_REGION", "S3_REGION"),
			},
			&cli.StringFlag{
				Name:    "acl",
				Usage:   "upload files with acl",
				Value:   "private",
				Sources: cli.EnvVars("PLUGIN_ACL"),
			},
			&cli.StringFlag{
				Name:    "source",
				Usage:   "upload files from source folder",
				Sources: cli.EnvVars("PLUGIN_SOURCE"),
			},
			&cli.StringFlag{
				Name:    "target",
				Usage:   "upload files to target folder",
				Sources: cli.EnvVars("PLUGIN_TARGET"),
			},
			&cli.StringFlag{
				Name:    "strip-prefix",
				Usage:   "strip the prefix from the target",
				Sources: cli.EnvVars("PLUGIN_STRIP_PREFIX"),
			},
			&cli.StringSliceFlag{
				Name:    "exclude",
				Usage:   "ignore files matching exclude pattern",
				Sources: cli.EnvVars("PLUGIN_EXCLUDE"),
			},
			&cli.StringFlag{
				Name:    "encryption",
				Usage:   "server-side encryption algorithm, defaults to none",
				Sources: cli.EnvVars("PLUGIN_ENCRYPTION"),
			},
			&cli.StringFlag{
				Name:    "encryption-key",
				Usage:   "server-side encryption key, defaults to none",
				Sources: cli.EnvVars("PLUGIN_ENCRYPTION_KEY"),
			},
			&cli.BoolFlag{
				Name:    "dry-run",
				Usage:   "dry run for debug purposes",
				Sources: cli.EnvVars("PLUGIN_DRY_RUN"),
			},
			&cli.BoolFlag{
				Name:    "path-style",
				Usage:   "use path style for bucket paths",
				Sources: cli.EnvVars("PLUGIN_PATH_STYLE"),
			},
			&cli.StringFlag{
				Name:    "content-type",
				Usage:   "set content type header for uploaded objects",
				Sources: cli.EnvVars("PLUGIN_CONTENT_TYPE"),
			},
			&cli.StringFlag{
				Name:    "content-encoding",
				Usage:   "set content encoding header for uploaded objects",
				Sources: cli.EnvVars("PLUGIN_CONTENT_ENCODING"),
			},
			&cli.StringFlag{
				Name:    "cache-control",
				Usage:   "set cache-control header for uploaded objects",
				Sources: cli.EnvVars("PLUGIN_CACHE_CONTROL"),
			},
			&cli.StringFlag{
				Name:    "storage-class",
				Usage:   "set storage class to choose the best backend",
				Sources: cli.EnvVars("PLUGIN_STORAGE_CLASS"),
			},
			&cli.StringFlag{
				Name:    "env-file",
				Usage:   "source env file",
				Sources: cli.EnvVars("PLUGIN_ENV_FILE"),
			},
			&cli.BoolFlag{
				Name:    "compress",
				Usage:   "prior to upload, compress files and use gzip content-encoding",
				Sources: cli.EnvVars("PLUGIN_COMPRESS"),
			},
			&cli.BoolFlag{
				Name:    "overwrite",
				Usage:   "overwrite existing files",
				Sources: cli.EnvVars("PLUGIN_OVERWRITE"),
			},
			&cli.IntFlag{
				Name:    "retry-count",
				Usage:   "number of times to retry an upload of a file if it fails",
				Sources: cli.EnvVars("PLUGIN_RETRY_COUNT"),
				Value:   0,
			},
			&cli.DurationFlag{
				Name:    "retry-delay",
				Usage:   "tamount of time to wait before each retry",
				Sources: cli.EnvVars("PLUGIN_RETRY_DELAY"),
				Value:   time.Duration(5) * time.Second, //nolint:mnd
			},
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, c *cli.Command) error {
	if c.String("env-file") != "" {
		_ = godotenv.Load(c.String("env-file"))
	}

	plugin := Plugin{
		Endpoint:              c.String("endpoint"),
		Key:                   c.String("access-key"),
		Secret:                c.String("secret-key"),
		AssumeRole:            c.String("assume-role"),
		AssumeRoleSessionName: c.String("assume-role-session-name"),
		Bucket:                c.String("bucket"),
		Region:                c.String("region"),
		Access:                c.String("acl"),
		Source:                c.String("source"),
		Target:                c.String("target"),
		StripPrefix:           c.String("strip-prefix"),
		Exclude:               c.StringSlice("exclude"),
		Encryption:            c.String("encryption"),
		EncryptionKey:         c.String("encryption-key"),
		StorageClass:          c.String("storage-class"),
		PathStyle:             c.Bool("path-style"),
		DryRun:                c.Bool("dry-run"),
		Compress:              c.Bool("compress"),
		Overwrite:             c.Bool("overwrite"),
		RetryCount:            c.Int("retry-count"),
		RetryDelay:            c.Duration("retry-delay"),
		ContentType:           map[string]string{},
		ContentEncoding:       map[string]string{},
		CacheControl:          map[string]string{},
	}

	if err := json.Unmarshal([]byte(c.String("content-type")), &plugin.ContentType); err != nil {
		plugin.ContentType[".*"] = c.String("content-type")
	}

	if err := json.Unmarshal([]byte(c.String("content-encoding")), &plugin.ContentEncoding); err != nil {
		plugin.ContentEncoding[".*"] = c.String("content-encoding")
	}

	if err := json.Unmarshal([]byte(c.String("cache-control")), &plugin.CacheControl); err != nil {
		plugin.CacheControl[".*"] = c.String("cache-control")
	}

	return plugin.Exec(ctx)
}
