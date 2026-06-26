---
name: S3
author: Woodpecker Authors
icon: https://codeberg.org/woodpecker-plugins/s3/raw/branch/main/s3.svg
description: Plugin to publish files and artifacts to Amazon S3 or Minio.
tags: [publish, s3, amazon, minio, storage]
containerImage: woodpeckerci/plugin-s3
containerImageUrl: https://hub.docker.com/r/woodpeckerci/plugin-s3
url: https://codeberg.org/woodpecker-plugins/s3
---

# S3 Plugin

The S3 plugin uploads files and build artifacts to your S3 bucket, or S3-compatible bucket such as Minio.
If you can not use docker.io there is an mirror at `codeberg.org/woodpecker-plugins/s3`.
The below pipeline configuration demonstrates simple usage:

```yml
steps:
  upload:
    image: docker.io/woodpeckerci/plugin-s3
    settings:
      bucket: my-bucket-name
      access_key: a50d28f4dd477bc184fbd10b376de753
      secret_key: dummy-secret-key
      source: public/**/*
      target: /target/location
```

Source the aws credentials from secrets:

```yml
steps:
  upload:
    image: docker.io/woodpeckerci/plugin-s3
    settings:
      bucket: my-bucket-name
      access_key:
        from_secret: aws_access_key_id
      secret_key:
        from_secret: aws_secret_access_key
      source: public/**/*
      target: /target/location
```

Use the pipeline number in the S3 target prefix:

```yml
steps:
  upload:
    image: docker.io/woodpeckerci/plugin-s3
    settings:
      bucket: my-bucket-name
      source: public/**/*
      target: /target/location/${CI_PIPELINE_NUMBER}
```

Override the default acl and region:

```yml
steps:
  - name: upload
    image: docker.io/woodpeckerci/plugin-s3
    settings:
      bucket: my-bucket-name
      acl: public-read
      region: us-east-1
      source: public/**/*
      target: /target/location
```

Configure the plugin to strip path prefixes when uploading:

```yml
steps:
  upload:
    image: docker.io/woodpeckerci/plugin-s3
    settings:
      bucket: my-bucket-name
      source: public/**/*
      target: /target/location
      strip_prefix: public/
```

Configure the plugin to exclude files from upload and compress:

```yml
steps:
  upload:
    image: docker.io/woodpeckerci/plugin-s3
    settings:
      bucket: my-bucket-name
      source: public/**/*
      target: /target/location
      exclude:
        - **/*.xml
      compress: true
```

Configure the plugin to connect to a Minio server:

```yml
steps:
  upload:
    image: docker.io/woodpeckerci/plugin-s3
    settings:
      bucket: my-bucket-name
      source: public/**/*
      target: /target/location
      path_style: true
      endpoint: https://play.minio.io:9000
```

Some settings allow you to map regexes so you can assign different values to matching files.
Here's how you could add specific cache control headers for different types of files:

```yml
steps:
  upload:
    image: docker.io/woodpeckerci/plugin-s3
    settings:
      # ...
      cache_control:
        "\\\\.html$": "public,max_age=600"
        "\\\\.(woff2|js|css|png|svg)$": "public,max_age=12000"
```

> Note that \\\\. means a literal '.' - you need to escape backslashes twice, once for the regex (\. becomes \\.), then againg for the yaml string (\\ becomes \\\\), resulting in 4.

## Settings Reference

| Setting                    | Description                                                                                                    |
| -------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `endpoint`                 | custom endpoint URL (optional, to use a S3 compatible non-Amazon service)                                      |
| `access_key`               | amazon key (optional)                                                                                          |
| `secret_key`               | amazon secret key (optional)                                                                                   |
| `assume_role`              | aws iam role to assume (optional)                                                                              |
| `assume_role_session_name` | aws iam role session name to assume (default `woodpecker-s3`)                                                  |
| `bucket`                   | bucket name                                                                                                    |
| `region`                   | bucket region (`us-east-1`, `eu-west-1`, etc)                                                                  |
| `acl`                      | access to files that are uploaded (`private`, `public-read`, etc)                                              |
| `source`                   | source location of the files, using a glob matching pattern. Location must be within the woodpecker workspace. |
| `target`                   | target location of files in the bucket                                                                         |
| `encryption`               | if provided, use server-side encryption                                                                        |
| `encryption-key`           | server-side encryption key for KMS                                                                             |
| `strip_prefix`             | strip the prefix from source path                                                                              |
| `exclude`                  | glob exclusion patterns                                                                                        |
| `path_style`               | whether path style URLs should be used (true for minio)                                                        |
| `env_file`                 | load env vars from file                                                                                        |
| `compress`                 | prior to upload, compress files and use gzip content-encoding (false by default)                               |
| `overwrite`                | overwrite existing files (true by default)                                                                     |
| `retry_count`              | number of times to retry an upload of a file if it fails (0 by default)                                        |
| `retry_delay`              | amount of time to wait before each retry (5s by default)                                                       |
| `content_type`             | map of content types with regexes as keys                                                                      |
| `content_encoding`         | map of content encoding with regexes as keys                                                                   |
| `cache_control`            | map of cache-control header with regexes as keys                                                               |
| `storage_class`            | set storage class to choose the best backend                                                                   |
| `dry_run`                  | dry run for debug purposes                                                                                     |
