# plugin-s3

[![Build Status](https://ci.codeberg.org/api/badges/15974/status.svg)](https://ci.codeberg.org/woodpecker-plugins/s3)
[![GoDoc](https://godoc.org/codeberg.org/woodpecker-plugins/s3?status.svg)](https://godoc.org/codeberg.org/woodpecker-plugins/s3)
[![Docker pulls](https://img.shields.io/docker/pulls/woodpeckerci/plugin-s3)](https://hub.docker.com/r/woodpeckerci/plugin-s3)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Woodpecker CI plugin for publishing artifacts to Amazon S3 compatible object storage

## Build

Build the binary with the following commands:

```sh
go build
go test
```

## Docker

Build the Docker image with the following commands:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -tags netgo
docker build --rm=true -t woodpeckerci/plugin-s3 .
```

Please note incorrectly building the image for the correct x64 linux and with
CGO disabled will result in an error when running the Docker image:

```sh
docker: Error response from daemon: Container command
'/bin/plugin-s3' not found or does not exist..
```

## Usage

Execute from the working directory:

```sh
docker run --rm \
  -e PLUGIN_SOURCE=<source> \
  -e PLUGIN_TARGET=<target> \
  -e PLUGIN_BUCKET=<bucket> \
  -e AWS_ACCESS_KEY_ID=<token> \
  -e AWS_SECRET_ACCESS_KEY=<secret> \
  -v $(pwd):$(pwd) \
  -w $(pwd) \
  woodpeckerci/plugin-s3 --dry-run
```
