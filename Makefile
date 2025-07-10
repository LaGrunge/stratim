GOFILES_NOVENDOR = $(shell find . -type f -name '*.go' -not -path "./vendor/*" -not -path "./.git/*")
GO_PACKAGES ?= $(shell go list ./... | grep -v /vendor/)

TARGETOS ?= linux
TARGETARCH ?= amd64

VERSION ?= next
VERSION_NUMBER ?= 0.0.0
ifneq ($(CI_COMMIT_TAG),)
	VERSION := $(CI_COMMIT_TAG:v%=%)
	VERSION_NUMBER := ${VERSION}
endif

# append commit-sha to next version
BUILD_VERSION := $(VERSION)
ifeq ($(BUILD_VERSION),next)
	CI_COMMIT_SHA ?= $(shell git rev-parse HEAD)
	BUILD_VERSION := $(shell echo "next-$(shell echo ${CI_COMMIT_SHA} | head -c 8)")
endif

LDFLAGS := -s -w -extldflags "-static" -X main.version=${BUILD_VERSION}

.PHONY: all
all: build

vendor:
	go mod tidy
	go mod vendor

format:
	@gofumpt -extra -w ${GOFILES_NOVENDOR}

.PHONY: clean
clean:
	go clean -i ./...
	rm -rf release/

.PHONY: lint
lint:
	@golangci-lint run

.PHONY: test
test:
	go test -race -cover ./...

build:
	CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags '${LDFLAGS}' -o release/plugin-s3

.PHONY: version
version:
	@echo ${VERSION}
