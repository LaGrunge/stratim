#!/usr/bin/env bash
#
# Build the Stratim server, agent, autoscaler and secrets extension images
# from this checkout and push them to Docker Hub:
#
#   lagrunge/stratim-server       linux/amd64
#   lagrunge/stratim-agent        linux/amd64, linux/arm64
#   lagrunge/stratim-autoscaler   linux/amd64
#   lagrunge/stratim-secrets-aws  linux/amd64
#
# Each is tagged with the short commit and with `next`. The binaries are
# built here (Go on the host, the UI in a node container) and only copied
# into the images, so a build needs no Go or node toolchain inside Docker.
#
# Needs `docker login` to Docker Hub as lagrunge beforehand.

set -euo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_PREFIX="${REPO_PREFIX:-lagrunge/stratim}"
PUSH=1
SKIP_UI=0

log() { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31merror\033[0m %s\n' "$*" >&2; exit 1; }

usage() {
  cat >&2 <<EOF
Usage: ${0##*/} [options]

  -n, --no-push     build the images locally (linux/amd64 only), push nothing
      --prefix NAME image name prefix (default: $REPO_PREFIX)
      --skip-ui     reuse the UI already built in web/dist
  -h, --help        this help
EOF
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--no-push) PUSH=0; shift ;;
    --prefix)  [[ $# -ge 2 ]] || die "$1 needs a value"; REPO_PREFIX="$2"; shift 2 ;;
    --skip-ui) SKIP_UI=1; shift ;;
    -h|--help) usage 0 ;;
    *) die "unknown option: $1 (try --help)" ;;
  esac
done

cd "$SRC"
[[ -z "$(git status --porcelain --untracked-files=no)" ]] || die "$SRC has uncommitted changes; images must match a commit"
SHA="$(git rev-parse --short HEAD)"
VERSION="$(git describe --tags --always 2>/dev/null || echo "$SHA")"
MODULE="go.woodpecker-ci.org/woodpecker/v3"

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

if (( SKIP_UI )); then
  [[ -f web/dist/index.html && "$(cat web/dist/index.html)" != "test" ]] || die "no built UI in web/dist"
else
  log "building the UI ($VERSION)"
  docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp -e COREPACK_HOME=/tmp/corepack \
    -v "$SRC/web:/web" -w /web docker.io/node:24 \
    sh -c 'corepack pnpm install --frozen-lockfile && corepack pnpm build'
fi

build() { # build <binary> <package> <ldflags> <arch>...
  local bin="$1" pkg="$2" ldflags="$3"; shift 3
  local arch dir="${bin#woodpecker-}"
  dir="${dir#stratim-}"
  for arch in "$@"; do
    log "building $bin for linux/$arch"
    CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -tags grpcnotrace \
      -ldflags "-s -w $ldflags" -o "$STAGE/$dir/linux_$arch/$bin" "$pkg"
  done
}

# CGO off: Stratim runs on MariaDB, so the server needs no sqlite.
build woodpecker-server "$MODULE/cmd/server" "-X $MODULE/version.Version=$VERSION" amd64
build woodpecker-agent "$MODULE/cmd/agent" "-X $MODULE/version.Version=$VERSION" amd64 arm64
build woodpecker-autoscaler "$MODULE/autoscaler/cmd/woodpecker-autoscaler" \
  "-X $MODULE/autoscaler/version.Version=$VERSION" amd64
build stratim-secrets-aws "$MODULE/cmd/secrets-aws" "" amd64

image() { # image <name> <platforms>
  local name="$1" platforms="$2" out
  if (( PUSH )); then out=--push; else out=--load; fi
  # --load takes a single platform only
  (( PUSH )) || platforms="linux/amd64"
  log "image $REPO_PREFIX-$name:$SHA ($platforms)"
  docker buildx build $out --platform "$platforms" \
    --label "org.opencontainers.image.source=https://github.com/LaGrunge/stratim" \
    --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" \
    --label "org.opencontainers.image.version=$VERSION" \
    -f "$SRC/docker/Dockerfile.$name.prebuilt" \
    -t "$REPO_PREFIX-$name:$SHA" -t "$REPO_PREFIX-$name:next" "$STAGE"
}

image server linux/amd64
image agent linux/amd64,linux/arm64
image autoscaler linux/amd64
image secrets-aws linux/amd64

log "done: $REPO_PREFIX-{server,agent,autoscaler,secrets-aws}:$SHA"
echo "$SHA"
