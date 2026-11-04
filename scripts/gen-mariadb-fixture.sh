#!/usr/bin/env bash
#
# Regenerate server/store/datastore/migration/test-files/mariadb.sql, the
# existing MariaDB database that TestMigrate upgrades.
#
# The fixture is an old database on purpose: its schema is that of an older
# Woodpecker release (FIXTURE_BASE below), so TestMigrate runs every migration
# since then against MariaDB. Its rows are those of the Postgres fixture
# (test-files/postgres.sql), which predates that release.
#
# How: start Postgres and MariaDB, load postgres.sql, check out FIXTURE_BASE in
# a temporary worktree, and run TestGenerateMariaDBFixture there. That test
# migrates both databases to the release's schema and copies the rows into
# MariaDB, which is then dumped.
#
# Needs docker, git with the upstream release tags, and Go. Containers and the
# worktree are removed on exit. Bump FIXTURE_BASE only to a release that
# predates the newest migration, or the upgrade case tests nothing.

set -euo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_BASE="${FIXTURE_BASE:-v3.9.0}"
MIGRATION_DIR="server/store/datastore/migration"
OUT="$SRC/$MIGRATION_DIR/test-files/mariadb.sql"
MARIADB_IMAGE="${MARIADB_IMAGE:-docker.io/mariadb:13.0}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-docker.io/postgres:18}"
NAME="stratim-fixture-$$"
WORKTREE="$(mktemp -d)"

log() { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31merror\033[0m %s\n' "$*" >&2; exit 1; }

case "${1:-}" in
  -h | --help)
    echo "Usage: [FIXTURE_BASE=vX.Y.Z] ${0##*/}   (see the header of this script)" >&2
    exit 0
    ;;
  "") ;;
  *) die "unknown argument: $1" ;;
esac

git -C "$SRC" rev-parse --verify --quiet "refs/tags/$FIXTURE_BASE" >/dev/null ||
  die "tag $FIXTURE_BASE not found; fetch the upstream tags first"

cleanup() {
  docker rm -f "$NAME-pg" "$NAME-my" >/dev/null 2>&1 || true
  git -C "$SRC" worktree remove --force "$WORKTREE" >/dev/null 2>&1 || rm -rf "$WORKTREE"
  rm -f "$OUT.tmp"
}
trap cleanup EXIT

log "starting $POSTGRES_IMAGE and $MARIADB_IMAGE"
docker run -d --name "$NAME-pg" -p 127.0.0.1::5432 \
  -e POSTGRES_USER=postgres -e POSTGRES_HOST_AUTH_METHOD=trust "$POSTGRES_IMAGE" >/dev/null
docker run -d --name "$NAME-my" -p 127.0.0.1::3306 \
  -e MARIADB_DATABASE=fixture -e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=1 "$MARIADB_IMAGE" >/dev/null

port() { docker port "$1" "$2" | head -n1 | sed 's/.*://'; }
PG_PORT="$(port "$NAME-pg" 5432)"
MY_PORT="$(port "$NAME-my" 3306)"

# Both images first run a temporary server on the unix socket only, then
# restart it with networking. Probe over TCP so that only the real one counts.
wait_for() {
  local what="$1"
  shift
  for _ in $(seq 1 60); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  die "$what did not come up"
}
wait_for postgres docker exec "$NAME-pg" psql -h 127.0.0.1 -U postgres -c 'SELECT 1'
wait_for mariadb docker exec "$NAME-my" mariadb -h 127.0.0.1 --protocol=tcp -u root -e 'SELECT 1' fixture

log "loading the Postgres fixture"
docker exec -i "$NAME-pg" psql -q -v ON_ERROR_STOP=1 -U postgres \
  <"$SRC/$MIGRATION_DIR/test-files/postgres.sql" >/dev/null

log "migrating both to the schema of $FIXTURE_BASE and copying the rows"
git -C "$SRC" worktree add --quiet --detach "$WORKTREE" "$FIXTURE_BASE"
cp "$SRC/$MIGRATION_DIR/fixture_gen_test.go" "$WORKTREE/$MIGRATION_DIR/"
(
  cd "$WORKTREE"
  FIXTURE_POSTGRES_DATASOURCE="host=127.0.0.1 port=$PG_PORT user=postgres dbname=postgres sslmode=disable" \
    FIXTURE_MARIADB_DATASOURCE="root@tcp(127.0.0.1:$MY_PORT)/fixture?parseTime=true" \
    go test -count=1 -tags 'test fixturegen' -run '^TestGenerateMariaDBFixture$' -v "./$MIGRATION_DIR/"
)

log "dumping to ${OUT#"$SRC"/}"
{
  echo "-- MariaDB fixture for TestMigrate: the rows of postgres.sql at the schema of"
  echo "-- Woodpecker $FIXTURE_BASE. Regenerate with scripts/gen-mariadb-fixture.sh."
  docker exec "$NAME-my" mariadb-dump -u root --skip-dump-date --skip-extended-insert \
    --skip-comments --no-create-db fixture
} >"$OUT.tmp"
mv "$OUT.tmp" "$OUT"
log "done: $(grep -c '^INSERT INTO' "$OUT") rows"
