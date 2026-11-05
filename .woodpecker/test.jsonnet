// Go tests: unit, e2e on the dummy backend and on MariaDB, datastores, fuzz.
local ci = import 'ci.libsonnet';
local s = import 'stratim.libsonnet';

local paths = ci.config(std.thisFile) + ci.paths.go;
local onChange = [{ path: paths }];  // any event the workflow runs on
local onPullRequest = [s.when.pull_request() + { path: paths }];
local waitForMariadb = "timeout 120 bash -c 'until (exec 3<>/dev/tcp/mariadb/3306) 2>/dev/null; do sleep 1; done'";

local goStep(commands, extra={}) = { image: ci.images.golang, commands: commands } + extra;
local afterVendor = { depends_on: ['vendor'] };

{
  when: [s.when.pull_request(), s.when.push('${CI_REPO_DEFAULT_BRANCH}') + { path: paths }],

  steps: ci.ordered({
    vendor: goStep(['go mod vendor'], { when: { path: paths + ['.woodpecker/**'] } }),

    'lint-pipeline': goStep([
      'go run go.woodpecker-ci.org/woodpecker/v3/cmd/cli lint',
      // builds .woodpecker/ as the server would, for every event
      'go test ./tools/ciconfig/',
    ], afterVendor {
      environment: {
        WOODPECKER_DISABLE_UPDATE_CHECK: true,
        WOODPECKER_LINT_STRICT: true,
        WOODPECKER_PLUGINS_PRIVILEGED: 'docker.io/woodpeckerci/plugin-docker-buildx',
      },
      when: [s.when.pull_request() + { path: ['.woodpecker/**'] }],
    }),

    'dummy-web': goStep(['mkdir -p web/dist/', 'echo "test" > web/dist/index.html'], { when: onChange }),

    lint: afterVendor { image: ci.images.golangciLint, commands: ['make lint'], when: onPullRequest },

    'check-openapi': goStep([
      'make generate-openapi',
      'DIFF=$(git diff | head)',
      '[ -n "$DIFF" ] && { echo "openapi not up to date, exec make generate-openapi and commit"; exit 1; } || true',
    ], afterVendor { when: onPullRequest }),

    'lint-license-header': goStep([
      'make install-addlicense',  // cspell:words addlicense
      // plugins/clone and plugins/s3 started as Drone plugins and carry their
      // own Apache-2.0 LICENSE; a "Woodpecker Authors" header would misstate
      // who holds the copyright.
      "bash -c 'shopt -s globstar; addlicense -check -ignore \"vendor/**\" -ignore cmd/server/openapi/docs.go -ignore \"plugins/clone/**\" -ignore \"plugins/s3/**\" **/*.go'",
    ], { when: onPullRequest }),

    test: goStep(['make test-agent', 'make test-server', 'make test-cli', 'make test-lib'], afterVendor { when: onChange }),

    'test-e2e': goStep(['make test-e2e'], afterVendor { when: onChange }),

    // The same scenarios on MariaDB. Every scenario recreates its database, so
    // it must stay distinct from the mariadb job's (e2e_test here, test
    // there): the two jobs share the service and run at the same time.
    'test-e2e-mariadb': goStep([
      waitForMariadb,
      'make test-e2e E2E_TIMEOUT=600s E2E_COVERAGE_FILE=e2e-mariadb-coverage.out',
    ], afterVendor {
      environment: { STRATIM_E2E_DATASOURCE: 'root@tcp(mariadb:3306)/e2e_test?parseTime=true' },
      when: onChange,  // same conditions as the mariadb service it needs
    }),

    'test-fuzz': goStep(['make fuzz'], afterVendor { environment: { FUZZ_TIME: '2s' }, when: onChange }),

    // Stratim runs on MariaDB only; sqlite and postgres stay informational
    // until their drivers are removed in phase 1.
    sqlite: goStep(['make test-server-datastore'], afterVendor {
      environment: { WOODPECKER_DATABASE_DRIVER: 'sqlite3' },
      when: onChange,
      failure: 'ignore',
    }),

    postgres: goStep(['make test-server-datastore'], afterVendor {
      environment: {
        WOODPECKER_DATABASE_DRIVER: 'postgres',
        WOODPECKER_DATABASE_DATASOURCE: 'host=postgres user=postgres dbname=postgres sslmode=disable',  // cspell:disable-line
      },
      when: onPullRequest,
      failure: 'ignore',
    }),

    // MariaDB is the database Stratim runs on, so unlike postgres it is
    // tested on pushes to main too; codecov depends on it.
    mariadb: goStep([
      // the image restarts its server once after initializing the data
      // directory, with networking off until then
      waitForMariadb,
      'make test-server-datastore-coverage',
      // the fixture generator runs only by hand; keep it compiling
      "go vet -tags 'test fixturegen' ./server/store/datastore/migration/",
    ], afterVendor {
      environment: {
        WOODPECKER_DATABASE_DRIVER: 'mysql',
        // database name distinct from test-e2e-mariadb's, see there
        WOODPECKER_DATABASE_DATASOURCE: 'root@tcp(mariadb:3306)/test?parseTime=true',
      },
      when: onChange,
    }),

    codecov: {
      depends_on: ['test', 'mariadb', 'test-e2e', 'test-e2e-mariadb'],
      pull: true,
      image: 'docker.io/woodpeckerci/plugin-codecov:2.3.3',
      settings: {
        files: [
          'agent-coverage.out',
          'cli-coverage.out',
          'coverage.out',
          'server-coverage.out',
          'datastore-coverage.out',
          'e2e-coverage.out',
          'e2e-mariadb-coverage.out',
        ],
        token: { from_secret: 'codecov_token' },
      },
      when: onChange,
      failure: 'ignore',
    },
  }, [
    'vendor', 'lint-pipeline', 'dummy-web', 'lint', 'check-openapi', 'lint-license-header', 'test', 'test-e2e',
    'test-e2e-mariadb', 'test-fuzz', 'sqlite', 'postgres', 'mariadb', 'codecov',
  ]),

  services: ci.ordered({
    postgres: {
      image: 'docker.io/postgres:18',
      ports: ['5432'],
      environment: { POSTGRES_USER: 'postgres', POSTGRES_HOST_AUTH_METHOD: 'trust' },
      when: onPullRequest,
    },
    mariadb: {
      image: 'docker.io/mariadb:13.0',
      ports: ['3306'],
      environment: { MARIADB_DATABASE: 'test', MARIADB_ALLOW_EMPTY_ROOT_PASSWORD: 'yes' },
      when: onChange,
    },
  }, ['postgres', 'mariadb']),
}
