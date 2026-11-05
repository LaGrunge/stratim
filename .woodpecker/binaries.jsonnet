// Release binaries: cross-compiled server, agent, cli, packages, checksums.
local ci = import 'ci.libsonnet';
local s = import 'stratim.libsonnet';

// cspell:words bindata netgo dryrun
local goStep(name, depends_on, commands) = ci.goStep(name, commands, if depends_on == null then {} else { depends_on: depends_on });

{
  when: [
    s.when.tag(),
    s.when.pull_request() + { branch: '${CI_REPO_DEFAULT_BRANCH}', path: ['Makefile'] + ci.config(std.thisFile) },
  ],

  steps: [
    ci.pnpmStep('build-web', 'web/', ['pnpm install --frozen-lockfile', 'pnpm build']),
    goStep('vendor', null, ['go mod vendor']),
    {
      name: 'cross-compile-server',
      depends_on: ['vendor', 'build-web'],
      image: ci.images.xgo,
      pull: true,
      commands: ['apt update', 'apt install -y tree', 'make cross-compile-server'],
      environment: {
        PLATFORMS: 'linux|arm64/v8;linux|amd64;linux|riscv64;windows|amd64;freebsd|amd64',
        TAGS: 'bindata sqlite sqlite_unlock_notify netgo',
        ARCHIVE_IT: '1',
      },
    },
    goStep('build-tarball', ['vendor', 'build-web'], ['make build-tarball']),
    goStep('build-agent', ['vendor'], ['apt update', 'apt install -y zip', 'make release-agent']),
    goStep('build-cli', ['vendor'], ['apt update', 'apt install -y zip', 'make release-cli']),
    goStep('build-deb-rpm', ['cross-compile-server', 'build-agent', 'build-cli'], ['make bundle']),
    goStep('checksums', ['cross-compile-server', 'build-agent', 'build-cli', 'build-deb-rpm', 'build-tarball'], ['make release-checksums']),
    goStep('release-dryrun', ['checksums'], ['ls -la dist/*.*', 'cat dist/checksums.txt']),
  ],
}
