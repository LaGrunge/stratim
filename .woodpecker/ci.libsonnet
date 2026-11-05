// What Stratim's own workflows share. The first part is helpers any project
// could keep; the second is this project's data.
local s = import 'stratim.libsonnet';

local images = {
  golang: 'docker.io/golang:1.27',
  node: 'docker.io/node:26-alpine',
  alpine: 'docker.io/alpine:3.24',
  xgo: 'docker.io/techknowlogick/xgo:go-1.27.x',
  trivy: 'docker.io/woodpeckerci/plugin-trivy:1.6.1',
  golangciLint: 'golangci/golangci-lint:v2.15.0',
};

{
  // ---- helpers ----

  // config(file) is what a workflow's path filter needs to re-run it when
  // its config changes: the config itself (pass std.thisFile) and this
  // library. So any change here re-runs every workflow that filters on
  // paths; the price of sharing it.
  config(file):: [file, '.woodpecker/ci.libsonnet'],

  // ordered(byName, order) lists steps (or services) given by name, in
  // order: jsonnet writes an object's fields sorted, and the order of steps
  // is part of a workflow.
  ordered(byName, order):: [{ name: name } + byName[name] for name in order],

  // pnpm(commands) installs pnpm, then runs commands.
  pnpm(commands):: [
    'npx get-pnpm',
    "export PNPM_HOME='/root/.local/share/pnpm'",
    'export PATH="$PNPM_HOME/bin:$PATH"',
  ] + commands,

  // goStep and pnpmStep are steps in this project's Go and node images;
  // extra adds any other step keys.
  goStep(name, commands, extra={}):: s.step(name, images.golang, commands, extra),
  pnpmStep(name, dir, commands, extra={}):: s.step(name, images.node, self.pnpm(commands), { directory: dir } + extra),

  // ---- this project ----

  images:: images,

  paths:: {
    // Go code and what builds or checks it
    go: ['.golangci.yaml', '**/*.go', 'go.*', 'pipeline/schema/**', 'Makefile', 'codecov.yaml'],
    web: ['web/**', 'server/api/**'],
    docs: ['docs/**', 'cli/**', 'cmd/cli/**', 'server/api/**'],
    images: ['web/**', 'server/api/**', '**/*.go', 'go.*', 'pipeline/schema/**', 'docker/**'],
  },

  // synthetic: the triggers of the synthetic workflows (see synthetic.jsonnet)
  synthetic:: [s.when.cron('synthetic'), s.when.manual('synthetic')],
}
