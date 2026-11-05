// Trivy scans of the Go code, the docs and the web UI.
local ci = import 'ci.libsonnet';
local s = import 'stratim.libsonnet';

local scan(settings) = { depends_on: [], image: ci.images.trivy, settings: { server: 'server' } + settings };

{
  when: [s.when.event(['pull_request']), s.when.push(['${CI_REPO_DEFAULT_BRANCH}'])],

  steps: [
    scan({ 'skip-dirs': 'web/,docs/' }) { name: 'backend' },
    scan({ 'skip-dirs': 'node_modules/,plugins/woodpecker-plugins/node_modules/', dir: 'docs/' }) { name: 'docs' },
    scan({ 'skip-dirs': 'node_modules/', dir: 'web/' }) { name: 'web' },
  ],

  services: [{
    name: 'server',
    image: ci.images.trivy,
    settings: { 'db-repository': 'mirror.gcr.io/aquasec/trivy-db:2' },
    ports: [10000],
  }],
}
