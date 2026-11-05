// One workflow, named after the file.
local s = import 'stratim.libsonnet';

s.workflow([
  s.step('compile', 'docker.io/golang:1.27', ['go build ./...']),
  s.step('unit', 'docker.io/golang:1.27', ['go test ./...'], { depends_on: ['compile'] }),
], { when: [s.when.push('main'), s.when.pull_request()] })
