// Every condition stratim.libsonnet offers, in one workflow per condition.
local s = import 'stratim.libsonnet';

local probe(when) = s.workflow([s.step('probe', 'docker.io/library/alpine:3.24', ['true'])], { when: when });

{
  event: probe([s.when.event(['push', 'tag'])]),
  branch: probe([s.when.push() + s.when.branch(['main', 'release/*'])]),
  push: probe([s.when.push('main')]),
  'pull-request': probe([s.when.pull_request()]),
  tag: probe([s.when.tag('v*')]),
  cron: probe([s.when.cron('nightly')]),
  manual: probe([s.when.manual(), s.when.manual('docs')]),
  path: probe([s.when.push() + s.when.path(['web/**'])]),
  status: probe([s.when.push() + s.when.status(['success', 'failure'])]),
}
