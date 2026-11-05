// Workflows by name from one config, with the library the server serves:
// a build per platform, then a report that needs them all.
local s = import 'stratim.libsonnet';

local platforms = ['amd64', 'arm64'];
local step(name, commands, extra={}) = s.step(name, 'dummy', commands, extra);

{
  ['build-' + v.platform]: s.workflow([step('build-' + v.platform, ['echo building for ' + v.platform])], { skip_clone: true })
  for v in s.matrix({ platform: platforms })
} + {
  report: s.workflow([step('report', ['echo all built'])], {
    skip_clone: true,
    depends_on: ['build-' + p for p in platforms],
  }),
}
