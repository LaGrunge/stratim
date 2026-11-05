// Nightly: long tests after every release build, on the cron or by hand.
local s = import 'stratim.libsonnet';
local p = import 'project.libsonnet';

s.workflow(
  [s.step('long-tests', p.image('rocky9'), ['ctest --test-dir build -L long'])],
  {
    when: [s.when.cron('nightly'), s.when.manual('nightly')],
    depends_on: ['build-%s-release' % platform for platform in p.platforms],
  },
)
