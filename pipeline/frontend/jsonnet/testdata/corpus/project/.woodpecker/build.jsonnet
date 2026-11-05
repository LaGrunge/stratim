// A build per platform and build type: the matrix most projects keep in
// jsonnet. Pull requests only build when sources change.
local s = import 'stratim.libsonnet';
local p = import 'project.libsonnet';

{
  ['build-%s-%s' % [std.asciiLower(v.platform), std.asciiLower(v.type)]]: s.workflow(
    [p.build(v.platform, v.type), p.test(v.platform)],
    {
      when: [
        s.when.push('main'),
        s.when.pull_request() + s.when.path(['src/**', 'CMakeLists.txt'], exclude=['**/*.md']),
      ],
    },
  )
  for v in s.matrix({ platform: p.platforms, type: p.builds })
}
