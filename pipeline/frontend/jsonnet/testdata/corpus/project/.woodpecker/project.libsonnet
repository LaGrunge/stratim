// The project's own library, next to its configs: images, the shared build
// step, and the platforms it ships for.
local s = import 'stratim.libsonnet';

{
  platforms:: ['rocky9', 'ubuntu26.04'],
  builds:: ['Debug', 'Release', 'Coverage'],

  image(platform):: {
    rocky9: 'docker.io/library/rockylinux:9',
    'ubuntu26.04': 'docker.io/library/ubuntu:26.04',
  }[platform],

  build(platform, type):: s.step('build', self.image(platform), [
    'cmake -B build -DCMAKE_BUILD_TYPE=' + type,
    'cmake --build build -j"$(nproc)"',
  ], { environment: { BUILD_TYPE: type } }),

  test(platform):: s.step('test', self.image(platform), ['ctest --test-dir build --output-on-failure'], {
    depends_on: ['build'],
  }),
}
