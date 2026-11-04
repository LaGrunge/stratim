local lib = import 'steps.libsonnet';

lib.workflow([
  lib.step('compile', ['echo compiling']),
  lib.step('unit-test', ['echo unit testing']),
])
