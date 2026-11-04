local lib = import 'steps.libsonnet';

// a step per target, as projects generate their matrix in jsonnet
lib.workflow(
  [lib.step('deploy-' + target, ['echo deploying to ' + target]) for target in ['staging', 'production']],
  depends_on=['build'],
)
