// cspell:words foldl
// stratim.libsonnet v0: helpers for writing .woodpecker/*.jsonnet. The
// Stratim server serves this file to every config as `import
// 'stratim.libsonnet'`, in the version of that server.
//
// It only grows: what a function returns for given arguments never changes,
// so that restarting an old pipeline on a newer server builds the same
// workflows. A change that would break this ships as stratim.v1.libsonnet.
//
//   local s = import 'stratim.libsonnet';
//   {
//     ['test-' + v.GO]: s.workflow(
//       [s.step('test', 'golang:' + v.GO, ['go test ./...'])],
//       { when: [s.when.push('main'), s.when.pull_request()] },
//     )
//     for v in s.matrix({ GO: ['1.26', '1.27'] })
//   }
{
  version:: 0,

  // vars are the variables the run was given (a manual run, a cron job, the
  // API, a restart or deployment), as strings by name.
  vars:: import 'stratim.vars',

  // var is the run's value of the variable name, or default when the run
  // gave none or an empty one, as a workflow's `params` read it. To have
  // the value in the steps' environment and in `when` too, with options,
  // declare it in the workflow's `params`.
  var(name, default=''):: local v = std.get(self.vars, name, ''); if v == '' then default else v,

  // workflow is a workflow of steps; extra adds any other workflow keys
  // (when, depends_on, services, ...).
  workflow(steps, extra={}):: { steps: steps } + extra,

  // step is a step running commands in image; extra adds any other step
  // keys (environment, depends_on, when, ...).
  step(name, image, commands=[], extra={}):: {
    name: name,
    image: image,
  } + (if commands == [] then {} else { commands: commands }) + extra,

  // matrix is every combination of the axes' values, as objects:
  // matrix({ A: [1, 2], B: ['x'] }) == [{ A: 1, B: 'x' }, { A: 2, B: 'x' }].
  matrix(axes):: std.foldl(
    function(combos, axis) [combo { [axis]: value } for combo in combos for value in axes[axis]],
    std.objectFields(axes),
    [{}],
  ),

  // when holds conditions for a `when` list; add them up to require all,
  // e.g. s.when.push('main') + s.when.path(['**/*.go']).
  when:: {
    event(events):: { event: events },
    branch(branches):: { branch: branches },
    push(branch=null):: { event: 'push' } + (if branch == null then {} else { branch: branch }),
    pull_request():: { event: 'pull_request' },
    tag(ref=null):: { event: 'tag' } + (if ref == null then {} else { ref: ref }),
    cron(name):: { event: 'cron', cron: name },
    manual(task=null):: { event: 'manual' } + (if task == null then {} else { evaluate: 'TASK == ' + std.escapeStringJson(task) }),
    path(include, exclude=[]):: { path: { include: include } + (if exclude == [] then {} else { exclude: exclude }) },
    status(statuses):: { status: statuses },
  },
}
