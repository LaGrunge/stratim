# Jsonnet workflows

Workflows can be written in [jsonnet](https://jsonnet.org) instead of YAML. The server evaluates them itself on every pipeline, so there is no generated YAML to commit or keep in sync.

## Files

In `.woodpecker/`, next to or instead of YAML files:

- `name.jsonnet` is a config. It evaluates to either
  - one workflow (an object with `steps`), named after the file: `name`; or
  - an object of workflows by name, for example `{ build: {...}, test: {...} }`. Names may use letters, digits, `.`, `_` and `-`.
- `name.libsonnet` is a library: configs import it, it is never run on its own.

A single `.woodpecker.jsonnet` at the repository root works like `.woodpecker.yaml`; it can import `stratim.libsonnet` only, since nothing else is fetched with it.

Every `.jsonnet` file in the config folder is a config: keep generator sources of committed YAML outside of it.

A config can import `stratim.libsonnet` (below) and the `.jsonnet` and `.libsonnet` files next to it, by a path relative to it. It cannot read anything else. Two configs, or a config and a YAML file, may not produce workflows of the same name.

Variables such as `${CI_COMMIT_BRANCH}` are substituted after evaluation, as in YAML: write them into strings as they are.

The server evaluates each config in a separate process with no environment, which is stopped after 30 seconds and may use at most 1 GiB of memory, and a config may produce at most 16 MiB of JSON. The same config files are evaluated once and the result is reused; a config that ran out of time or memory is not evaluated again for five minutes. A config folder may hold at most 100 jsonnet configs, yielding at most 500 workflows.

## stratim.libsonnet

The server serves `stratim.libsonnet` in its own version to every config:

```jsonnet
local s = import 'stratim.libsonnet';

{
  ['test-' + v.GO]: s.workflow(
    [s.step('test', 'golang:' + v.GO, ['go test ./...'])],
    { when: [s.when.push('main'), s.when.pull_request() + s.when.path(['**/*.go'])] },
  )
  for v in s.matrix({ GO: ['1.26', '1.27'] })
}
```

| Function                                               | Returns                                                                                 |
| ------------------------------------------------------ | --------------------------------------------------------------------------------------- |
| `s.workflow(steps, extra={})`                          | a workflow; `extra` adds other workflow keys (`when`, `depends_on`, `services`, ...)    |
| `s.step(name, image, commands=[], extra={})`           | a step; `extra` adds other step keys (`environment`, `depends_on`, ...)                 |
| `s.matrix(axes)`                                       | every combination of the axes as objects: `s.matrix({A: [1, 2]})` is `[{A: 1}, {A: 2}]` |
| `s.when.event`, `branch`, `push`, `pull_request`       | conditions for a `when` list; add them up to require all of them                        |
| `s.when.tag`, `cron`, `manual(task)`, `path`, `status` | `manual('docs')` matches a manual run with `TASK=docs`                                  |

`stratim.libsonnet` only grows: a function keeps returning the same for the same arguments, so restarting an old pipeline on a newer server builds the same workflows. A change that would break this comes as a new library, `stratim.v1.libsonnet`.

## Server settings

The server reads `.jsonnet` and `.libsonnet` files of a config folder by default. A server that sets `WOODPECKER_DEFAULT_PIPELINE_CONFIG_EXTENSIONS` needs both in the list for jsonnet configs to work.

| Setting                       | Default | Meaning                                                   |
| ----------------------------- | ------- | --------------------------------------------------------- |
| `STRATIM_JSONNET_CONCURRENCY` | `2`     | configs evaluated at once; each may take the memory below |
| `STRATIM_JSONNET_MEMORY_MIB`  | `1024`  | memory one evaluation may use                             |
| `STRATIM_JSONNET_TIMEOUT`     | `30s`   | time one evaluation may take                              |

If the server cannot run its evaluation processes (it checks at start), jsonnet configs fail with an error rather than being evaluated without these limits.

## Checking locally

`woodpecker-cli lint` and `woodpecker-cli exec` evaluate jsonnet configs the same way: `woodpecker-cli lint .woodpecker/` lints every workflow the configs produce.
