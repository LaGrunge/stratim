# Workflows

A pipeline has at least one workflow. A workflow is a set of steps that are executed in sequence using the same workspace which is a shared folder containing the repository and all the generated data from previous steps.

In case there is a single configuration in `.woodpecker.yaml` Woodpecker will create a pipeline with a single workflow.

By placing the configurations in a folder which is by default named `.woodpecker/` Woodpecker will create a pipeline with multiple workflows each named by the file they are defined in. Only `.yml` and `.yaml` files, and [jsonnet configs](./26-jsonnet.md), will be used and files in any subfolders like `.woodpecker/sub-folder/test.yaml` will be ignored.

You can also set some custom path like `.my-ci/pipelines/` instead of `.woodpecker/` in the [project settings](./75-project-settings.md).

## Benefits of using workflows

- faster lint/test feedback, the workflow doesn't have to run fully to have a lint status pushed to the remote
- better organization of a pipeline along various concerns using one workflow for: testing, linting, building and deploying
- utilizing more agents to speed up the execution of the whole pipeline

## Example workflow definition

:::warning
Please note that files are only shared between steps of the same workflow (see [File changes are incremental](./20-workflow-syntax.md#file-changes-are-incremental)). That means you cannot access artifacts e.g. from the `build` workflow in the `deploy` workflow.
If you still need to pass artifacts between the workflows you need use some storage [plugin](./51-plugins/51-overview.md) (e.g. one which stores files in an Amazon S3 bucket).
:::

```bash
.woodpecker/
├── build.yaml
├── deploy.yaml
├── lint.yaml
└── test.yaml
```

```yaml title=".woodpecker/build.yaml"
steps:
  - name: build
    image: debian:stable-slim
    commands:
      - echo building
      - sleep 5
```

```yaml title=".woodpecker/deploy.yaml"
steps:
  - name: deploy
    image: debian:stable-slim
    commands:
      - echo deploying

depends_on:
  - lint
  - build
  - test
```

```yaml title=".woodpecker/test.yaml"
steps:
  - name: test
    image: debian:stable-slim
    commands:
      - echo testing
      - sleep 5

depends_on:
  - build
```

```yaml title=".woodpecker/lint.yaml"
steps:
  - name: lint
    image: debian:stable-slim
    commands:
      - echo linting
      - sleep 5
```

## Status lines

Each workflow will report its own status back to your forge.

## Flow control

The workflows run in parallel on separate agents and share nothing.

Dependencies between workflows can be set with the `depends_on` element. A workflow doesn't execute until all of its dependencies finished successfully.

The name for a `depends_on` entry is the filename without the path, leading dots and without the file extension `.yml` or `.yaml`. If the project config for example uses `.woodpecker/` as path for CI files with a file named `.woodpecker/.lint.yaml` the corresponding `depends_on` entry would be `lint`.

```diff
 steps:
   - name: deploy
     image: debian:stable-slim
     commands:
       - echo deploying

+depends_on:
+  - lint
+  - build
+  - test
```

When triggering a pipeline manually, a single workflow can be selected. Only workflows whose `when` conditions match the `manual` event can be selected. Woodpecker then runs only this workflow and, recursively, the workflows it hard depends on.

Workflows that need to run even on failures should set the `status` filter.

```diff
 steps:
   - name: notify
     image: debian:stable-slim
     commands:
       - echo notifying

 depends_on:
   - deploy

+when:
+  - status: [ success, failure ]
```

This works just like the [`status` filter for steps](./20-workflow-syntax.md#status), matched against the workflows it depends on: `success` runs it when all of them succeeded, `failure` when none did, `timeout` also when one timed out, `killed` also when one was killed and `skipped` also when one was skipped. A workflow that runs after its dependency succeeded or was skipped:

```yaml
depends_on:
  - deploy

when:
  - status: [success, skipped]
```

### Optional dependencies

In a monorepo, workflows often use `when: path` to only run when relevant files change. A deploy workflow may need to wait for all check workflows, but some of them might not run because their path filter didn't match. With `depends_on`, this would block the deploy workflow entirely.

Mark a dependency as `optional: true` so it is only enforced when the referenced workflow is part of the pipeline. If the dependency is not built (e.g. its `when` conditions don't match), it is silently ignored.

```diff
 steps:
   - name: deploy
     image: debian:stable-slim
     commands:
       - echo deploying app a

 depends_on:
   - check-a
+  - name: check-b
+    optional: true
+  - name: check-c
+    optional: true
```

In this example, `deploy` always waits for `check-a`. It also waits for `check-b` and `check-c` if they are part of the pipeline, but runs without them if they were filtered out.

The same syntax works at the step level within a workflow: if a step uses `depends_on` with `optional: true` on another step that was filtered out by a `when` condition, the dependency is silently dropped.

:::info
Some workflows don't need the source code, like creating a notification on failure.
Read more about `skip_clone` at [pipeline syntax](./20-workflow-syntax.md#skip_clone)
:::

## Restarting failed workflows

When one workflow out of many fails, for example because of a flaky test or a network hiccup, you usually don't want to wait for all the others again. **Restart failed** in the menu next to **Restart** on the pipeline page (or `woodpecker-cli pipeline start --failed`) creates a new pipeline that only runs:

- the workflows that did not pass: the ones that failed, errored or timed out, and the ones that were canceled or skipped. Canceling a pipeline skips every workflow that had not started yet, so restarting a canceled pipeline picks up everything that did not finish. For a [matrix workflow](./30-matrix-workflows.md) only the variants that did not pass run again.
- every workflow that depends on one of them, directly or through other workflows, as the configuration of the restart declares it. `depends_on` is never skipped: a deploy that waits for the tests runs again together with them.
- workflows added to the configuration since, as they never passed.

The other workflows of the configuration are not run again; a workflow that is no longer in the configuration is left out. The new pipeline links to the one it was restarted from and lists the workflows it took over with the pipeline each of them ran in, where their logs are. This list is recorded when the restart is created: deleting an earlier pipeline does not change what a later restart takes over, it only removes the logs of the workflows that ran in it. In the forge their statuses stay as they are, so a commit turns green once the restarted workflows pass.

A workflow is the smallest unit that can be restarted: all its steps run again from the start, as the workspace of the failed run is gone.

A dependency on a workflow that is not run again counts with the status it finished with. That status also gates `when: status` filters, so a workflow that only runs on failure is skipped again once the restarted workflows pass.

If your workflows exchange files through some external storage and use `CI_PIPELINE_NUMBER` to find them, keep in mind that a workflow taken over from an earlier pipeline stored its files under that pipeline's number. `CI_PIPELINE_REUSED_WORKFLOWS` tells you where each of them ran. The variants of a matrix workflow are listed one by one as `name/N`, with `N` the position of the variant in the matrix, like in the status the forge shows, in the matrix of the current configuration. A variant that is not in the list runs in the current pipeline. The pairs are separated by `,` and each has a single `=`: a `%`, `,` or `=` in a workflow name is written as `%25`, `%2C` or `%3D`, so to look up a name, encode those characters in it first.

```bash
# CI_PIPELINE_REUSED_WORKFLOWS=build=12,lint=12,test/1=12,test/2=13
name=$(printf '%s' "build" | sed -e 's/%/%25/g' -e 's/,/%2C/g' -e 's/=/%3D/g')
build_pipeline=$(echo "$CI_PIPELINE_REUSED_WORKFLOWS" | tr ',' '\n' | awk -F= -v n="$name" '$1 == n { print $2 }')
aws s3 cp "s3://my-bucket/artifacts/${build_pipeline:-$CI_PIPELINE_NUMBER}/app.tar.gz" .
```

## Concurrency

By default workflows run with no concurrency limit. Some workflows, however, must not run more than a given number of times at once. A typical example is a deployment workflow: running two deployments at the same time can cause race conditions or corrupt state. Cancelling the previous pipeline is often not an option either, since it could interrupt an ongoing deployment.

The `concurrency` setting limits how many instances of a workflow may run at the same time. When the limit is reached, additional instances stay queued and start only once a running one has finished. Nothing is cancelled.

```yaml title=".woodpecker/deploy.yaml"
steps:
  - name: deploy
    image: debian:stable-slim
    commands:
      - echo deploying

depends_on:
  - test

concurrency:
  limit: 1
```

You can also use the shorthand form to only set the limit:

```yaml
concurrency: 1
```

### Ordering

Queued workflows of the same group start in the order their pipelines were created, **not** in the order they become ready to run. This matters when a workflow depends on other workflows (via `depends_on`) whose duration varies: even if a later pipeline's checks finish first, its limited workflow will not overtake an earlier pipeline that is still waiting. This guarantees that, for example, deployments happen in commit order.

### Groups

By default, the limit applies per workflow within a repository. Different runs of the same workflow are limited against each other, while different workflows (and other repositories) are unaffected.

Setting a `group` is optional. You can set a custom `group` to share a limit across workflows or to make the limit more specific. The group supports [environment variable substitution](./50-environment.md), so you can, for example, limit concurrency per branch or per deployment target:

```yaml
concurrency:
  limit: 1
  group: deploy-${CI_COMMIT_BRANCH}
```

## Priority

Workflows waiting in the queue are handed to agents in the order they were queued; a workflow waiting for its dependencies is queued once they are done. A workflow can ask to go before the other workflows of its repository with `priority`: an integer from `-10` to `10`, `0` if left out.

```yaml title=".woodpecker/hotfix.yaml"
priority: 5

steps:
  - name: build
    image: golang
    commands:
      - go build
```

A priority orders the workflows of one repository, not those of other repositories:

- The queue keeps, for each repository, the places its waiting workflows were queued at. They fill these places by their priority, the highest first, ties in the order they were queued. A workflow of another repository keeps its place: one queued before a high priority workflow is still handed out before it.
- A repository has as many places ahead of another repository's workflow as it had workflows queued before it: once a workflow is handed out, its place is gone. Workflows of a higher priority queued later do not keep a low one's place for themselves.
- A negative priority lets the repository's other workflows go first, for example a nightly job that should not hold up its repository's pushes.

A priority orders the queue and nothing more:

- A workflow still waits for its [`depends_on`](#flow-control) workflows; once they are done it goes before the lower ones of its repository.
- It still runs only on an agent its [labels](./20-workflow-syntax.md#labels) match. A workflow of a higher priority that no idle agent may take does not hold up a lower one an agent may take.
- It does not stop a workflow that is running already.
- It does not order a [concurrency group](#concurrency): the free slot of a group goes to the earliest pipeline (after a pipeline moved to the top, see below), whatever the priority of its workflows.

A priority out of range fails the pipeline with a linter error.

:::note
Priorities are not fair within a repository: while workflows of a higher priority keep coming, one of a lower priority of the same repository waits. Other repositories are not held up by them.
:::

### Move to the top

A server admin can move a pipeline to the top of the queue, server-wide: the **Move to top** button of the pipeline, shown to server admins while some of its workflows wait in the queue. Its waiting workflows then go before all others waiting, of every repository and whatever their priority; the pipeline moved last goes first. Among themselves they keep their priority and dependencies, and in a concurrency group the moved pipeline gets the next free slot. Moving does not touch the workflows already running, and a workflow restarted later is queued anew. The queue keeps the move over a server restart.

The API does the same, for server admins: `POST /api/repos/{repo_id}/pipelines/{pipeline_number}/prioritize` answers `204` once the pipeline was moved, `400` if nothing of it waits in the queue and `403` for a user who is not a server admin.
