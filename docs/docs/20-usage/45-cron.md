# Cron

To configure cron jobs you need at least push access to the repository. A workflow can also declare its own crons in its config: see [Crons in the config](#crons-in-the-config).

## Add a new cron job

1. To create a new cron job adjust your pipeline config(s) and add the event filter to all steps you would like to run by the cron job:

   ```diff
    steps:
      - name: sync_locales
        image: weblate_sync
        settings:
          url: example.com
          token:
            from_secret: weblate_token
   +    when:
   +      event: cron
   +      cron: "name of the cron job" # if you only want to execute this step by a specific cron job
   ```

2. Create a new cron job in the repository settings:

   ![cron settings](./cron-settings.png)

   The supported schedule syntax can be found at <https://pkg.go.dev/github.com/gdgvda/cron#hdr-CRON_Expression_Format>. If you need general understanding of the cron syntax <https://it-tools.tech/crontab-generator> is a good place to start and experiment.

   Examples: `@every 5m`, `@daily`, `30 * * * *` ...

## Crons in the config

A workflow can declare the crons that run it, under `crons`, by name. Such a cron runs this workflow, and the workflows it depends on, on the default branch, whatever their `when` says about the event, ref, branch, cron, changed paths or `evaluate`. Their `when: status` and `when: matrix` still hold, and so does the `when` of their steps.

`crons` only adds a way to run the workflow: its `when` still decides which other events run it. A workflow that should run only on its crons keeps them out with `when: event: cron`. A workflow without `when` runs on every push, pull request and tag too.

```yaml
crons:
  nightly:
    schedule: 0 3 * * * # the syntax above
    timezone: Europe/Berlin # UTC if left out
    variables: # as a manual run's: parameters read them
      TIER: full

when:
  event: cron # only on crons, not on pushes

depends_on: [build]

steps:
  - name: test
    image: golang
    commands:
      - ./test.sh --tier ${TIER}
```

The server reads them from the config of a push to the default branch when its pipeline is created, or approved where pushes need approval. It creates the crons declared, updates those changed and deletes those gone. A push whose commit is no longer the head of the default branch changes nothing, as long as the forge can tell the head; and neither does a push whose config was overtaken by a newer push's. A push with no config at all, or skipped with `[skip ci]`, changes none: to delete a workflow's crons, remove them from its `crons`. Do so before deleting the config: crons left behind can then only be disabled in the settings.

Such a cron is named `<workflow>/<name>` (`test/nightly`), and both are letters, digits, `.`, `_` and `-`: that is the name `when: cron` sees, so a step that only runs on it says `when: {cron: test/nightly}`. It is marked "from the config" in the repository settings. There it can be run and enabled or disabled, nothing else: change or remove it in the config, so that renaming it in the settings cannot break the schedule. A cron disabled in the settings stays disabled when its config changes, unless its workflow is renamed: then it is a new cron.

Crons are read once the config parses: a config that does not parse changes none. Nor does one whose crons cannot all be read, cannot run (`0 0 30 2 *`) or declare one name twice: the pipeline shows a warning saying why. A push that runs no workflow leaves no pipeline to show it on, and the warning goes to the server's log. Cron jobs created in the settings are left alone. A name given to one in the settings cannot contain `/`; one named so before keeps its name, but to keep it apart from the config's, rename it.

To move a cron job from the settings into the config: declare it in its workflow, push to the default branch, check that it shows "from the config", then delete the old one in the settings. Until then both run. A cron of the settings runs every workflow whose `when` it matches, a cron of the config one workflow and those it depends on: if it ran several, declare it in a workflow that depends on all of them.
