// Repository-wide checks on pull requests: editorconfig, spelling, prettier,
// and the docs and migration checks some labels ask for.
local ci = import 'ci.libsonnet';
local s = import 'stratim.libsonnet';

local labelled(label) = [s.when.pull_request() + { evaluate: 'CI_COMMIT_PULL_REQUEST_LABELS contains "%s"' % label }];

{
  when: [s.when.pull_request()],

  steps: [
    {
      name: 'lint-editorconfig',
      image: 'docker.io/woodpeckerci/plugin-editorconfig-checker:0.3.4',
      depends_on: [],
      when: [s.when.pull_request()],
    },
    {
      name: 'spellcheck',
      image: ci.images.node,
      depends_on: [],
      commands: ci.pnpm([
        "pnpx cspell lint --no-progress --gitignore '{**,.*}/{*,.*}'",
        'apk add --no-cache -U tree',  // busybox tree don't understand "-I"
        // cspell:disable-next-line
        "tree --gitignore -I 012_columns_rename_procs_to_steps.go -I versioned_docs -I '*opensource.svg'| pnpx cspell lint --no-progress stdin",
      ]),
    },
    {
      name: 'prettier',
      image: 'docker.io/woodpeckerci/plugin-prettier:next',
      pull: true,
      depends_on: [],
      settings: { version: '3.6.2' },
    },
    s.step('check-feature-docs', ci.images.alpine, ['apk add --no-cache -q jq && ./.woodpecker/check-feature-docs.sh'], {
      depends_on: [],
      when: labelled('feature'),
    }),
    s.step('check-breaking-migrations', ci.images.alpine, ['apk add --no-cache -q jq && ./.woodpecker/check-breaking-migrations.sh'], {
      depends_on: [],
      when: labelled('breaking'),
    }),
  ],
}
