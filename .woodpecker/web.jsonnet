// The web UI: install, lint, format, typecheck, test.
local ci = import 'ci.libsonnet';
local s = import 'stratim.libsonnet';

local onChange = { path: ci.config(std.thisFile) + ci.paths.web };
local step(name, commands, extra={}) = ci.pnpmStep(name, 'web/', commands, { when: onChange } + extra);
local afterInstall = { depends_on: ['install-dependencies'] };

{
  when: [s.when.pull_request(), s.when.push(['release/*'])],

  steps: [
    step('install-dependencies', ['pnpm install --frozen-lockfile']),
    step('lint', ['pnpm lint'], afterInstall),
    step('format-check', ['pnpm format:check'], afterInstall),
    step('typecheck', ['pnpm typecheck'], afterInstall),
    step('test', ['pnpm test'], {
      // wait for format-check, else test artifacts are falsely detected as wrong
      depends_on: ['install-dependencies', 'format-check'],
    }),
  ],
}
