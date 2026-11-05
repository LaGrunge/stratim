// The docs site, with the generated CLI docs.
local ci = import 'ci.libsonnet';
local s = import 'stratim.libsonnet';

local docsPaths = ci.config(std.thisFile) + ci.paths.docs;
local onChange = [{ path: docsPaths, event: ['tag', 'pull_request', 'push'] }, s.when.manual()];
local pnpmStep(name, commands, when) = ci.pnpmStep(name, 'docs/', commands, { when: when });

{
  when: [
    s.when.tag(),
    s.when.pull_request(),
    s.when.push(['${CI_REPO_DEFAULT_BRANCH}']) + { path: docsPaths + ci.paths.images },
    s.when.manual('docs'),
  ],

  steps: [
    pnpmStep('install-dependencies', ['pnpm install --frozen-lockfile'], onChange),
    pnpmStep('format-check', ['pnpm format:check'], [s.when.pull_request() + { path: docsPaths }]),
    ci.goStep('build-cli', ['make generate-docs'], { when: onChange }),
    pnpmStep('build', ['pnpm build'], onChange),
  ],
}
