// Synthetic load (S0-3): workflows that load the CI the way real projects do,
// and a last one that checks how it coped against
// .woodpecker/synthetic/budgets.json. They run only on the "synthetic" cron
// or a manual run with TASK=synthetic, never on pushes or pull requests.
local ci = import 'ci.libsonnet';
local s = import 'stratim.libsonnet';

local load(steps) = s.workflow(steps, { when: ci.synthetic, skip_clone: true });

// two identical workflows that hold an agent each for a minute: they must
// overlap, or the pool did not run them in parallel
local parallel = load([s.step('hold', ci.images.alpine, ['sleep 60'])]);

local workflows = {
  // one step writes a large log, as a sanitizer or MTR step does: 3 million
  // numbered lines of 100 bytes, 300 MB, all of which its log must hold
  'synthetic-logs': load([s.step('big-log', ci.images.alpine, [
    'awk -v n="$LINES" \'BEGIN { for (i = 1; i <= n; i++) printf "%09d synthetic log line padded to one hundred bytes ..........................................\\n", i }\'',
  ], { environment: { LINES: '3000000' } })]),

  // 10 000 tests reported as TeamCity service messages, some failed or ignored
  'synthetic-tests': load([s.step('service-messages', ci.images.alpine, [|||
    awk -v n="$TESTS" 'BEGIN {
      print "##teamcity[testSuiteStarted name=\x27synthetic\x27]"
      for (i = 1; i <= n; i++) {
        t = sprintf("synthetic.case_%05d", i)
        print "##teamcity[testStarted name=\x27" t "\x27]"
        if (i % 1000 == 0) print "##teamcity[testFailed name=\x27" t "\x27 message=\x27synthetic failure\x27]"
        else if (i % 997 == 0) print "##teamcity[testIgnored name=\x27" t "\x27 message=\x27synthetic skip\x27]"
        print "##teamcity[testFinished name=\x27" t "\x27 duration=\x27" (i % 50) "\x27]"
      }
      print "##teamcity[testSuiteFinished name=\x27synthetic\x27]"
    }'
  |||], { environment: { TESTS: '10000' } })]),

  'synthetic-parallel-a': parallel,
  'synthetic-parallel-b': parallel,
};

workflows {
  // reads this very pipeline from the API once the others are done, whatever
  // their outcome, and fails when one of them broke a budget. Not yet here: a
  // step that hits its timeout; S0-6 makes timeouts workflow settings.
  'synthetic-check': s.workflow([
    s.step('check', ci.images.golang, ['go run ./tools/synthetic-check -budgets .woodpecker/synthetic/budgets.json'], {
      environment: {
        STRATIM_API_URL: { from_secret: 'stratim_api_url' },
        STRATIM_API_TOKEN: { from_secret: 'stratim_api_token' },
      },
    }),
  ], {
    // every outcome when.status knows: the check also reports workflows that
    // failed, and a missing or skipped one shows up as a broken budget
    when: [w + s.when.status(['success', 'failure']) for w in ci.synthetic],
    depends_on: std.objectFields(workflows),
  }),
}
