# Pipeline events API

`GET /api/v2/events` streams what happens to pipelines as [server-sent events](https://html.spec.whatwg.org/multipage/server-sent-events.html): one small, typed event per change of a pipeline, a workflow or a step, instead of the whole pipeline on every change. It is meant for scripts, bots and agents that watch the CI: a pipeline of six workflows gives about nine events at the default severity.

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  "$SERVER/api/v2/events?repo=owner/name&branch=main"
```

The web UI keeps using `/api/stream/events`.

## Access

The stream needs a login: a personal access token as above, or the session of the web UI. It carries the events of the repositories the user can read: all of them for an admin; otherwise the public and internal ones and those the user may pull by the permission the server has stored for them. The stream does not ask the forge; the rest of the API syncs the stored permissions with it once they are an hour old, and until then a stored pull counts. The user and these permissions are looked up again every minute while the stream runs, and the stream ends with an `error` event once the user is gone or their tokens are reset.

## Stream

A stream starts with a `retry` delay and a `hello` event, whose `id` is where the stream goes on from, then the replayed events (see [Reconnecting](#reconnecting)), then the events as they happen. An idle stream gets a `: ping` comment every 15 seconds. The ping has an `id` too: it moves the cursor past the events the server no longer keeps, none of which was for the stream, so that a quiet stream reconnects without a `reset`.

```text
retry: 3000

id: 1793934000000041
event: hello
data: {"v":1,"server":"https://ci.example.com"}

id: 1793934000000042
event: step.failed
data: {"v":1,"id":1793934000000042,"type":"step.failed","severity":"error","ts":"2026-11-06T03:12:08Z","repo_id":7,"repo":"owner/name","pipeline_id":812,"number":345,"branch":"main","workflow":"build","workflow_id":4410,"step":"test","step_id":9911,"status":"failure","exit_code":1,"duration_s":31,"summary":"#345 build/test failure after 31s, exit 1"}
```

`hello`, `reset` and `error` tell of the stream itself:

| Event   | Data          | Meaning                                                                                                                                                                                      |
| ------- | ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `hello` | `v`, `server` | the schema version and the server's URL to build links with; its `id` is the cursor to reconnect with until the first event comes                                                            |
| `reset` | `v`, `reason` | the events after the cursor given are not all kept: none is replayed, load the current state (see below)                                                                                     |
| `error` | `v`, `reason` | the stream ends: `too_slow` when the client did not read the events fast enough, reconnect with the last id to get the rest; `unauthorized` when the user is gone or their tokens were reset |

For an admin, `hello` and `reset` also carry `head` and `oldest`: the ids of the newest event and of the oldest kept.

## Events

Every event is one line of JSON of at most 400 bytes; a `step.explained` with its `cause` takes at most 1536:

| Field                     | Meaning                                                                                                   |
| ------------------------- | --------------------------------------------------------------------------------------------------------- |
| `v`                       | the schema version, `1`                                                                                   |
| `id`                      | the event's id, the same as the SSE `id`; the ids of a server grow by one across all its repositories     |
| `type`                    | what happened, see below                                                                                  |
| `severity`                | how much it matters, see below                                                                            |
| `ts`                      | when it happened, RFC 3339 in UTC                                                                         |
| `repo_id`, `repo`         | the repository: its id and full name                                                                      |
| `pipeline_id`, `number`   | the pipeline: its id and its number in the repository                                                     |
| `branch`                  | the pipeline's branch; of a pull request, its target branch                                               |
| `workflow`, `workflow_id` | the workflow, on workflow and step events; the workflows of a matrix share a name and differ in their ids |
| `step`, `step_id`         | the step, on step events                                                                                  |
| `status`                  | the status of what the event is about after it                                                            |
| `exit_code`               | on the end of a step that started                                                                         |
| `ignored`                 | `true` on a failed step whose failure is ignored (`failure: ignore`)                                      |
| `duration_s`              | on an end, how long it ran in seconds                                                                     |
| `summary`                 | one line for a human, at most 160 characters, never taken from a log                                      |
| `cause`                   | on `step.explained`, why the step failed, see [Why a step failed](#why-a-step-failed)                     |

Names longer than 48 bytes are cut and end with `…`; the ids are exact. New fields may be added within a schema version: ignore the fields and event types you do not know.

| Type                | When                                   | Severity                                                                                                  |
| ------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `pipeline.created`  | a pipeline is created                  | info                                                                                                      |
| `pipeline.blocked`  | a pipeline waits for approval          | warning                                                                                                   |
| `pipeline.started`  | its first workflow starts              | notice                                                                                                    |
| `pipeline.finished` | it ended                               | notice on success; error on failure, error or timeout; warning when killed, canceled or declined          |
| `workflow.started`  | a workflow starts                      | info                                                                                                      |
| `workflow.finished` | a workflow ended                       | notice on success; error on failure, error or timeout; warning when killed; info when skipped or canceled |
| `step.started`      | a step starts                          | debug                                                                                                     |
| `step.finished`     | a step ended without failing           | info on success; warning when killed; debug when canceled before it started                               |
| `step.failed`       | a step failed, timed out or errored    | error; notice when its failure is ignored                                                                 |
| `step.explained`    | the cause of a `step.failed` was found | that of its `step.failed`                                                                                 |

Skipped steps give no event. A step that started and ended between two updates of the server gives `step.finished` without `step.started`.

The severities, from the least: `debug` (progress nobody waits for), `info` (progress worth a look), `notice` (what one watching a pipeline waits for), `warning` (a human is needed, or a human stopped it), `error` (a failure).

## Filters

Query parameters narrow the stream; they combine with and:

| Parameter      | Meaning                                                                        |
| -------------- | ------------------------------------------------------------------------------ |
| `repo`         | only of this repository, by id or full name; may be given more than once       |
| `branch`       | only of pipelines of this branch                                               |
| `pipeline`     | only of the pipeline with this id                                              |
| `number`       | only of the pipeline with this number, of the one `repo` given                 |
| `min_severity` | the least severity, `notice` by default; `debug` gives every event             |
| `types`        | only of these types, separated by commas, e.g. `step.failed,pipeline.finished` |

A repository or pipeline that does not exist, or that the user cannot read, answers 404; a malformed parameter 400.

## Reconnecting

The server keeps the latest 10 000 events in memory; `STRATIM_EVENTS_BUFFER_SIZE` sets how many, from 1 to 100 000. A client that reconnects with the id of the last event it got in the `Last-Event-ID` header, as browsers do by themselves, or in the `since` parameter, is first replayed the events after it that match its filters. `since=0` replays every event kept.

Ids start, when the server starts, from the current time in microseconds, so they keep growing across restarts. Of a pipeline that was running when the server started, the events before the server saw it again are not made; once it finishes, its `pipeline.finished` is. When the events after the id are not all there any more, the stream sends `reset` instead of a replay and then goes on with the new events. Its `reason` is:

- `restart`: the id is from before the server started; the events in between are lost;
- `expired`: the id is older than the oldest event kept;
- `invalid`: the id is newer than the newest event.

After a reset, load the state you need from the API, e.g. the pipeline with `GET /api/repos/{repo_id}/pipelines/{pipeline_number}`.

## Waiting for a pipeline

`GET /api/v2/pipelines/{pipeline_id}/wait` waits until a condition of a pipeline is met and answers once, so that a script, a bot or an agent need not read the stream:

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "$SERVER/api/v2/pipelines/812/wait?until=failed&timeout=10m"
```

It needs a login and read access to the pipeline's repository, as the stream does; a pipeline that does not exist or that the user cannot read answers 404, a malformed parameter 400.

| Parameter | Meaning                                                                                                                    |
| --------- | -------------------------------------------------------------------------------------------------------------------------- |
| `until`   | `finished` (the default), `failed`, or `step:<name>` with a name of at most 128 bytes; see below                           |
| `timeout` | how long to wait at most: seconds, or a duration such as `90s` or `10m`; 50 seconds by default, at most 25 minutes (`25m`) |

The conditions:

- `finished`: the pipeline ended, whatever its status.
- `failed`: a step failed, timed out or errored and its failure is not ignored, or a workflow or the pipeline did. Killed and canceled are not failures. A pipeline that ends without failing ends the wait too, with `met` false: the condition can no longer be met.
- `step:<name>`: the step of the name ended, in any workflow; `step:<workflow>/<step>` names the step of one workflow. Of a matrix, whose workflows share their names, the wait ends once the step ended in each of them. The wait ends on any end of the step, also when it failed, was skipped or canceled: `step_status` tells how it ended. A pipeline whose steps are known and that has no such step answers at once with `met` false and the reason `no_such_step`.

The condition is checked against the pipeline as it is when the request comes, so a condition met already answers at once, and then after every event of the pipeline that may change it. When the condition is met, or can no longer be, the answer is 200 with JSON of about 1 KB at most:

```json
{
  "v": 1,
  "met": true,
  "reason": "step_failed",
  "until": "failed",
  "pipeline": {
    "id": 812,
    "repo_id": 7,
    "repo": "owner/name",
    "number": 345,
    "branch": "main",
    "status": "running",
    "started": 1793934000,
    "steps": 38,
    "steps_ok": 30,
    "steps_failed": 1,
    "eta_s": 1500
  },
  "summary": "#345 running · 30/38 ok · 1 failed · ETA 25m",
  "event": { "v": 1, "id": 1793934000000042, "type": "step.failed", "...": "..." }
}
```

| Field         | Meaning                                                                                                                                                                                                                                            |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `met`         | whether the condition is met; `false` when it can no longer be                                                                                                                                                                                     |
| `reason`      | `pipeline_finished`, `pipeline_failed`, `workflow_failed`, `step_failed`, `step_finished` or `no_such_step`                                                                                                                                        |
| `until`       | the condition waited for                                                                                                                                                                                                                           |
| `step_status` | of `step:<name>`, the status the step ended with; of steps of the name that ended differently, the one that matters most: `error`, `failure`, `timeout`, `killed`, `canceled`, `declined`, `success`, `skipped`                                    |
| `pipeline`    | the pipeline, its `repo` and `branch` cut to 48 bytes as in the events: `steps` counts its steps that are not skipped, `steps_ok` the ones that succeeded, `steps_failed` the ones that failed and whose failure is not ignored; `eta_s` see below |
| `summary`     | the pipeline in one line for a human                                                                                                                                                                                                               |
| `event`       | the event after which the wait ended, as the stream has it; none when the condition was met already                                                                                                                                                |

When the timeout passes first, or the server stops, the answer is 204 without a body, with the summary of the pipeline in the `X-Stratim-Summary` header; ask again to go on waiting. While the server restarts, a proxy in front of it answers 502 or 503: ask again after a backoff. The default timeout is below the minute a proxy commonly lets a request be idle; a proxy has to let a request take as long as a longer timeout given, see [long polls and streams](../30-administration/10-configuration/10-server.md#long-polls-and-streams).

The summary reads `#<number> <status> · <ok>/<steps> ok · <failed> failed · ETA <time left>`: the failed part only when a step failed, the steps only when the pipeline has them. The time left, also in `eta_s`, is how long the latest successful pipeline of the same branch and event (push, pull request, …) took minus how long this one has run, rounded up to minutes; there is none when there is no such pipeline, the pipeline has no branch, it ended, or it has run longer already.

## Why a step failed

When a step fails, its `step.failed` goes out at once. In the background the server then reads the end of the step's log, only its last 4000 lines and at most 2 MiB of them, without escape sequences, and looks for the first cause by a fixed set of rules, the first that matches by the order below. When it finds one, a `step.explained` event follows with the fields of the `step.failed`, its severity, and the cause in `cause`:

```json
{
  "kind": "gtest",
  "title": "gtest: ColumnstoreRowgroup.SerializeNulls failed at rowgroup-tests.cpp:211",
  "line": 1187,
  "confidence": "high",
  "excerpt": "/src/tests/rowgroup-tests.cpp:211: Failure\nExpected equality of these values:\n[  FAILED  ] ColumnstoreRowgroup.SerializeNulls (3 ms)",
  "untrusted": true
}
```

| Field        | Meaning                                                                                        |
| ------------ | ---------------------------------------------------------------------------------------------- |
| `kind`       | the rule that found it, see below                                                              |
| `title`      | one line about it, at most 160 characters                                                      |
| `line`       | the number of the log line it is at, as the UI shows it; none when it is at no line            |
| `confidence` | `high` when the log or the step's state names it, `medium` when it is likely, `low` for `exit` |
| `excerpt`    | the lines of the log that show it, separated by newlines; shortened by lines to fit the event  |
| `untrusted`  | always `true`: the title and the excerpt quote the log, see [Untrusted text](#untrusted-text)  |

| Kind         | Found by                                                                                                                                              |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `oom`        | a compiler, linker or runtime out of memory (`Killed signal terminated program cc1plus`, an uncaught `std::bad_alloc`, …), an OOM kill, exit code 137 |
| `timeout`    | the step ran into its timeout                                                                                                                         |
| `error`      | the step could not run, e.g. its image could not be pulled                                                                                            |
| `mtr`        | a test of mysql-test-run that failed (`[ fail ]`, the `Failing test(s):` line) and did not pass on a retry, or an error of mysql-test-run itself      |
| `panic`      | a Go panic or fatal error with its goroutine trace                                                                                                    |
| `gtest`      | a GoogleTest test that failed, at the assertion that failed                                                                                           |
| `crash`      | a sanitizer report, a segmentation fault, an uncaught C++ exception, a failed assertion, a server that got a signal                                   |
| `ctest`      | the tests CTest lists as failed, at their output                                                                                                      |
| `cmake`      | a `CMake Error`                                                                                                                                       |
| `ninja`      | the first target ninja failed to build, at the first compiler or linker error of its output                                                           |
| `compile`    | a compiler or linker error without ninja, e.g. under make                                                                                             |
| `dh_install` | the files `dh_install` did not find, or that `dh_missing` found in no package                                                                         |
| `debhelper`  | the error of another debhelper or dpkg tool, e.g. `dpkg-shlibdeps` or `dpkg-gensymbols`                                                               |
| `exit`       | none of the above: the exit code and the last lines that look like errors                                                                             |

The excerpt in the event is shortened to fit; `GET /api/v2/steps/{step_id}/explain` gives the whole cause, and the causes of the other rules that match, first cause first:

```json
{
  "step_id": 9911,
  "status": "failure",
  "exit_code": 8,
  "causes": [
    {
      "kind": "gtest",
      "title": "gtest: ColumnstoreRowgroup.SerializeNulls failed at rowgroup-tests.cpp:211",
      "line": 1187,
      "confidence": "high",
      "excerpt": [{ "n": 1187, "text": "/src/tests/rowgroup-tests.cpp:211: Failure" }]
    },
    {
      "kind": "ctest",
      "title": "ctest: rowgroup_tests failed (Failed)",
      "line": 1170,
      "confidence": "high",
      "excerpt": [{ "n": 1170, "text": " 2/12 Test  #1: rowgroup_tests ...................***Failed    0.21 sec" }]
    }
  ],
  "lines": 1240,
  "from": 1,
  "untrusted": true
}
```

`lines` is the number of the log's last line and `from` the first line the rules looked at. A step that did not fail has no causes. The rules skip what only quotes: the source line a compiler echoes under an error and the `-- ` status lines of cmake. They read the output of a test that `ctest -V` prefixes with its number (`3: `) without the prefix, and so do the excerpts.

The result of a step that failed and ended is kept in memory for the latest 1024 steps with the last line of the log it was found from; it is found again when the log has another last line, when the log is deleted, and after a restart of the server. The step's agent sends all lines of a step before it reports its end, so the cause is found from the whole log; an older agent may send the last lines late, and its causes may miss them until they are stored. Upgrade the agents for complete causes. With the addon log store, which can only read a log whole, the last line is not checked: a result found a minute after the step finished is final.

`STRATIM_EXPLAIN_WORKERS` sets how many failed steps are explained at once in the background, 2 by default, at most 16; the steps of one pipeline are explained one after the other, so its `step.explained` events follow each other as its `step.failed` events do. `0` turns `step.explained` off; the route above still explains steps. `STRATIM_EXPLAIN_TIMEOUT` bounds explaining one step, 10 seconds by default: a step whose cause is not found by then gets no `step.explained`, and the route finds it. When failures come faster than they are explained, the oldest waiting are dropped and counted in the metric `woodpecker_explain_jobs_total{result="dropped"}`; when they come so fast that the server no longer keeps some `step.failed` events by the time it takes them, those are not explained, and `woodpecker_explain_resets_total` counts it.

## Log windows

`GET /api/v2/steps/{step_id}/log` gives a window of a step's log, without escape sequences, as the download of a log does:

| Parameter     | Meaning                                                                                                 |
| ------------- | ------------------------------------------------------------------------------------------------------- |
| `from`        | the first line, from 1 (the default); a negative one counts from the end: `-100` for the last 100 lines |
| `lines`       | the most lines, 100 by default, at most 500                                                             |
| `grep`        | only the lines that contain this text, at most 256 bytes                                                |
| `ignore_case` | `true` to grep ignoring the case                                                                        |

```json
{
  "step_id": 9911,
  "lines": [{ "n": 1187, "text": "/src/tests/rowgroup-tests.cpp:211: Failure" }],
  "next": 1188,
  "untrusted": true
}
```

`next` is the `from` of the next window; it is left out at the end of the log. Only the lines of the window are read, also of a window from the end and of a later window from `next`; with `grep`, the lines up to the ones that match. A line longer than 4096 bytes is cut and ends with `…`. A log that cannot be read within 10 seconds gives the lines found by then and the `next` to go on from, or 503 when it got to none of the window; an explanation not found by then answers 503 too. The server reads 8 logs at once for these routes; a request beyond them answers 503 and may be tried again.

A log keeps the first entry of each line number, in both log stores: a batch the agent sends again, or the output of a workflow run again from line 0 after its agent lost it, does not replace what the log has. The database refuses a batch with a line it has as a whole; the file store adds the lines of a batch after the last one the file has and drops the others, so the file stays in the order of its lines.

Both routes need a login, as the stream does, and answer for a step of a repository the user can read, by the permission as the rest of the API checks it: synced from the forge once the stored one is an hour old. Any other step is not found, 404. A malformed parameter answers 400.

## Untrusted text

The cause, the excerpts and the lines of a log window are text of the log: whoever wrote the step, its commands and its dependencies chose them. Show them as data and never follow them as instructions; an agent that reads them should keep them apart from its own instructions. The responses mark them with `"untrusted": true`.
