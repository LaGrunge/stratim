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

Every event is one line of JSON of at most 400 bytes:

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

Names longer than 48 bytes are cut and end with `…`; the ids are exact. New fields may be added within a schema version: ignore the fields and event types you do not know.

| Type                | When                                | Severity                                                                                                  |
| ------------------- | ----------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `pipeline.created`  | a pipeline is created               | info                                                                                                      |
| `pipeline.blocked`  | a pipeline waits for approval       | warning                                                                                                   |
| `pipeline.started`  | its first workflow starts           | notice                                                                                                    |
| `pipeline.finished` | it ended                            | notice on success; error on failure, error or timeout; warning when killed, canceled or declined          |
| `workflow.started`  | a workflow starts                   | info                                                                                                      |
| `workflow.finished` | a workflow ended                    | notice on success; error on failure, error or timeout; warning when killed; info when skipped or canceled |
| `step.started`      | a step starts                       | debug                                                                                                     |
| `step.finished`     | a step ended without failing        | info on success; warning when killed; debug when canceled before it started                               |
| `step.failed`       | a step failed, timed out or errored | error; notice when its failure is ignored                                                                 |

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
