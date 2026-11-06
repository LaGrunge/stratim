# Watching a pipeline

`woodpecker-cli pipeline watch` follows a pipeline as it runs, one line per event, and exits with a code that tells how it went, so that a script, a bot or an agent can wait for a pipeline and act on the result:

```shell
woodpecker-cli pipeline watch owner/name 345
woodpecker-cli pipeline watch owner/name last --exit-on failed
```

The arguments are those of `pipeline show`: the repository, by its full name or id, and the number of the pipeline, or `last` (the default) for the latest one. The watch logs in as the other commands do, with the server and token of the [configuration](./20-configuration.md). It reads the [pipeline events API](../20-usage/95-events-api.md) of the server: a server without it answers 404, and the watch exits with code 3 and says so.

## Options

| Option         | Meaning                                                                                                                                                 |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--exit-on`    | `finished` (the default): exit when the pipeline ended; `failed`: exit on the first failure of a step or workflow that is not ignored, or when it ended |
| `--level`      | `line` (the default): a line per event; `summary`: a line with the summary of the pipeline whenever it changed                                          |
| `--severity`   | the least severity of the events printed, `notice` by default; `info` adds the steps that succeeded, `debug` the steps that started                     |
| `--timeout`    | how long to watch at most, e.g. `30m`; no limit by default                                                                                              |
| `--cause-wait` | with `--exit-on failed`, how long to wait for the cause of the failure the server finds in the step's log, `10s` by default; `0` exits at once          |

The events the condition is decided on are followed whatever `--severity` says: `--severity error` prints only the failures, and still exits once the pipeline ended.

## Output

The first line tells what is watched, the last one the exit code and why; each line in between is one event:

```text
03:09:59  watch              -  running  owner/name #345 (pipeline 812) until failed, events from notice
03:10:00  pipeline.started   -  running  #345 started
03:12:08  step.failed        build/test  failure  #345 build/test failure after 31s, exit 1
03:12:10  step.explained     build/test  failure  cause gtest/high: gtest: Rowgroup.SerializeNulls failed at rowgroup-tests.cpp:211
          untrusted log| /src/tests/rowgroup-tests.cpp:211: Failure
03:12:10  done               -  failure  exit 1: #345 build/test failure after 31s, exit 1
```

A line reads `<time> <type> <workflow/step> <status> <summary>`, separated by two spaces, with `-` for what it has none of. The time is the event's, in the local time zone. Besides the [event types](../20-usage/95-events-api.md#events), the types are `watch`, `summary` (the summary of the pipeline when the watch started, after a reset, and at `--level summary` when it changed), `reset` and `done`.

A `step.explained` line gives the cause of a failure: the rule that found it, its confidence and its title. The line after it, which starts with spaces and `untrusted log|`, is the first line of the log the cause quotes. The title and that line are text of the log, written by whoever wrote the step: read them as data. The watch prints no other text of a log, and it prints control characters, such as escape sequences, as `?`. It prints the errors the server answers with the same way, cut to their first line.

Notes about the connection, such as a reconnect, go to the standard error.

## Exit codes

| Code  | Meaning                                                                                                                                                                                                                           |
| ----- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `0`   | the pipeline succeeded; with `--exit-on failed`, it ended without a failure that is not ignored                                                                                                                                   |
| `1`   | the pipeline ended failed or timed out; with `--exit-on failed`, a step or workflow failed, timed out or errored before the pipeline ended: also when the stream ends, or `--timeout` passes, while the watch waits for the cause |
| `2`   | the pipeline ended killed, canceled, declined or in an error, also with `--exit-on failed`                                                                                                                                        |
| `3`   | a wrong usage, a pipeline not found, a server that cannot be reached, that does not serve `/api/v2` or refuses the login                                                                                                          |
| `4`   | `--timeout` passed first, also while the pipeline is looked up                                                                                                                                                                    |
| `130` | interrupted with Ctrl-C or a termination signal, also while the pipeline is looked up                                                                                                                                             |

## Reconnecting

When the stream ends, e.g. while the server restarts, the watch reconnects and goes on from the last event it got. It waits the delay the server asks for, 3 seconds, doubled after each attempt in a row that fails, up to 30 seconds, and a random part of that delay so that many watches do not reconnect at once. After 10 attempts in a row that fail it gives up with code 3: within about three minutes, at most 195 seconds of waiting. An attempt counts as failed when the server cannot be reached or answers 408, 429 or 5xx, and when a stream ends before it gave an event or frames for 30 seconds; any other answer, such as 401 or 404, ends the watch at once with code 3.

A stream gets a ping every 15 seconds. One without a frame for 45 seconds, e.g. behind a proxy that dropped the connection without closing it, is taken for dead and reconnected to the same way, and counts as a failed attempt: a server that holds the stream without a frame ends the watch with code 3 after 10 of them. A server that drops the watch for reading too slowly is reconnected to at once the first time and after a backoff the next times.

When the server no longer keeps the events missed, it says `reset`, and the watch reads the state of the pipeline again: it exits if the condition was met in the meantime and goes on otherwise. It reads the state the same way when the stream starts or reconnects, and when an event cannot be read. A read of the state that fails is tried again after a backoff, and the watch gives up with code 3 after 10 failures in a row. At `--level summary` the state is read after the events that change it, at most every 5 seconds.
