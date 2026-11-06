# Bot tokens

A bot token lets a program, a script or an AI agent call `/api/v2` on your behalf with only what it needs: the scopes you give it, on the repositories you name, for a limited time. Unlike your personal access token, which can do everything you can, a bot token never reaches the rest of the API, each one can be revoked on its own, and every call made with it is recorded.

Create one in your user settings, under **Bot tokens**: give it a name, its scopes, optionally the repositories it is restricted to, and how long it lives. The token is shown once, when it is created: copy it then.

```bash
curl -H "X-Stratim-Token: $STRATIM_BOT_TOKEN" "$SERVER/api/v2/whoami"
```

## Scopes

| Scope     | Allows                                                                                                                                                                                                       |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `read`    | [`GET /api/v2/events`](95-events-api.md), [`GET /api/v2/pipelines/{pipeline_id}/wait`](95-events-api.md#waiting-for-a-pipeline), `GET /api/v2/steps/{step_id}/explain` and `GET /api/v2/steps/{step_id}/log` |
| `restart` | `POST /api/v2/pipelines/{pipeline_id}/restart`: restart a pipeline, or only its failed workflows                                                                                                             |

`GET /api/v2/whoami` tells who the token belongs to, its scopes and when it expires; any bot token may call it. `annotate` is reserved for annotations of pipelines and cannot be granted yet.

Every other route answers a bot token with 403, among them the whole API outside `/api/v2`, user and admin settings, secrets, and the management of bot tokens: a bot token cannot create, list or revoke bot tokens.

## What a token may do

A bot token may do what its scope allows, on the repositories it is restricted to, and never more than its owner may: a restart needs the owner's push access to the repository, the events, waits and steps are those of the repositories the owner may read. A token restricted to some repositories gets the events of those only, and 404 for the others on every route. A token is never an admin, even when its owner is: it acts with the permissions its owner has on each repository. The token of an admin has to name its repositories; a token that names none is refused once its owner becomes an admin.

A token lives 30 days unless you choose otherwise, at most 365. Once it expired or was revoked, it is refused with 401, and a stream open with it ends within a minute with an `error` event, `unauthorized`. Revoking a token does not touch your other tokens, nor resetting your personal access token your bot tokens. At most 20 tokens of a user may be active at once.

## Sending the token

Send the token in the `X-Stratim-Token` header:

```bash
curl -N -H "X-Stratim-Token: $STRATIM_BOT_TOKEN" "$SERVER/api/v2/events?repo=owner/name"
```

`Authorization: Bearer stb_…` works too, the scheme in any case, where no proxy in front of the server uses that header for its own authentication. A request that carries a bot token is authenticated by it alone: a cookie or another token next to it is not looked at, and a wrong token is refused with 401 rather than served as a guest. Any `X-Stratim-Token` header makes a request one of a bot token: empty, given twice, or not starting with `stb_`, it is refused with 401.

A bot token in the query (`?access_token=stb_…`) is refused, so that clients do not send it there, where it ends up in the logs of proxies; revoke a token that was sent that way.

The CLI and the Go client (`woodpecker-go`) send a token that starts with `stb_` in this header by themselves; `woodpecker.NewBotTokenTransport` does it for any `http.Client`.

### Behind a proxy

A proxy in front of the server has to pass the `X-Stratim-Token` header unchanged on the requests under `/api/v2/`, and let them take as long as a [long poll or a stream](../30-administration/10-configuration/10-server.md#long-polls-and-streams) does. A proxy that authenticates the users of the web UI itself, with the `Authorization` header, can let through without its own authentication the requests under `/api/v2/` whose `X-Stratim-Token` header is one value matching `^stb_`: the server refuses any of them whose token is not a valid bot token, and every route of `/api/v2/` needs a login. Bot clients behind such a proxy use `X-Stratim-Token`, not `Authorization`.

For the client's address in the audit, set `STRATIM_TRUSTED_PROXIES` to the addresses of the proxies, see [trusted proxies](../30-administration/10-configuration/10-server.md#trusted-proxies).

## Audit

Every call made with a known bot token, refused ones included, is recorded: when, the route (not the full path or query; a path that matches no route is recorded as it was sent, cut to 255 bytes), the repository and pipeline it was about (for a call that was answered, not one that was refused), the answer's status, how long it took, the client's address, and the reason a restart was given or why the call was refused. The server also logs a line for each, at the info level. A call with a token the server does not know is refused and only logged, at most a few lines a minute. The owner, or an admin, lists the calls of a token with `GET /api/v2/bot-tokens/{token_id}/calls`.

The calls are kept 30 days; `STRATIM_BOT_TOKEN_AUDIT_RETENTION` (at least `1h`) sets how long.

## API

| Request                                   | Does                                                                                                                                                                               |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `POST /api/v2/bot-tokens`                 | creates a token: `name`, `scopes`, `repo_ids` (optional), `expires_in_days` (1 to 365, 30 if left out). The answer carries the token in `token`, the only time the server tells it |
| `GET /api/v2/bot-tokens`                  | lists your tokens, newest first, with their `status`: `active`, `expired` or `revoked`; `?all=true` lists those of all users, for an admin                                         |
| `DELETE /api/v2/bot-tokens/{token_id}`    | revokes a token; it stays listed                                                                                                                                                   |
| `GET /api/v2/bot-tokens/{token_id}/calls` | the latest calls of a token, `?limit=` up to 1000                                                                                                                                  |

These need your login or personal access token. The server keeps only a hash of each token.
