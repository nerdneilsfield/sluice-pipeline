# Sources and Sinks reference

This page lists every built-in Source and Sink, its `with` keys, the payload a Source produces, and the rules that decide whether an Item or Delivery is repeated. For Telegram, SMTP email, and Feishu see [Send RSS notifications](notifications.md); they are not repeated here.

| `use` | Kind | Purpose |
| --- | --- | --- |
| `sluice.sources.rss` | Source | RSS 2.0, Atom, RSS 1.0 feeds |
| `sluice.sources.webpage` | Source | One page, or a list of records selected from a page |
| `sluice.sources.json_api` | Source | Any JSON HTTP API that returns a list of objects |
| `sluice.sources.hacker_news` | Source | Hacker News stories from the official Firebase API |
| `sluice.sources.github_trending` | Source | The GitHub Trending page |
| `sluice.sources.github_activity` | Source | Releases and commits of one GitHub repository |
| `sluice.sinks.webhook` | Sink | One JSON `POST` per Delivery |
| `sluice.sinks.file` | Sink | One local file per Delivery |
| `sluice.sinks.telegram`, `sluice.sinks.email`, `sluice.sinks.feishu` | Sink | See [notifications](notifications.md) |

A complete runnable definition is [examples/hn-to-files.yml](../examples/hn-to-files.yml): Hacker News, a render Stage, and the file Sink.

## Rules shared by all Sources

- **Scheduling.** Source-level `schedule` (`<n>m` or `<n>h`), `timeout` (`<n>s`, `<n>m`, or `<n>h`), and `retries` sit beside `with`, not inside it. Set `schedule` on any Source that `tick` should poll repeatedly. Every `schedule` and `max_wait` interval (Source or Node) is at most 366 days, and every `timeout` is at most 7 days.
- **`timeout` (Source and Delivery Node).** It does not interrupt a running call. It is checked only at the start of each reconcile pass: a Source or Delivery attempt still `running` after `timeout` is marked failed, so its late result is discarded and a retry may start on a later pass. A hung call in the same process keeps its thread until it returns; Sluice has no way to cancel it. To bound a hung call, rely on the request timeouts (`timeout_seconds` below, `llm.timeout_seconds`, web provider `timeout`). See [Timeouts](operations.md#timeouts).
- **Stateless collection.** `json_api`, `hacker_news`, `github_trending`, and `github_activity` keep no checkpoint. They fetch the current window on every run and rely on Item identity to drop repeats. An entry that leaves the window (for example, drops out of the first `limit` Hacker News ids) before the next run is never collected, so schedule runs often enough for the window you read.
- **Failure.** A failed request fails the whole collection. No partial records are published. Error text is a fixed message plus the HTTP status where there is one; it never contains the URL, query, header values, token, or response body.
- **Unknown keys.** The four network Sources and the `webhook` and `file` Sinks reject any key not listed for them. The error is raised when the Source runs or the Sink sends, not when the definition loads. The exception is credentials: see [Credentials](#credentials).
- **`timeout_seconds`.** Accepted by `json_api`, `hacker_news`, `github_trending`, `github_activity`, `webhook`. Number greater than 0 and at most 300; default 30. It bounds one HTTP request. `hacker_news` makes one request per story, so a collection can take up to `limit + 1` times this value.

## Deduplication and first run

An Item's identity is a digest of the Source scope, the record's `logical_id`, its payload, and its metadata.

- **First run.** There is no "start from now" mode. The first collection emits up to `limit` entries, and every one of them flows to every Delivery downstream. Point a new definition at a test Sink, or set a small `limit`, before connecting a Sink with side effects.
- **Same `logical_id`, same payload.** No new Item, no new Delivery. Repeat polls are silent.
- **Same `logical_id`, different payload.** This is the payload-change rule. A new Item is inserted and the old row stays. A non-batch Node treats the new Item as a new generation and reruns. A batch Node reruns only with `include_updates`.
- **Delivery key.** A Delivery key is a digest of the pipeline, the Node, the *rendered* Item id, and the Sink binding. If a payload change does not alter the rendered text, the key is the same and no second Delivery is created. Rendering only stable fields therefore suppresses noise from fields that change often, such as view counts.

Which fields change the payload is listed per Source below.

## Template fields

The `render_telegram` and `render_email` Tasks format the payload with `str.format`, for example `template: "{title}\n{url}"`. They are historical names; both produce plain text for any Sink. A missing top-level key renders as an empty string. A list or object value renders as compact JSON with sorted keys (`["a","b"]`). Nested values use index syntax, `{author[name]}`; indexing into a key that is absent fails the render. Write a literal brace as `{{` or `}}`. A Source payload field listed below as "omitted when absent" is therefore safe to reference at the top level.

## Sources

### `sluice.sources.json_api`

Fetches one URL and treats a list in the JSON response as records.

| Key | Type | Default | Limits |
| --- | --- | --- | --- |
| `url` | string | required | `http` or `https`, no user info. Not read from `env:`. |
| `id_field` | string | required | Dotted path inside each item. Value must be a non-empty string or an integer (not a boolean). |
| `items_path` | string | `""` | Dotted keys to the list, such as `data.items`. `""` means the response root is the list. |
| `headers` | map of string to string | none | Values are literal strings, except that a sensitive name must be exactly `env:NAME` (see [Credentials](#credentials)); `env:` values are resolved on each run. Names such as `Cookie`, `Host`, `Proxy-Authorization`, and `X-Forwarded-*` are rejected. |
| `limit` | integer | 100 | 1 to 1000. The first N items are kept. |
| `timeout_seconds` | number | 30 | (0, 300] |
| `target_allow_hosts` | list of strings | none | Unique, non-empty. See [Private targets](#private-targets). |
| `target_allow_cidrs` | list of strings | none | Unique, valid CIDRs. See [Private targets](#private-targets). |

Limits: the response is at most 5 MiB. The first `limit` elements must be JSON objects. A missing `items_path`, a value that is not a list, or an invalid JSON body fails the run. An empty list is a valid empty result. `NaN` and `Infinity` are rejected.

- `logical_id`: `str(<id_field value>)`. Duplicate ids inside one response keep the first.
- Payload: the item object unchanged, nested values included. Template fields are its keys.
- Payload change: any change anywhere in the object is a new Item. Render only stable fields if the API carries counters or timestamps.

### `sluice.sources.hacker_news`

| Key | Type | Default | Limits |
| --- | --- | --- | --- |
| `feed` | string | `top` | `top`, `new`, `best`, `ask`, `show`, `job` |
| `limit` | integer | 30 | 1 to 100 |
| `include_stats` | boolean | `false` | Adds `score` and `descendants` to the payload. |
| `timeout_seconds` | number | 30 | (0, 300] |

The host is fixed to `hacker-news.firebaseio.com`. One request lists story ids, then one request per id, in order: at most 101 requests, each response at most 256 KiB. Deleted and dead items are skipped.

- `logical_id`: `hn:<id>`.
- Payload fields: `id`, `title`, `url`, `hn_url`, `by`, `time`, `type`, `text`. `url` is the story URL, or the discussion URL when the story has none. `hn_url` is `https://news.ycombinator.com/item?id=<id>`. `time` is a Unix timestamp. `text` is raw HTML from the API. `title`, `by`, `type`, `text`, and `time` are omitted when absent.
- With `include_stats: true`: `score` and `descendants`.
- Payload change: without `include_stats`, score and comment-count changes do not create a new Item; an edit to `title` or `text` still does. With `include_stats: true` every score or comment-count change is a new Item. Use it as a re-emit switch, or render only fields that do not change.

### `sluice.sources.github_trending`

Parses the public HTML page at `https://github.com/trending`. The page is not an API and can change without notice.

| Key | Type | Default | Limits |
| --- | --- | --- | --- |
| `since` | string | `daily` | `daily`, `weekly`, `monthly` |
| `language` | string | none | Matches `^[A-Za-z0-9+#._-]{1,40}$`; URL-quoted into the path. |
| `limit` | integer | 25 | 1 to 50, applied after rows without a valid repository are skipped. |
| `include_stats` | boolean | `false` | Adds `stars_total`, `forks`, `stars_period`. |
| `timeout_seconds` | number | 30 | (0, 300] |

The response must be status 200 with media type `text/html`, at most 2 MiB. If the page has no row with a valid repository, the run fails with `GitHub Trending page layout not recognized` rather than reporting an empty success. A repository listed twice keeps its first row.

- `logical_id`: `owner/name`.
- Payload fields: `repo`, `url`, `description`, `language`. `description` and `language` are omitted when absent.
- With `include_stats: true`: `stars_total`, `forks`, `stars_period` (integers, each omitted when the page does not show it).
- Payload change: by default count changes do not create a new Item, and a repository that re-enters the list unchanged is not delivered again; a change to `description` or `language` still creates one. With `include_stats: true` any change in a count is a new Item.

### `sluice.sources.github_activity`

Releases and commits of one repository from the public REST API at `api.github.com`.

| Key | Type | Default | Limits |
| --- | --- | --- | --- |
| `repo` | string | required | `owner/name`, matches `^[\w.-]+/[\w.-]+$`; a `.` or `..` segment is rejected. |
| `events` | list | `[releases, commits]` | Non-empty, no duplicates, subset of `releases`, `commits`. |
| `branch` | string | none | Non-empty. Commits only; sent as `sha`. |
| `limit` | integer | 30 | 1 to 100, per event kind (`per_page`). |
| `token` | string | none | Must be exactly `env:NAME`; a literal token is rejected when the definition loads. |
| `timeout_seconds` | number | 30 | (0, 300] |

Each response is at most 5 MiB. Requests carry `Accept: application/vnd.github+json` and `User-Agent: sluice`, plus `Authorization: Bearer <token>` when `token` is set. If `token` names an unset environment variable the run fails; it does not fall back to anonymous requests. Anonymous requests are limited by GitHub to 60 per hour, and a rate-limit rejection fails the run with `GitHub API HTTP 403` or `429`. A renamed repository answers `301`, which is refused and fails with `GitHub API HTTP 301`.

Release records:

- `logical_id`: `github:<repo>:release:<id>`. Drafts and releases without an integer `id` are skipped.
- Payload fields: `kind` (`release`), `repo`, `prerelease`, and when present `tag`, `name`, `url`, `body`, `published_at`, `author`.
- Payload change: editing a release body or title is a payload change and produces a new Item.

Commit records:

- `logical_id`: `github:<repo>:commit:<sha>`. Commits without a `sha` are skipped.
- Payload fields: `kind` (`commit`), `repo`, `sha`, `title` (first line of the message), and when present `message`, `author`, `date`, `url`.
- Payload change: commits are immutable and are never re-emitted.

The first run emits up to `limit` releases and up to `limit` commits.

### Web Sources: `rss` and `webpage`

These two go through the `web` provider layer (fetch chains, extractors, provider SSRF policy), not through the rules above. Their keys are checked when the definition loads.

| Source | Keys |
| --- | --- |
| `sluice.sources.rss` | `url` (required), `entry_output` (`text` default, or `markdown`), `raw_fallback_chain`, `cursor_param` (alias `checkpoint_param`), `next_checkpoint`, `web_cache_version` |
| `sluice.sources.webpage` | `url` and `fetch_chain` (required); either `list_selector`, or `extract_chain` plus `output` (`markdown` or `text`) with the quality keys `min_chars` (default 500), `min_words`, `max_link_ratio`, `reject_challenges` (always `true`); `next_checkpoint`, `web_cache_version` |

- RSS payload fields: `title`, `link`, `content`, `published`, `updated`, each omitted when the entry lacks it. `logical_id` is the entry's `guid` or `id`, else its link, else its title, else its position.
- Webpage payload fields: `url`, `content`, `title` (omitted when absent). A single page uses its final URL as `logical_id`, so a content change is a payload change.
- Private targets for these Sources are configured on the web provider with the same `target_allow_hosts` and `target_allow_cidrs` keys. See the [README](../README.md#pipeline-concepts).

## Sinks

Sinks are at-least-once. A failed Delivery is retried according to the Node's `retries`, and a retry after a lost response can repeat the side effect. Make the receiver idempotent on the Delivery key.

A Delivery row stores the Node's `retries`, `timeout`, and `concurrency` when it is created. On every `tick` or `reconcile`, the current values from the definition replace the stored ones for rows of the current Sink binding that are `queued` or `failed`. Raising `retries` from `0` to `3` therefore lets already failed Deliveries retry on the next `tick`. `running` and `succeeded` rows keep what they had, and a Sink change is a new binding, not a policy change.

When a Node's Sink or its `with` configuration changes, the Node gets a new binding and new Delivery keys. Rows of the old binding that were never attempted (`queued`, no attempt, `retry_count` 0) can never be sent, so the next `tick` or `reconcile` deletes them. Rows that were ever attempted (`running`, `failed`, `succeeded`, or with a retry) stay as history, and `retry-delivery` rejects them.

Both Sinks below receive the Stage output as their payload. After a render Task that is a string.

### `sluice.sinks.webhook`

Sends one JSON `POST` per Delivery.

| Key | Type | Default | Limits |
| --- | --- | --- | --- |
| `url` | string | required | `https` only. Must be exactly `env:NAME`; a literal URL is rejected when the definition loads. |
| `headers` | map of string to string | none | Same sensitive-name rule and name restrictions as `json_api`: a sensitive name must be exactly `env:NAME`, checked when the definition loads. |
| `timeout_seconds` | number | 30 | (0, 300] |
| `target_allow_hosts` | list of strings | none | See [Private targets](#private-targets). |
| `target_allow_cidrs` | list of strings | none | See [Private targets](#private-targets). |

Request body:

```json
{"delivery_key": "<64 hex characters>", "payload": "<rendered payload>"}
```

- The body is canonical JSON, at most 1 MiB; a larger body fails before any request is made.
- The header `Idempotency-Key` is set to the Delivery key. Any `Idempotency-Key` in `headers`, in any letter case, is removed first, so it cannot be overridden.
- Any status from 200 to 299 succeeds. The response body is read and discarded; a body larger than 64 KiB fails the Delivery even with a 2xx status. `external_id` is empty.
- Any other status, including every 3xx, fails with `Webhook HTTP <status>`. A blocked address, a timeout, or a transport error fails with a fixed reason. The URL and header values never appear in errors, because webhook URLs commonly embed tokens.
- `http://` URLs are rejected, even for a host allowed by `target_allow_hosts`. A receiver on a Docker network that speaks only plain HTTP needs a TLS-terminating proxy in front.

### `sluice.sinks.file`

Writes one file per Delivery.

| Key | Type | Default | Limits |
| --- | --- | --- | --- |
| `directory` | string | required | Relative to [`file_root`](#file-root-and-docker), or absolute and inside it. Created if missing. |
| `extension` | string | `txt` | `txt` or `md` |

- Unknown option names and an `extension` other than `txt` or `md` fail when the definition loads.
- The file is `<directory>/<delivery_key>.<extension>`, and the name is the Delivery's `external_id`. File names come only from the Delivery key, never from payload content.
- Content: a string is written as is. A mapping with a string `content` field writes that field. Any other value is written as canonical JSON. Empty or whitespace-only content fails the Delivery, and so does content over 10 MiB.
- A retry rewrites identical content to the same name. A changed rendered payload has a new Delivery key and so a new file; old files are kept. There is no append mode.
- Writes are atomic: a temporary file in the same directory (mode `0600`) is flushed, synced, and renamed over the final name; the final file therefore has mode `0600`. The temporary file is removed on any error.

## Private targets

By default `json_api` and `webhook` connect only to public addresses. The guard resolves the host name, checks every returned address, and pins the connection to the checked addresses. It blocks loopback, private (RFC 1918 and other private ranges), link-local, multicast, reserved, and unspecified addresses, and the carrier-grade NAT range `100.64.0.0/10`. The link-local block covers cloud metadata addresses. If DNS returns both public and non-public addresses, the request is refused. URLs with user info (`user:pass@host`) are refused.

HTTP Sources and the `webhook` Sink ignore `HTTP_PROXY`, `HTTPS_PROXY`, and the other proxy environment variables (`trust_env=False`), so the address policy stays authoritative: the connection goes to the address that was checked, not to a proxy. A deployment that can reach the target only through a proxy needs a different route to it.

To reach a receiver on a private network, allow it explicitly. These keys exist on `json_api` and the `webhook` Sink only; the fixed-host Sources (`hacker_news`, `github_trending`, `github_activity`) reject them.

| Key | Meaning |
| --- | --- |
| `target_allow_hosts` | Exact host names. A name matches only itself; `n8n` does not allow `n8n.example.com`. Applies to DNS names, not to IP literals. |
| `target_allow_cidrs` | Networks in CIDR notation, IPv4 or IPv6. An IP literal in the URL must be covered by a CIDR. |

Either key opens only the destinations it names. Ports are not restricted. Prefer a host name over a wide CIDR.

Examples:

```yaml
# json_api: Home Assistant on the same Docker network (plain HTTP is allowed for Sources)
sources:
  ha:
    use: sluice.sources.json_api
    schedule: 5m
    with:
      url: http://homeassistant:8123/api/states
      id_field: entity_id
      headers:
        Authorization: env:HA_AUTHORIZATION   # the variable holds "Bearer <token>"
      target_allow_hosts: [homeassistant]
```

```yaml
# webhook: n8n behind HTTPS on the same Docker network
deliver:
  use: sluice.sinks.webhook
  with:
    url: env:N8N_WEBHOOK_URL                  # https://n8n/webhook/...
    target_allow_hosts: [n8n]
```

```yaml
# Service published on the Docker host. On Linux, start the container with
# --add-host=host.docker.internal:host-gateway so the name resolves.
target_allow_hosts: [host.docker.internal]
```

```yaml
# Any container on one Docker network. Read the subnet from
# `docker network inspect <network>`; 172.18.0.0/16 is an example.
target_allow_cidrs: [172.18.0.0/16]
```

A header value of `env:NAME` is replaced whole by the variable's value, so a bearer scheme belongs in the variable.

Invalid values in these keys are reported when the Source runs or the Sink sends, not at load.

## Redirects

`json_api`, `hacker_news`, `github_trending`, `github_activity`, and `webhook` do not follow redirects. Any 3xx status is an error: `GitHub API HTTP 301`, `Webhook HTTP 302`, and so on. Headers may carry credentials, and following a redirect could send them to another origin. Configure the final URL. The `rss` and `webpage` Sources fetch through web providers instead; their HTTPX provider follows up to 10 redirects, checking each target against the same address policy.

## Credentials

Authored configuration is stored in SQLite as written, and the binding identity includes it. Put secrets in environment variables and reference them as `env:NAME`; the value is resolved at run time and is not stored or logged.

These rules are checked when the definition loads, so a literal credential is rejected before anything is written to SQLite. The error names the key, never the value.

- `github_activity` `token`, Telegram `bot_token`, email `password`, Feishu `webhook` and `secret`, and `webhook` `url` must be exactly `env:NAME`. Webhook URLs usually carry the secret in the path or query, which cannot be detected reliably in a literal, so the whole URL is env-only.
- A `json_api` or `webhook` header whose name contains `authorization`, `proxy-authorization`, `cookie`, `set-cookie`, `api-key`, `apikey`, `token`, `secret`, `signature`, `subscription-key`, `access-key`, `x-auth`, or `credential` (case-insensitive) must have an exact `env:NAME` value. Other headers, such as `X-Source: sluice` or `X-Session-Id`, stay literal.

| Place | `env:` accepted | Notes |
| --- | --- | --- |
| `json_api` `headers` values | required for sensitive names | Missing or empty variable fails the run. |
| `github_activity` `token` | required | A literal value is rejected. |
| `webhook` `url`, and `headers` values | `url` required; `headers` required for sensitive names | Missing or empty variable fails the Delivery. |
| Telegram `bot_token`, email `password`, Feishu `webhook` and `secret` | required | A literal value is rejected. |
| `json_api` `url` | no | Do not put a token in the query string; use a header. |

The variables must be set in the environment of the process that runs `tick`. In Docker, pass them with `-e NAME` or `--env-file`.

## `file_root` and Docker

`file_root` is an optional top-level key of the definition. It sets the directory that confines every file Sink `directory`.

| Value | Resolved against | Default |
| --- | --- | --- |
| non-empty string | the definition file's directory, or taken as is when absolute | the definition file's directory |

A `directory` that resolves outside `file_root` is rejected when the definition loads (`DefinitionError`) and again at send time. Both `..` and a symlink pointing outside count. `file_root` is runtime configuration like `database`: the Delivery stores the `directory` as authored, so moving the project or changing `file_root` does not change Delivery keys.

In Docker the definition directory is usually a read-only mount, so mount a separate host directory for output and set `file_root` to it:

```yaml
version: 1
database: /app/data/state.db
file_root: /app/output
```

```yaml
# in the Sink
deliver:
  use: sluice.sinks.file
  with: {directory: stories}                  # becomes /app/output/stories
```

Copy `examples/hn-to-files.yml` to `configs/` and set `database` and `file_root` as above first; the bundled file uses paths relative to itself, which a read-only mount cannot write.

```sh
mkdir -p configs data output
docker run --rm \
  -v "$PWD/configs:/app/configs:ro" \
  -v "$PWD/data:/app/data" \
  -v "$PWD/output:/app/output" \
  ghcr.io/nerdneilsfield/sluice-pipeline:latest tick /app/configs/hn-to-files.yml \
  --at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

Files appear on the host under `output/stories/`. The image sets no `USER`, so the container writes as root: the files and any directory the container creates are owned by root, and files are mode `0600`. To read them as a normal user, run `sudo chown -R "$USER" output` after the run. Create `output` yourself before the first run; if Docker creates the mount point it is also owned by root.

## See also

- [Send RSS notifications](notifications.md) for Telegram, email, and Feishu.
- [Write a definition](configuration.md) for definition-level keys.
- [Operate Sluice](operations.md) for scheduling `tick` and inspecting Delivery state.
