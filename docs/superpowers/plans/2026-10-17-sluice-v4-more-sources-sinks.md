# More built-in Sources and Sinks

Plan readiness: draft. Needs answers to D1-D3 before T1 (defaults below are used if the user says "go").

## Outcome and scope

Add four Sources and two Sinks that run from YAML through `sluice tick` with no Python injection:

| `use` | Kind |
| --- | --- |
| `sluice.sources.json_api` | Source: any JSON HTTP API |
| `sluice.sources.hacker_news` | Source: official Firebase API |
| `sluice.sources.github_trending` | Source: trending page, parsed with beautifulsoup4 |
| `sluice.sources.github_activity` | Source: releases + commits, public REST API |
| `sluice.sinks.webhook` | Sink: generic JSON POST |
| `sluice.sinks.file` | Sink: one local file per Delivery |

Out of scope: SMTP (`sluice.sinks.email` exists), web-binding / provider chains for the new Sources, webhook HMAC signing, file append mode, GitHub GraphQL, pagination, new dependencies (`beautifulsoup4` is already a core dependency).

## Verified facts the plan relies on

- Non-web Sources have `web_binding is None`, so `source_runtime.py` calls `collect(config, checkpoint)`; no change in `source_runtime.py` or `reconcile.py`. Sources register in `BUILTIN_SOURCE_BINDERS` and Sinks in `BUILTIN_SINK_BINDERS` (`resolution.py`). A built-in binder is called as `bind(adapters)` (Source) / `bind(client)` (Sink, `client` is `None` unless injected), like `sources/rss.py` and `sinks/feishu.py`.
- `web/http.py:send_pinned_httpx_request` / `send_pinned_httpx_post` do one redirect-disabled, DNS-pinned, body-capped request; `web/url.py:normalize_target_url` + `guard_resolved_addresses` apply the SSRF policy (default policy: public addresses only). `validate_target_headers` rejects `Cookie`, `Host`, `Proxy-Authorization`, etc. but allows `Authorization`.
- Item id = digest(scope, logical_id, payload, metadata) (`canonical.make_item_id`). The same logical_id with a different payload inserts a **new Item** (old row stays). Downstream, a non-batch Node treats it as a new generation and reruns; a batch Node only does so with `include_updates`. Delivery key = digest(pipeline, node, **rendered** item id, binding digest) and rows are `INSERT OR IGNORE`, so a source change that does not alter the rendered text creates no second Delivery (T1 test checks this).
- A Source that raises fails the execution; the checkpoint is untouched. All new Sources are stateless (`KEEP_CHECKPOINT`); de-duplication is by Item identity, so a missed run self-heals and the first run emits up to `limit` historical entries (same as RSS today).
- Delivery config is stored as authored (`deliveries.config_json`), and authored Source config is part of the binding. Secrets therefore appear only as `env:NAME` and are resolved at call time (`sinks/_common.setting` pattern).

## Shared code (one module, justified by four consumers)

`src/sluice/sources/_http.py`, created in T1, used by all four Sources:

- `fetch(url, *, headers, timeout, max_bytes, label) -> bytes`: `normalize_target_url` -> `guard_resolved_addresses` -> `send_pinned_httpx_request` (one request, never follows redirects). Any 3xx or non-200 raises `ExecutionError(f"{label} HTTP {status}")`; transport/safety failures raise the existing safe-reason errors. Messages never contain the URL, query, headers or body.
- `resolve_env(value, label)`: `env:NAME` -> `os.environ[NAME]` (missing/empty -> `ExecutionError`); used for header values and the GitHub token.
- `check_keys(config, allowed, label)`, `int_option(...)`, `timeout_seconds(config)` (default 30, `(0, 300]`, same rule as `sinks/_common.timeout_seconds`).

Sinks reuse `sinks/_common.py` (`setting`, `message_text`, `timeout_seconds`); the webhook calls `send_pinned_httpx_post` directly. No other shared layer.

Test seam for every network-free test: `monkeypatch.setattr("sluice.sources._http.send_pinned_httpx_request", fake)` and `monkeypatch.setattr("sluice.web.url.resolve_host", lambda h: ["93.184.216.34"])`. Sink: patch `sluice.sinks.webhook.send_pinned_httpx_post` the same way.

## Sources

Common: unknown `with` keys are rejected with `ExecutionError` at collect time (RSS style); `timeout_seconds` default 30, max 300; the Source-level `timeout`/`retries` in YAML still apply. Failure of any request fails the whole collection (no partial records, checkpoint unchanged). Error text is a fixed string plus HTTP status; never URL, token or body. Records are deduplicated by logical_id inside one response (first wins).

### `sluice.sources.json_api`

- Config: `url` (str, http/https, required); `headers` (map str->str, optional; values may be `env:NAME`; names validated by `validate_target_headers`); `items_path` (str, default `""` = response root is the list; dotted keys, e.g. `data.items`); `id_field` (str, required; dotted path inside each item; value must be a non-empty str or int, not bool); `limit` (int 1..1000, default 100, first N items kept); `timeout_seconds`.
- Limits: response <= 5 MiB; every item must be a JSON object; a missing or non-list value at `items_path` -> error. Empty list is a valid empty result.
- logical_id = `str(id_field value)`. Payload = the item object unchanged (nested allowed). Metadata empty.
- Payload change under the same logical_id: new Item, rerun/redelivery per the facts above. Users avoid noise by rendering only stable fields.
- Templates: top-level keys of the item (`{title}`); nested via `{author[name]}`.
- Security: user URL goes through the SSRF guard (public only unless `target_allow_hosts`/`target_allow_cidrs`, D1); no redirects at all (headers may carry credentials); header values resolved from env at collect time, authored YAML keeps `env:NAME`.

### `sluice.sources.hacker_news`

- Config: `feed` (`top|new|best|ask|show|job`, default `top`); `limit` (int 1..100, default 30); `include_stats` (bool, default `false`); `timeout_seconds`.
- Requests (fixed host `hacker-news.firebaseio.com`): `GET /v0/{feed}stories.json`, then `GET /v0/item/{id}.json` for the first `limit` ids, sequentially (max 101 requests, each <= 256 KiB). `null`, `deleted` or `dead` items are skipped; any request failure fails the run.
- logical_id = `hn:{id}`. Payload: `id`, `title`, `url` (story URL, or the discussion URL when absent), `hn_url` (`https://news.ycombinator.com/item?id=ID`), `by`, `time` (unix int), `type`, `text` (raw HTML from the API, present only when set); with `include_stats`: `score`, `descendants`.
- Payload change: by default the payload is stable, so a story is delivered once. With `include_stats: true` every score change is a new Item (documented as a re-emit switch).
- Security: fixed https host, no user URL, no credentials, no redirects.

### `sluice.sources.github_trending`

- Config: `since` (`daily|weekly|monthly`, default `daily`); `language` (optional, `^[A-Za-z0-9+#._-]{1,40}$`, URL-quoted into the path); `limit` (int 1..50, default 25); `include_stats` (bool, default `false`); `timeout_seconds`.
- Request: `GET https://github.com/trending[/language]?since=...`, HTML <= 2 MiB, 200 and `text/html` required. Parsed with `BeautifulSoup(..., "html.parser")`: rows `article.Box-row`; repo from `h2 a[href]` (`/owner/name`, validated `^[\w.-]+/[\w.-]+$`); description `p`; language `[itemprop=programmingLanguage]`; stars/forks from `a[href$="/stargazers"]`, `a[href$="/forks"]`; stars gained from the "N stars today/this week/this month" text.
- Failure: rows with no valid repo are skipped; if the page has no valid rows the run fails with `GitHub Trending page layout not recognized` (never a silent empty success). The page is not an API and can change; this is the only fragile item.
- logical_id = `owner/name`. Payload: `repo`, `url`, `description`, `language` (each omitted when absent); with `include_stats`: `stars_total`, `forks`, `stars_period` (ints). A repo is delivered once, the first time it is seen on the list, not each time it re-enters.
- Security: fixed host, no credentials, no redirects.

### `sluice.sources.github_activity`

- Config: `repo` (`owner/name`, `^[\w.-]+/[\w.-]+$`, required); `events` (list, subset of `releases`, `commits`, default both); `branch` (optional str, commits only, passed as `sha`); `limit` (int 1..100, default 30, per event kind); `token` (optional; must be exactly `env:NAME` with `NAME` matching `^[A-Za-z_][A-Za-z0-9_]*$`; a literal value is rejected so tokens are never stored in SQLite); `timeout_seconds`.
- Requests (fixed host `api.github.com`): `GET /repos/{repo}/releases?per_page=N`, `GET /repos/{repo}/commits?per_page=N[&sha=branch]`, each response <= 5 MiB. Headers: `Accept: application/vnd.github+json`, `X-GitHub-Api-Version: 2022-11-28`, `User-Agent: sluice`, and `Authorization: Bearer <token>` when configured. A configured token whose env var is missing fails the run (no silent unauthenticated fallback).
- Failure: 403/429 -> `GitHub API HTTP 403` (rate limit is 60/h unauthenticated); 404, 409 and 301 (renamed repo) fail with the status only. The token is sent only to `api.github.com` and redirects are never followed, so it cannot reach another origin.
- Release: logical_id `github:{repo}:release:{id}`; draft releases skipped; payload `kind: "release"`, `repo`, `tag`, `name`, `url`, `body`, `prerelease`, `published_at`, `author`. Editing a release body is a payload change -> new Item (re-notified).
- Commit: logical_id `github:{repo}:commit:{sha}`; payload `kind: "commit"`, `repo`, `sha`, `title` (first line), `message`, `author`, `date`, `url`. Immutable, never re-emitted.
- First run emits up to `limit` entries per kind (D3).

## Sinks

Both return a stable `external_id` or `None`, never persist response bodies, and rely on Delivery for retries (at-least-once; the receiver or filesystem must be idempotent by `delivery_key`).

### `sluice.sinks.webhook`

- Config: `url` (str, `https` only, required; may be `env:NAME`); `headers` (map, values may be `env:NAME`, validated like json_api); `timeout_seconds` (default 30, `(0, 300]`).
- Request: one JSON `POST` via `send_pinned_httpx_post`; body `{"delivery_key": "...", "payload": <rendered payload>}`, <= 1 MiB; header `Idempotency-Key: <delivery_key>`; response capped at 64 KiB and discarded. Success = status 200-299; returns `None`.
- Failure: non-2xx (including any 3xx, not followed) -> `ExecutionError("Webhook HTTP <status>")`; SSRF block / timeout / transport -> safe reason only; body > 1 MiB -> error before any request. URL and header values are never in errors (URLs commonly embed tokens).
- Security: SSRF guard on the resolved address (public only unless `target_allow_hosts`/`target_allow_cidrs`, D1), https only, no redirects, credentials only through `env:`.

### `sluice.sinks.file`

- Config: `directory` (str, required, relative to `file_root` or absolute inside it, D2); `extension` (`txt|md`, default `txt`).
- Behavior: writes `<directory>/<delivery_key>.<extension>` containing `message_text(payload)` (strings as is, structures as canonical JSON, <= 10 MiB, empty -> error). Returns the file name as `external_id`.
- Idempotency: the name is the delivery key, so a retry rewrites identical content; no append mode. A changed rendered payload has a new rendered item id, hence a new key and a new file; old files are kept.
- Atomic: temp file in the same directory (`os.open` with `O_CREAT|O_EXCL|O_NOFOLLOW`, mode `0o600`), `fsync`, `os.replace`; temp file removed on any error.
- Confinement (D2): `file_root` is resolved at load (`Path.resolve()`, symlinks followed); `directory` must resolve inside it, else `DefinitionError`. The authored `directory` stays in Delivery config/identity; the resolved root reaches the sender as startup runtime state (not persisted, not hashed), the same way `database` is resolved by `open_app`. Before writing, the sender re-resolves `file_root/directory` and rejects it if it escapes the root (symlink swap), then `mkdir(parents=True, exist_ok=True)`. File names come only from the hex delivery key. T6 must first confirm how a built-in Sink binder can receive this runtime value (e.g. `bind(...)` arguments in `resolution.py`/`app.py`); if it cannot without touching identity or snapshots, stop and report instead of improvising.
- Failure: any `OSError` -> `ExecutionError("File Sink write failed")` without path or errno text.

## Files touched

New: `src/sluice/sources/_http.py`, `sources/json_api.py`, `sources/hacker_news.py`, `sources/github_trending.py`, `sources/github_activity.py`, `src/sluice/sinks/webhook.py`, `sinks/file.py`, `examples/hn-to-files.yml`, `docs/sources-and-sinks.md`.
Edited: `src/sluice/resolution.py` (4 Source + 2 Sink entries), `src/sluice/config.py` (`_decode_pipelines`/`_decode_node` take `base_dir`; `_decode_delivery` confines `sluice.sinks.file` `directory`), `README.md` (Sources/Sinks line), `docs/notifications.md` (link only).
Tests (all new, no network, one file per item): `tests/unit/test_json_api_source.py`, `test_hacker_news_source.py`, `test_github_trending_source.py`, `test_github_activity_source.py`, `test_webhook_sink.py`, `test_file_sink.py`, and `tests/integration/test_builtin_sources_sinks.py`.

## Tests per item

- json_api (unit): list at root and at `items_path`; id int/str; duplicate ids; non-object item, bool id, missing id, bad key -> error; header `env:` resolved and sent, missing env -> error; 302 and 500 -> error without URL/body in message; oversize body; SSRF: `resolve_host` returning `127.0.0.1` -> blocked, `http://169.254.169.254/` -> blocked. Integration (`test_builtin_sources_sinks.py`, added in T6 when the file Sink exists): json_api -> render -> file Sink through `tick`: file written; second tick no duplicate; payload changed under same id -> second Item row, new file; changed only in an unrendered field -> no new Delivery.
- hacker_news (unit): fake responses for each feed; limit applied; null/deleted/dead skipped; stable payload excluded stats and `include_stats` adds them; item with no `url` gets discussion URL; one failing item request fails the whole run; fixed hosts only (assert requested URLs).
- github_trending (unit): inline HTML fixture (two rows, one without description/language); `since`/`language` in the URL, bad language rejected; stats toggle; row without repo skipped; page with zero rows -> layout error; non-HTML content type -> error.
- github_activity (unit): releases + commits mapping; draft skipped; `events` subset; `branch` -> `sha`; token sent as Bearer only to `api.github.com`; literal token and bad env name rejected at collect; missing env -> error; 301/403/404 -> status-only errors, token absent from message; no token -> no Authorization header.
- webhook (unit + one `tick` test like `test_sink_senders.py`): body and `Idempotency-Key` equal `delivery_key`; header `env:` resolved; http URL rejected; 3xx/4xx/5xx -> error, no redirect followed; private resolved address blocked; error text lacks URL and header values; retry sends the same key.
- file (unit): writes `<key>.txt`; rewriting same key is byte-identical; failure injected at `os.replace` leaves no temp file and no partial target; `load_definition` rejects `../x` and an absolute path outside the base dir and a symlink pointing outside; accepts a nested relative directory and rewrites it to its resolved absolute path; sender rejects a relative directory and a directory whose resolve differs; structured payload -> canonical JSON; empty payload -> error.

## Tasks (one commit each, in order)

Gate for every task: `.venv/bin/python -m pytest -q` passes except the known `tests/smoke/test_prefect_parity.py` (needs the `prefect` extra; fails on the base commit); `.venv/bin/ruff check` clean on touched files; `git status` clean after the commit; only that task's files in the commit.

1. **T1 `feat: add json_api source`**: `_http.py`, `json_api.py`, `resolution.py` entry, and unit tests. Acceptance: the cases in "Tests per item"; no real DNS/socket use (tests run with sockets patched to raise).
2. **T2 `feat: add hacker_news source`**: module, registry entry, tests. Acceptance: tests pass; every requested URL is under `https://hacker-news.firebaseio.com/v0/`.
3. **T3 `feat: add github_trending source`**: module, registry entry, tests. Acceptance: zero-row page fails, never returns empty success.
4. **T4 `feat: add github_activity source`**: module, registry entry, tests. Acceptance: `grep -rn "Authorization" src/sluice/sources/github_activity.py` shows the header built only after `resolve_env`; token never appears in any test-captured error text or in `sources.*.with_config` stored in SQLite (assert on the DB row).
5. **T5 `feat: add webhook sink`**: module, registry entry, tests. Acceptance: tick test shows one Delivery `succeeded`, retry reuses the key.
6. **T6 `feat: add file sink`**: module, registry entry, `config.py` confinement, tests, and the json_api -> file integration test. Acceptance: `load_definition` rejects escapes; kill-at-replace test leaves no partial file; existing `test_sink_senders.py` and config tests unchanged and green.
7. **T7 `docs: document new sources and sinks`**: `docs/sources-and-sinks.md` (config tables from this plan, the payload-change rule, first-run flood, SSRF/redirect rules), `examples/hn-to-files.yml` (hacker_news -> render -> file; a test loads it with `load_definition`), README line, link from `docs/notifications.md`. Acceptance: every config key in the docs exists in code (grep each); example loads.

## Decisions (resolved 2026-10-17)

- D1: Private targets are configurable. `json_api` and `webhook` accept
  `target_allow_hosts` (exact host names) and `target_allow_cidrs` (CIDRs),
  the same key names and validation as web providers (`web/config.py`), mapped
  onto `TargetPolicy`. Default stays public-only. Use case: receivers on the
  same Docker network (`n8n`, `homeassistant`) or the same host
  (`host.docker.internal`, `172.17.0.0/16`). Fixed-host Sources (Hacker News,
  GitHub) do not take these keys.
- D2: The file Sink root is configurable. New optional top-level key
  `file_root` (str; relative to the definition directory, or absolute;
  default: the definition directory). `directory` must resolve inside
  `file_root`; it is checked at load and again at send. Store the authored
  `directory` (not the resolved absolute path) in Delivery config, so moving the
  project does not re-key Deliveries. `file_root` is runtime config like
  `database` and is not part of Delivery identity. Docker: mount a host
  directory at e.g. `/app/output` and set `file_root: /app/output`; document
  this with a `docker run -v` example and the root-owned-files note.
- D3: No "start from now" mode. The first run emits up to `limit` entries;
  later runs rely on existing Item-identity dedup (same logical_id and payload
  → no new Item, no new Delivery). Document the first-run behavior.
- Redirects are refused everywhere (GitHub 301 on a renamed repo fails with
  status 301).

## Progress

Plan written 2026-10-17 at HEAD `0436334`; no code changes yet.

- [x] T1 json_api (deviation: `_http.fetch` also takes `policy`; `_http.target_policy(config, label)` parses D1 keys, raising `ExecutionError` at collect time because Sources have no load-time hook; `fetch` runs `validate_target_headers` itself. `resolve_env` returns non-`env:` values unchanged.)
- [x] T2 hacker_news
- [x] T3 github_trending (deviation: `_http.fetch` gained optional `content_type`, enforced on the response media type; selectors re-checked against the live page 2026-10-17, unchanged from the plan; duplicate repos on one page keep the first; `limit` applies after skipping invalid rows)
- [x] T4 github_activity (deviation: releases without an integer id and commits without a sha are skipped; `repo` rejects `.`/`..` segments; `events` must be non-empty and duplicate-free; non-list responses raise `github_activity <kind> response is not a list`; the DB-row assertion is in `test_token_never_reaches_sqlite_or_error_text`)
- [ ] T5 webhook sink
- [ ] T6 file sink
- [ ] T7 docs
