# Sluice V1.2 - Crawl4AI fetcher and API header support

**Status:** Design approved for implementation planning
**Date:** 2026-05-01
**Owner:** dengqi

## Summary

Sluice should support Crawl4AI as a first-class fetcher in the full-text
fetcher chain. At the same time, Firecrawl and Crawl4AI must both support
custom headers for their own service API requests, especially `Authorization`.

This design deliberately separates service API headers from target-page
headers. The immediate requirement is service API authentication and gateway
headers. Target-page headers are deferred to a later design.

## Verified External API Shape

This design was checked against current public documentation on 2026-05-01.

- Firecrawl v2 scrape endpoint: `POST /v2/scrape`, with service auth in the
  HTTP `Authorization` header. Firecrawl also has a request-body `headers`
  field, but that is for headers sent to the target webpage, not to Firecrawl
  itself.
- Firecrawl v1 remains available as `POST /v1/scrape`, but v2 is the default
  documented API.
- Crawl4AI Docker API exposes `POST /crawl` on the self-hosted server, commonly
  at `http://localhost:11235`.
- Crawl4AI can return synchronous `results` from `/crawl`.
- Crawl4AI also has job-style polling endpoints in recent Docker deployments:
  `POST /crawl/job` and `GET /crawl/job/{task_id}`. Older examples also show
  `GET /task/{task_id}` and `GET /job/{task_id}`.

References:

- https://docs.firecrawl.dev/api-reference/v2-endpoint/scrape
- https://docs.firecrawl.dev/api-reference/v1-endpoint/scrape
- https://github.com/unclecode/crawl4ai
- https://hub.docker.com/r/unclecode/crawl4ai
- https://docs.crawl4ai.com/core/self-hosting/

## Goals

1. Add a `crawl4ai` fetcher that works in the existing `FetcherChain`.
2. Add service API header support for both `firecrawl` and `crawl4ai`.
3. Keep `api_key` compatible for existing Firecrawl configs, but stop treating
   it as the only authentication path.
4. Add Crawl4AI polling so async job responses still produce markdown.
5. Preserve existing fetcher chain fallback behavior: any Crawl4AI or Firecrawl
   failure raises from that fetcher and lets the chain try the next fetcher.

## Non-Goals

- No target-page header injection in this change.
- No Crawl4AI webhook support.
- No Crawl4AI streaming endpoint support.
- No deep-crawl or multi-page Crawl4AI mode.
- No new CI or workflow behavior.

## Configuration Contract

### Shared service API headers

Fetchers that call an external service may accept:

```toml
api_headers = { Authorization = "env:CRAWL4AI_AUTH" }
```

`api_headers` are sent to the fetcher service itself. They are not sent to the
article URL being scraped.

All `api_headers` values support existing `env:` resolution.

Resolution happens in `build_fetcher_chain()` before fetcher construction, at
the same layer that already resolves `api_key`. Fetcher implementations receive
only concrete header values, never raw `env:...` strings.

`extra_headers` is not a public alias for this feature. Existing fetcher
constructors do not consistently accept that parameter, so the public contract
is only `api_headers`.

### Firecrawl

```toml
[fetchers.firecrawl]
type = "firecrawl"
base_url = "https://api.firecrawl.dev"
api_version = "v2"
api_headers = { Authorization = "env:FIRECRAWL_AUTH" }
timeout = 120
```

Rules:

- Default `api_version` is `"v2"`.
- `api_version = "v2"` sends `POST {base_url}/v2/scrape`.
- `api_version = "v1"` sends `POST {base_url}/v1/scrape`.
- `base_url` is normally the service root, for example
  `https://api.firecrawl.dev`.
- If `base_url` already ends with `/v1` or `/v2`, Sluice treats it as a
  versioned root and sends `POST {base_url}/scrape`.
- If a versioned `base_url` conflicts with explicit `api_version`, config
  loading fails with `ConfigError`. Example: `base_url =
  "https://api.firecrawl.dev/v1"` plus `api_version = "v2"` is invalid.
- Request headers are:
  - `Content-Type: application/json`
  - `api_headers`
  - fallback `Authorization: Bearer {api_key}` only when `api_key` is set and
    no explicit `Authorization` exists in `api_headers`.
- Response markdown is read from `data.markdown`.

The Firecrawl request body remains single-page scrape:

```json
{
  "url": "https://example.com/article",
  "formats": ["markdown"]
}
```

### Crawl4AI

```toml
[fetchers.crawl4ai]
type = "crawl4ai"
base_url = "http://localhost:11235"
api_headers = { Authorization = "env:CRAWL4AI_AUTH" }
timeout = 120
poll_interval = 2.0
poll_timeout = 120
poll_paths = ["/crawl/job/{task_id}", "/task/{task_id}", "/job/{task_id}"]
```

Rules:

- Default `base_url` is `http://localhost:11235`.
- Default `timeout` is `120`.
- Default `poll_interval` is `2.0`.
- Default `poll_timeout` is the same as `timeout` unless explicitly set.
- `api_headers` are sent on both the initial request and all polling requests.
- Optional `api_key` may be supported as a compatibility shortcut, using the
  same fallback rule as Firecrawl.

Initial request:

```http
POST {base_url}/crawl
Content-Type: application/json
```

```json
{
  "urls": ["https://example.com/article"]
}
```

Response handling order:

1. Parse inline markdown from the `/crawl` response. If non-empty markdown is
   found, return it immediately.
2. If no inline markdown exists, look for a polling identifier in this order:
   `task_id`, then `job_id`, then `id`.
3. If an identifier is found, poll. Polling checks configured `poll_paths` in
   order, substituting `{task_id}` with the selected identifier regardless of
   the original field name.
4. If no inline markdown and no identifier exists, raise a fetch error with the
   response shape summarized.

Polling success statuses:

- `"completed"`
- `"success"`
- `"done"`

Polling failure statuses:

- `"failed"`
- `"error"`
- `"cancelled"`

Unknown in-progress statuses continue until `poll_timeout`.

## Markdown Extraction Contract

Both synchronous and polling responses should be parsed defensively. The
fetcher should accept the following shapes:

1. `results[0].markdown`
2. `result.markdown`
3. `data.markdown`
4. top-level `markdown`

For each `markdown` value:

- If the value is a non-empty string, return it.
- If the value is a dict, try `raw_markdown`, then `fit_markdown`, then
  `markdown`.
- Any other type is ignored and parsing continues.

If the response indicates success but no markdown can be found, the fetcher
raises a fetch error. The chain may then fall back to the next fetcher.

## Error Handling

Fetcher failures should include enough context for debug logs:

- fetcher name
- URL being fetched
- API endpoint called
- status code when available
- Crawl4AI task id when available
- terminal job status when available

Do not log full `api_headers`. Header names may be logged, but values must not.

HTTP 4xx and 5xx responses raise from the fetcher. The chain catches them and
records the attempt details as it already does.

Crawl4AI polling timeout raises a timeout-style fetch error with the task id
and elapsed seconds.

## SSRF Boundary

The existing Sluice SSRF guard still applies to the article URL before sending
it to Firecrawl or Crawl4AI. This prevents feeds from using the fetcher service
as a proxy to internal addresses.

The fetcher service `base_url` itself is operator-controlled configuration and
is not subject to the target URL SSRF guard.

## Registry and CLI

`crawl4ai` should be registered like existing built-in fetchers.

The CLI explicit plugin import list should include the new fetcher so normal
commands can resolve configs without requiring side-effect imports elsewhere.

## Example Chain

```toml
[fetcher]
chain = ["trafilatura", "crawl4ai", "firecrawl", "jina_reader"]
min_chars = 500
on_all_failed = "use_raw_summary"

[fetchers.trafilatura]
type = "trafilatura"
timeout = 120

[fetchers.crawl4ai]
type = "crawl4ai"
base_url = "http://localhost:11235"
api_headers = { Authorization = "env:CRAWL4AI_AUTH" }
timeout = 120
poll_interval = 2.0
poll_timeout = 120

[fetchers.firecrawl]
type = "firecrawl"
base_url = "https://api.firecrawl.dev"
api_version = "v2"
api_headers = { Authorization = "env:FIRECRAWL_AUTH" }
timeout = 120
```

## Implementation Notes

- Add a small helper in `builders.py` to resolve header maps, including `env:`
  values.
- `build_fetcher_chain()` must resolve `api_headers` before calling the fetcher
  constructor. This mirrors the existing `api_key` resolution point and avoids
  sending literal `env:...` strings to Firecrawl or Crawl4AI.
- Keep `api_key` fallback local to service fetchers.
- Avoid using the name `headers` for service API headers in public examples,
  because Firecrawl already uses `headers` in its scrape body for target-page
  headers.
- Do not add polling to Firecrawl. Firecrawl scrape is synchronous for this
  fetcher.
- Do not add extra tests for docs or CI metadata.

## Acceptance Criteria

- A pipeline can include `crawl4ai` in `fetcher.chain`.
- Crawl4AI synchronous `results` responses return markdown.
- Crawl4AI task responses are polled until success, failure, or timeout.
- Firecrawl sends configured `api_headers` to the Firecrawl API request.
- Crawl4AI sends configured `api_headers` to `/crawl` and polling requests.
- Existing Firecrawl configs using `api_key` still work.
- No secret header values appear in logs.
