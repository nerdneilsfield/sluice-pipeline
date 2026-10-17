# Configure Crawl4AI 0.9.x

The Crawl4AI provider targets the 0.9.x Docker REST API only. It sends one synchronous `POST /crawl` per provider attempt, with ordinary `browser_config` and `crawler_config` dictionaries. It does not retry using legacy request formats or poll job endpoints.

## Server requirements

Set `CRAWL4AI_API_TOKEN` on the Crawl4AI server and expose it through an appropriate private network or HTTPS reverse proxy. Without a configured token, 0.9 defaults to container-internal loopback, so a published Docker port may still be inaccessible. Sluice's `api_key` supplies the Bearer token; it is separate from headers sent to the target webpage.

If you configure target headers or cookies, also set `CRAWL4AI_HOOKS_ENABLED=true` on the server. Sluice sends the fixed declarative actions `set_headers` and `add_cookies`, never inline Python hook code. Without headers or cookies, no hooks are sent and the hooks feature need not be enabled.

## Provider configuration

Add this provider to your definition's `web` section and reference its fetch chain from a webpage Source or fetch Task:

```yaml
web:
  providers:
    browser:
      type: crawl4ai
      base_url: http://crawl4ai.internal:11235
      api_key: env:CRAWL4AI_API_TOKEN
      wait_until: domcontentloaded
      wait_for_selector: article
      delay_before_return_html: 1s
  fetch_chains:
    browser:
      providers: [browser]
```

Replace the service hostname with your deployment address. Plain HTTP service endpoints must resolve exclusively to non-public addresses; use HTTPS for public endpoints. The upstream server also validates target URLs independently.

`wait_for_selector` accepts CSS, with or without the `css:` prefix. JavaScript waits are rejected. Target `target_headers` and `target_cookies` use the existing provider configuration fields; the adapter maps them to hooks rather than forbidden BrowserConfig fields. The upstream hook limits are 20 headers, 20 cookies, 64-character alphanumeric/hyphen header names, 256-character cookie names, and 4096-character cookie values. Cookies are scoped to the target hostname and `/`; `secure` is true for HTTPS targets and false for HTTP targets.

Headers set through browser hooks apply to browser requests, potentially including subresources. Only supply target credentials to a trusted Crawl4AI deployment and target site. Use environment references for secrets because authored literal configuration is retained in SQLite.

## Response handling

Sluice requires `success: true` and exactly one successful result in `results`. It reads `html` and the Markdown result, including `markdown.raw_markdown`. Old top-level result bodies and `data` envelopes are rejected. A failed per-URL result is a provider failure even if the outer response reports success; upstream error bodies are not retained as error messages.

When hooks were requested, Sluice also checks that the response reports successful attachment at `before_goto` for headers and `on_page_context_created` for cookies. Missing attachment is a failure, not permission to continue without credentials. A disabled hook feature normally returns HTTP 403; enable it server-side rather than expecting a client-side fallback.

Raw feed fallback only accepts an explicit exact `body` field. Browser-rendered `html` and `cleaned_html` are not treated as original XML bytes. A server that returns only rendered HTML cannot supply an RSS raw-source fallback; use the direct RSS fetch or another provider exposing the original body.

Protocol and safety tests use fake transports and the published 0.9.5 hook schemas. No live Crawl4AI deployment was available for this change.

References: [0.9.5 migration guide](https://github.com/unclecode/crawl4ai/blob/v0.9.5/deploy/docker/MIGRATION.md), [hook schemas](https://github.com/unclecode/crawl4ai/blob/v0.9.5/deploy/docker/hook_registry.py).
