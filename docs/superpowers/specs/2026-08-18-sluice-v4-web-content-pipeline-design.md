# Sluice v4 Web Content Pipeline Design

**Date:** 2026-08-18

**Status:** Proposed for review

**Repository:** `sluice-pipeline-v4`

**Implementation baseline before this Design:** `v4-cleanroom` at `070e8f8`

## 1. Purpose

Sluice v4 needs a production-grade web content path that works across ordinary
HTML pages, JavaScript applications, anti-bot interstitials, and hosted reader
or extraction services.

The current `HTTPClient.get(url) -> str` boundary and local HTML parser remain
useful fallbacks, but they are not sufficient as the complete design. The new
path restores the useful behavior of the historical master implementation,
adapts it to v4's immutable Stage boundary, and adds current providers and
extractors without inheriting the old runtime architecture.

This design covers:

- ordered, configurable fetch fallback per Item;
- separate, configurable content extraction fallback;
- multiple named instances of the same provider type;
- self-hosted and hosted deployments in the same chain;
- parallel processing of different Items through existing Stage concurrency;
- durable whole-chain retry followed by terminal Item drop;
- format-aware handling of raw HTML, text, and Markdown;
- webpage Source and RSS raw-fetch fallback;
- deterministic Stage caching, retained execution, rebuild, and provenance;
- SSRF, redirect, secret, response-size, and paid-request safety.

This design extends the clean-room design. Where the two documents differ for
`fetch_webpage`, `extract_content`, `to_markdown`, webpage Source fetching, or
RSS fallback, this document is authoritative.

## 2. Historical Behavior Retained

The pre-v4 master did not have an independent HTML extractor chain. Its
`FetcherChain` was an ordered URL-to-Markdown chain whose providers combined
fetching and extraction:

- Trafilatura performed direct HTTP fetching and local extraction;
- Crawl4AI performed browser fetching and returned Markdown or raw HTML;
- Firecrawl performed hosted or self-hosted scrape requests and returned
  Markdown or raw HTML;
- Jina Reader returned reader-generated Markdown.

The chain selected the first non-empty result meeting a minimum length. A
separate raw-capable chain used Crawl4AI or Firecrawl as an RSS fallback.

v4 retains the useful product behavior but changes the boundary:

- provider order is still explicit and deterministic;
- the first quality-passing document still wins;
- Trafilatura, Crawl4AI, Firecrawl, and Jina Reader all remain supported;
- RSS can still fall back through raw-capable providers;
- fetching, extraction, quality, retry, and persistence now have explicit
  contracts rather than being coupled to one processor class.

## 3. Design Principles

### 3.1 Two chains, one Item attempt

There are two independently configurable chains:

- `fetch_chain` obtains an artifact from the target URL;
- `extract_chain` derives usable main content and metadata from that artifact.

One `WebContentClient` coordinates both chains. For each fetch provider in
order, it obtains one artifact, applies the extractor chain when needed, and
evaluates the final document quality. If quality fails, it proceeds to the
next fetch provider.

The system does not stop merely because a provider returned HTTP 200. A 200
challenge page, login wall, empty JavaScript shell, malformed document, or
low-quality extraction remains a failed attempt and triggers fallback.

### 3.2 Preserve combined providers

Crawl4AI, Firecrawl, Jina Reader, Zyte, and Bright Data may already provide
rendered HTML, clean text, Markdown, or structured metadata. Their output is
represented directly in the artifact. If supplied Markdown or text passes
quality, the extractor chain uses `identity`; it does not fetch again or run a
second content extractor.

Where a remote service can return HTML and Markdown in one operation, the
adapter requests both in one operation. It must not call one endpoint for
Markdown and then repeat the crawl for raw HTML.

### 3.3 Parallel Items, sequential fallback

Different Items use the existing Stage worker pool and the node's persisted
`policy.concurrency`. A node with concurrency two may resolve two URLs at the
same time.

Providers for one Item are never raced. They execute in declared order and
stop after the first final quality pass. This keeps precedence deterministic,
avoids duplicate paid calls, and preserves one Stage Execution per Item.

No provider or extractor creates a nested executor or a second durable queue.

### 3.4 Stage cache remains the only content cache

This feature does not add the historical URL TTL cache. A successful web
document is cached only through the existing StageResult materialization key.
Provider order, provider options, extractor order, quality options, and
projector options participate in canonical Stage configuration.

An implementation-only provider change that does not alter canonical
configuration requires an explicit Stage `cache_version` bump.

## 4. Public Configuration

Web provider instances and chains are declared at the top level under `web`.
Names identify instances, not implementations. Multiple instances may share
the same `type` and appear consecutively in a chain.

```yaml
version: 1

web:
  providers:
    direct:
      type: httpx
      timeout: 15s
      max_body_bytes: 10485760

    impersonated:
      type: curl_cffi
      impersonate: chrome
      timeout: 20s

    crawl4ai_local_a:
      type: crawl4ai
      base_url: http://crawl4ai-a:11235
      timeout: 120s

    crawl4ai_local_b:
      type: crawl4ai
      base_url: http://crawl4ai-b:11235
      timeout: 120s

    firecrawl_local_a:
      type: firecrawl
      base_url: http://firecrawl-a:3002
      api_version: v2
      timeout: 60s

    firecrawl_local_b:
      type: firecrawl
      base_url: http://firecrawl-b:3002
      api_version: v2
      timeout: 60s

    firecrawl_cloud:
      type: firecrawl
      base_url: https://api.firecrawl.dev
      api_version: v2
      api_key: env:FIRECRAWL_API_KEY
      timeout: 60s

    zyte_cloud:
      type: zyte
      base_url: https://api.zyte.com
      api_key: env:ZYTE_API_KEY
      timeout: 60s

    bright_cloud:
      type: bright_data
      base_url: https://api.brightdata.com
      api_key: env:BRIGHT_DATA_API_KEY
      zone: env:BRIGHT_DATA_ZONE
      timeout: 60s

    jina_cloud:
      type: jina_reader
      base_url: https://r.jina.ai
      api_key: env:JINA_API_KEY
      timeout: 30s

  fetch_chains:
    robust:
      providers:
        - direct
        - impersonated
        - crawl4ai_local_a
        - crawl4ai_local_b
        - firecrawl_local_a
        - firecrawl_local_b
        - firecrawl_cloud
        - zyte_cloud
        - bright_cloud
        - jina_cloud

    raw_recovery:
      providers:
        - impersonated
        - crawl4ai_local_a
        - crawl4ai_local_b
        - firecrawl_local_a
        - firecrawl_local_b
        - firecrawl_cloud

  extractors:
    supplied:
      type: identity
    article:
      type: trafilatura
    news:
      type: newspaper4k
    fast:
      type: resiliparse
    fallback:
      type: beautifulsoup

  extract_chains:
    article:
      extractors:
        - supplied
        - article
        - news
        - fast
        - fallback
```

The recommended ordering is illustrative, not an implicit global default.
Each Source or Task explicitly names its chain. An empty chain, unknown name,
duplicate instance name within one chain, unsupported option, missing secret,
or missing configured optional dependency is a startup error.

The same provider instance may be reused by several different chains. A chain
must not list the same instance name twice. Retrying one instance is governed
by transport and node retry policy, not by duplicating its name.

### 4.1 Common provider fields

Every provider instance accepts the following common fields in addition to
its type-specific fields:

```text
type                  required built-in provider type
base_url              provider service endpoint when applicable
timeout               positive duration string; type-specific default
connect_timeout       positive duration string; no greater than timeout
max_body_bytes        positive integer; default 10485760
transport_retries     non-negative integer; safe-GET default 1, POST default 0
concurrency           positive process-wide instance limit; default 8
cooldown              non-negative duration string; default 60s
service_headers       service-only string mapping; sensitive values env-only
target_headers        target-only string mapping; sensitive values env-only
target_cookies        target-only mapping; every value env-only
proxy                 env reference or credential-free proxy URL
allow_remote_dns_proxy boolean; default false
target_allow_hosts    explicit host allowlist, default empty
target_allow_cidrs    explicit CIDR allowlist, default empty
```

Unknown common or type-specific fields are validation errors. Resolved secret
values are never canonicalized. All sanitized declared fields are retained in
execution snapshots. Canonical Stage keys and Source binding digests include
the provider instance name, type, endpoint, timeouts, retry, formats, browser,
quality, safety, concurrency, cooldown, and every other authored/defaulted
field. Provider concurrency and cooldown can change
which fallback wins, so changing either intentionally partitions identity.

Duration syntax is `^(0|[1-9][0-9]*)(s|m|h)$`; decimals, whitespace, signs,
and mixed units are rejected. `timeout`, `connect_timeout`, `poll_interval`,
and positive action deadlines must be greater than zero. `cooldown`,
`wait_for`, and named delay duration fields may use exactly `0s`; the
Firecrawl `wait` action's `milliseconds` field is a non-negative integer and
does not use duration syntax. Canonical bindings store total integer
seconds with their field names, so equivalent authored units hash equally.

Every `api_key` field requires an exact `env:` reference. Header names are
matched case-insensitively; `authorization`, `proxy-authorization`, `cookie`,
`set-cookie`, and names containing `api-key`, `token`, `secret`, or
`signature` require exact `env:` values. Every target cookie value is env-only.
A literal proxy URL is accepted only without userinfo; a credential-bearing
proxy must be supplied as one complete env reference. Non-sensitive header
values and Bright Data `zone` may be literal strings or env references.
Violations are loader errors, so authored credentials cannot enter canonical
snapshots before redaction.

### 4.2 Provider-specific fields

The initial adapters accept only this whitelist. Nested arbitrary vendor JSON
is not accepted.

| Type | Additional fields and defaults |
| --- | --- |
| `httpx` | No additional fields. Produces raw HTML, text, or raw source. |
| `curl_cffi` | `impersonate` non-empty profile name, default `chrome`; `http_version` one of `auto`, `2`, `3`, default `auto`. |
| `trafilatura` provider profile | HTTPX common fields plus `include_tables` boolean default `true`, `include_comments` boolean default `false`. It returns both the direct raw HTML and locally extracted Markdown without changing the declared extract chain. |
| `crawl4ai` | optional `api_key`; `browser_type` one of `chromium`, `firefox`, `webkit`, default `chromium`; `headless` boolean default `true`; `wait_until` one of `domcontentloaded`, `load`, `networkidle`, default `domcontentloaded`; optional `wait_for_selector`; `delay_before_return_html` duration default `0s`. The initial adapter supports only the synchronous `POST /crawl` API and always sends `cache_mode: bypass`; async job submission, polling, and webhooks are outside this design. |
| `firecrawl` | optional `api_key`; `api_version` is fixed/defaulted to `v2` and any other value is rejected; `only_main_content` boolean default `false`; optional `wait_for_selector`; `wait_for` duration default `0s`; `mobile` boolean default `false`; `actions` ordered list of validated native-subset actions defined below. Sluice asks for Markdown plus `rawHtml` for article requests and only `rawHtml` for list/raw requests. It always sends `maxAge: 0`, `storeInCache: false`, and `skipTlsVerification: false`; these safety fields are not user-overridable. |
| `jina_reader` | `api_key`; optional `target_selector`; `with_generated_alt` boolean default `false`; `remove_selectors` string list default empty. Output is Markdown only. |
| `zyte` | required `api_key`; `mode` one of `auto`, `http`, `browser`, default `auto`; optional ISO country `geolocation`; `actions` ordered validated Zyte-subset actions defined below; `automatic_content` boolean default `true`, mapped to the vendor `pageContent` request field. Auto requests browser HTML and `pageContent` in one response; HTTP mode requests `httpResponseBody` and sets `pageContentOptions.extractFrom=httpResponseBody`; browser mode requests `browserHtml` and sets `pageContentOptions.extractFrom=browserHtml`. |
| `bright_data` | required `api_key` and `zone`; `output` one of `html`, `markdown`, default `markdown`; optional ISO country `country`; `async` must be absent or `false`. |
| `startup_http` | Requires injected `HTTPClient` and `trusted: true`; no other service options. Its raw string response is normalized by Section 6.2 and is permitted only in tests or explicitly trusted deployments. |

Materialized common defaults are exact:

| Type | `timeout` | `connect_timeout` | `transport_retries` |
| --- | --- | --- | --- |
| `httpx` | `30s` | `10s` | `1` |
| `curl_cffi` | `30s` | `10s` | `1` |
| `trafilatura` provider profile | `10s` | `10s` | `1` |
| `crawl4ai` | `120s` | `10s` | `0` |
| `firecrawl` | `60s` | `10s` | `0` |
| `jina_reader` | `30s` | `10s` | `1` |
| `zyte` | `60s` | `10s` | `0` |
| `bright_data` | `60s` | `10s` | `0` |
| `startup_http` | `30s` | `10s` | `0` |

Every type also materializes `max_body_bytes=10485760`, `concurrency=8`, and
`cooldown=60s` unless explicitly authored; `startup_http` defaults to
`concurrency=1` because the injected protocol does not promise thread safety. The synthetic RSS direct provider
is named `__rss_direct__`, has type `httpx`, and uses the exact HTTPX row plus
those common defaults, with empty headers/cookies/allowlists and no proxy. Its
fully materialized representation is stored in `WebBinding.direct_provider`.

Endpoint rules are exact: `httpx`, `curl_cffi`, the Trafilatura profile, and
`startup_http` forbid `base_url`; Crawl4AI requires an authored `base_url`;
Firecrawl defaults to `https://api.firecrawl.dev`; Jina Reader defaults to
`https://r.jina.ai`; Zyte defaults to `https://api.zyte.com`; Bright Data
defaults to `https://api.brightdata.com`. A custom `base_url` replaces the
default for every remote type that supports it. The canonical base URL omits
the trailing slash, while each adapter appends its versioned path. Both an
authored value and a materialized default are stored identically in retained
bindings.

Action entries reject unknown keys. Because vendor action models are not
isomorphic, validation is provider-specific rather than pretending one action
object is portable. Firecrawl accepts: `wait {milliseconds}`,
`click {selector}`, `write {text}`, `press {key}`, and
`scroll {direction: up|down}`. Zyte accepts: `wait {milliseconds}` mapped to
`waitForTimeout`, `click {selector}` mapped to a CSS `ActionSelector`,
`write {selector,text}` mapped to `type`, `press {key}` mapped to `keyPress`,
and `scroll {x,y}` mapped to `scrollTo`; `x` and `y` are non-negative
integers. Unsupported action kinds or shapes are startup errors. A vendor
feature outside these subsets requires a design change rather than an
unvalidated escape hatch.

### 4.3 Task configuration

The normal content path is:

```yaml
stages:
  article_markdown:
    input: each
    cache_version: 1
    tasks:
      - use: sluice.tasks.fetch_webpage
        with:
          fetch_chain: robust
          extract_chain: article
          min_chars: 500
      - use: sluice.tasks.to_markdown
```

For text output, the final Task is `sluice.tasks.to_text`.

`extract_content` remains available for raw HTML or a pre-existing artifact:

```yaml
tasks:
  - use: local.tasks.produce_html
  - use: sluice.tasks.extract_content
    with:
      extract_chain: article
      min_chars: 500
  - use: sluice.tasks.to_markdown
```

### 4.4 Source configuration

A webpage Source may name a fetch chain. List selectors require a provider
result with raw HTML capability; Markdown-only providers are skipped for that
request.

```yaml
sources:
  releases:
    use: sluice.sources.webpage
    with:
      url: https://example.com/releases
      list_selector: article.release
      fetch_chain: robust
```

A single-page Source that materializes article content also names its extract
chain:

```yaml
sources:
  about:
    use: sluice.sources.webpage
    with:
      url: https://example.com/about
      fetch_chain: robust
      extract_chain: article
      output: markdown
```

RSS may define a raw fallback chain. The normal direct request remains first;
fallback runs only after a direct failure or unusable feed response. Only
providers capable of returning raw feed text are eligible.

```yaml
sources:
  feed:
    use: sluice.sources.rss
    with:
      url: https://example.com/feed.xml
      raw_fallback_chain: raw_recovery
```

The expanded chain configuration participates in the Source binding digest.
Changing its provider order, endpoint, or result-affecting options creates a
new Source binding and the ordinary binding-switch transaction clears old
edge contribution before applying revision zero of the new binding.

### 4.5 Built-in Task and Source fields

Unknown fields are rejected. Quality fields have the Section 9 defaults.

| Built-in | Accepted `with` fields |
| --- | --- |
| `fetch_webpage` | required `fetch_chain`, required `extract_chain`, optional non-empty fixed `url`, `url_field` default `url`, `fallback_url_field` default `link`, `min_chars`, optional positive `min_words`, optional `max_link_ratio` in `[0,1]` |
| `extract_content` | required `extract_chain`, `input_format` one of `auto`, `html`, `text`, `markdown` default `auto`, `min_chars`, optional positive `min_words`, optional `max_link_ratio` in `[0,1]` |
| `to_markdown` | `input_format` one of `auto`, `html`, `text`, `markdown` default `auto` |
| `to_text` | `input_format` one of `auto`, `html`, `text`, `markdown` default `auto` |
| webpage Source | required non-empty `url` and `fetch_chain`; optional non-empty `list_selector` with no extract/output/quality fields, otherwise required `extract_chain` and `output: markdown|text` plus optional quality fields; arbitrary JSON `next_checkpoint`; positive `web_cache_version` default `1` |
| RSS Source | required non-empty `url`; optional `raw_fallback_chain`; `entry_output: text|markdown` default `text`; optional non-empty `cursor_param` or deprecated identical `checkpoint_param` alias, but not both; arbitrary JSON `next_checkpoint`; positive `web_cache_version` default `1` |

For `fetch_webpage`, a configured fixed URL wins; otherwise a string input is
the URL; otherwise the Item field named by `url_field` is used, followed by
`fallback_url_field`. Empty or non-string values are execution errors.
`html`, `xml`, and literal body `content` are not valid authored Source fields;
the loader rejects them so raw documents cannot enter persisted Source config.

### 4.6 Extractor-specific fields

Extractor defaults are materialized in `WebBinding`. Unknown fields are
rejected.

| Type | Accepted fields and defaults |
| --- | --- |
| `identity` | No fields. It must be the first extractor exactly once. |
| `trafilatura` | `include_tables: true`, `include_comments: false`, `favor_precision: false`, `favor_recall: false` |
| `newspaper4k` | optional non-empty BCP-47 `language`; absent means library auto-detection |
| `resiliparse` | `main_content: true`, `preserve_formatting: false` |
| `beautifulsoup` | `mode: article|plain` default `article`; optional non-empty CSS `content_selector`; `remove_selectors` ordered unique CSS string list default empty |

An extract chain contains `identity` first and exactly once. Other extractor
types may appear at most once in one chain. This prevents ambiguous repeated
candidate scoring while allowing different named configurations in different
chains.

## 5. Configuration Expansion and Secrets

Named provider and chain references are authoring conveniences. During
definition loading, every Source and Stage receives a canonical expanded
snapshot containing:

- ordered provider instance names;
- provider types and non-secret result-affecting options;
- ordered extractor names, types, and options;
- quality and projector options;
- secret references such as `env:FIRECRAWL_API_KEY`, never resolved values.

`Definition` gains a frozen `web` authoring model. `TaskDef` and `SourceDef`
gain an optional frozen `web_binding` separate from their user-authored
`with_config`. A web binding has this exact canonical shape:

```text
WebBinding
  version               built-in web behavior version, initially 1
  direct_provider       optional synthetic RSS direct provider
  fetch_chain           optional for extract/project-only operations
    name
    providers[]         ordered fully expanded provider instances
  extract_chain         absent for raw-only requests
    name
    extractors[]        ordered fully expanded extractor instances
  quality               all defaults materialized
  output                artifact, markdown, text, raw_html, or raw_source
```

An RSS binding contains a synthetic `direct_provider` with the fixed core
HTTPX defaults and stores `raw_fallback_chain` as its optional `fetch_chain`.
The injected legacy `HTTPClient` may replace that direct transport in tests,
but does not alter semantic binding identity. An extract-only Task has no
`direct_provider` and no `fetch_chain`. A projector-only Task may have only
`output`. Missing sections are encoded as JSON null rather than omitted, so
snapshot shape and digests are stable.

`output` is derived deterministically when it is not an authored field:

```text
fetch_webpage Task          artifact
extract_content Task       artifact
to_markdown Task           markdown
to_text Task               text
webpage list Source        raw_html internally, then list SourceRecords
RSS Source                 raw_source internally, then RSS SourceRecords
single-page Source/helper  authored markdown or text
```

The internal raw outputs never become Items. Loader expansion writes the
derived value explicitly into `WebBinding`, so retained snapshots and keys do
not depend on re-inferring it from a later implementation.

Top-level providers and chains that are not referenced by a Source or Stage
are not copied into its binding. Provider and extractor mapping keys are
canonicalized in sorted order; chain member arrays retain declaration order.
Provider instance names are retained because they appear in provenance and
therefore affect Item metadata and identity.

For built-in web Sources and Tasks, the runtime receives a private immutable
invocation mapping containing the original user `with_config` plus its
`web_binding`. Custom Source and Task callables continue to receive exactly
their authored `with_config`; the private binding is never injected into a
custom callable. Stage snapshot encoding stores both fields explicitly, and
retained decoding reconstructs the same `TaskDef.web_binding`.

The new Stage snapshot shape remains the existing outer Stage object, but each
Task entry is exactly `{use, with, web_binding}`; `web_binding` is JSON null
for non-web Tasks. A Source execution's existing `config_json` remains its
authored `with_config`; the expanded Source `web_binding` is stored in the
binding snapshot used by `_binding_digest`, not passed as authored config.
The decoder accepts the pre-feature legacy Task shape `{use, with}` and
normalizes it to `web_binding: null`. Such a legacy terminal snapshot remains
readable, but an explicit rebuild of a legacy web Task that lacks an exact
binding fails with `DefinitionError`; it never borrows the current YAML chain.
Queued or running legacy web work is failed/superseded during startup recovery
rather than executed under guessed configuration.

The stored `web_binding` is the complete runtime binding. Key construction
derives a semantic projection that removes only resolved secret values; all
authored defaults and secret reference
names remain. `canonical_stage_config`, `stage_lookup_key`, and Source binding
digest use this semantic projection rather than hashing the full runtime
binding blindly. Snapshot round-trip validation asserts that both projections
are canonical and that decoding cannot change their digests.

The snapshot retains enough information to execute queued Stage work, Stage
retries, and rebuilds after the current YAML changes or removes the original
named chain.

`WebBinding.version` participates in every **web-bound** Stage key and built-in
web Source binding digest. A Stage with no built-in web Task receives no
`WebBinding`, so unrelated filter/dedup/LLM caches do not change. Projector-only
`to_markdown` or `to_text` Stages are web-bound because projection semantics
affect their payload and metadata. The first implementation therefore causes
one intentional cache partition for existing projector Stages whose fixtures
are updated to the new explicit snapshot shape. The version is bumped whenever
built-in provider, extractor, quality, or projection semantics change in a
result-affecting way. Stage authors may also bump the existing Stage
`cache_version`. Built-in web Sources accept
`web_cache_version`, a positive integer defaulting to one, solely as an
operator-controlled Source binding partition.

Resolved secret values do not affect canonical identity. Rotating a target
credential, cookie, proxy, or another secret behind the same environment
reference therefore requires a Stage `cache_version` bump or Source
`web_cache_version` bump when the rotation should refresh content.
Secret resolution occurs only during `open_app`; the process must be closed
and reopened after the environment changes. A cache/binding version bump
without reopening continues to use the immutable in-memory credential and is
not a credential reload mechanism.

`open_app` scans the current definition, every queued/running Stage snapshot,
every waiter snapshot, and retained terminal Stage snapshots required for
rebuild. It validates provider and extractor types, dependencies, endpoints,
and options, then constructs an immutable runtime catalog keyed by canonical
configuration digest. Current and immediately executable bindings resolve
secret values only into process memory. A terminal historical snapshot keeps
unresolved secret references until a rebuild is requested; a missing rebuild
secret fails that action without preventing read-only application startup.

Source retry behavior remains binding-scoped. A queued or running Source
execution whose persisted binding digest differs from the current expanded
Source binding is superseded before any provider call. It is not executable
merely because its old chain snapshot exists. When the binding digest is
unchanged, the current Source definition supplies the same expanded binding
and exact persisted checkpoint/config retry proceeds normally. Old Source
secrets or dependencies therefore never block startup after a binding change.

No database row, Item, log entry, exception string, management view, or SDD
artifact may contain a resolved secret. Provider fingerprints include only a
sanitized endpoint identity and canonical non-secret options.

## 6. Contracts

### 6.1 Fetch request

```text
FetchRequest
  url                  normalized target URL
  mode                 article, list_html, or raw_source
  acceptable           non-empty any-of capability set drawn from raw_html,
                       text, markdown, raw_source
  preferred            ordered desired capability tuple; providers request
                       every supported preferred representation in one call
  list_selector        canonical CSS selector required only for list_html;
                       null for article and raw_source
  target_headers       optional explicitly allowed target headers
  quality              immutable QualityPolicy
  stage_deadline       optional monotonic absolute Stage deadline; null for a
                       Source request
```

Service headers and credentials are not part of `target_headers`.

### 6.2 Document artifact

`DocumentArtifact` is a frozen, slotted dataclass exported from
`sluice.web.contracts`. All mapping fields use the existing deep-frozen JSON
representation. It is transient: it may be an intermediate `NEW` value but is
never a persisted Item payload.

```text
DocumentArtifact
  source_url            normalized target URL or null for URL-less local input
  final_url             normalized target URL or null for URL-less local input
  status_code           target HTTP status integer or null when a remote
                        provider does not report target status
  content_type          normalized MIME or deterministic sniffed MIME
  raw_html             optional
  raw_source           optional immutable bytes for RSS/Atom parsing only
  text                 optional
  markdown             optional
  selected_format      null on an unqualified candidate; raw_html, text, or
                       markdown on a quality-passing article candidate
  title                optional
  metadata             bounded structured page metadata
  capabilities         raw_html, raw_source, text, markdown, rendered_js
  provider_instance
  provider_type
  extraction           identity or extractor identity/version
  extract_binding_digest null for raw list/feed artifacts; SHA-256 of the
                       canonical extract chain plus quality policy after a
                       selected article candidate is established
  quality              metrics, pass flag, and failure reason
  attempts             redacted ordered attempt summaries
```

Artifacts never contain live clients, callbacks, mutable registries, or
durable state handles. `raw_source` preserves the decompressed feed bytes
without HTML cleanup or Markdown conversion and implies only the
`raw_source` capability. It is accepted only by RSS Source parsing and is
never passed to a Stage projector. A legacy injected `HTTPClient` string is
encoded as UTF-8 for this compatibility path; native HTTP providers preserve
response bytes.

The coordinator returns an article Artifact only after setting
`selected_format` to the exact representation that passed quality. Other
fields may remain for provenance or projection support but never override the
selection implicitly. Raw list/feed artifacts do not become article winners
and keep `selected_format` null.

`extract_content` constructs a local candidate Artifact deterministically for
bare string or Item input. A valid Item payload URL supplies both source and
final URL; absence supplies null, while an authored invalid URL is an execution
error. Status is null, content type is selected by `input_format`/auto
sniffing, provider instance/type are null, attempts are empty, and extraction
starts as `input`. The candidate receives only the matching raw HTML, text, or
Markdown capability. The extractor chain must produce a quality-passing
candidate and set `selected_format` before the Task returns it.

The `startup_http` compatibility adapter maps a successful returned string to
`status_code=200` and `final_url=request.url`. It deterministically sniffs
`text/html` for an HTML document, RSS/Atom XML for a raw-source request, and
`text/plain` otherwise, then populates only the representation requested by
the caller. It has no peer-address evidence, so it is allowed only when the
named instance is explicitly configured `trusted: true`; that flag is valid
only for `startup_http`, participates in semantic identity, and is intended
for tests or operator-supplied transports. `open_app` emits one bounded startup
warning for every such instance. This flag is an explicit trust exception: it
does not claim Sluice performed DNS, redirect, peer, or body-stream enforcement,
and the injected adapter is responsible for those controls in a non-test
deployment.

### 6.3 Provider

```text
WebProvider.fetch(request) -> DocumentArtifact
```

A provider performs one logical fetch operation. It may perform a bounded poll
for a job created by that operation, but it may not start a second crawl merely
to obtain another format. Provider errors are classified into stable reasons
rather than leaked as raw vendor exceptions.

### 6.4 Extractor

```text
WebExtractor.extract(artifact) -> DocumentArtifact | no_match
```

Extractors are local, deterministic operations over an existing artifact.
They do not perform network access. `identity` accepts provider-supplied text
or Markdown. Other extractors may consume raw HTML and add main content,
title, author, publication time, OpenGraph data, or JSON-LD.
`no_match` is the exported module-level singleton `NO_MATCH`; it is neither
`None` nor an exception and cannot be confused with a valid empty extraction.

### 6.5 Coordinator

```text
WebContentClient.resolve(request, fetch_chain, extract_chain)
    -> DocumentArtifact
```

The coordinator is bound once at startup. It receives expanded immutable
configuration per invocation; a resolved Task object is not bound to one
Stage's chain because the same built-in Task path may appear in current and
retained Stages with different configurations.

`BoundaryAdapters` adds an optional `web_content_client` injection for tests
and controlled deployments. The existing `http_client` injection remains
supported through a built-in `startup_http` provider adapter so current fake
HTTP tests and custom direct transports do not need network access. Neither
injection creates an implicit chain: configuration must still reference the
corresponding named provider instance, and its sanitized options remain part
of the canonical snapshot.

Startup resolution changes `bind_builtin_source(path, http_client)` to
`bind_builtin_source(path, adapters)` so webpage and RSS Sources can use the
same immutable runtime catalog as Tasks. Built-in Source wrappers receive the
private pair `(with_config, web_binding)`; custom Source callables keep the
public `collect(with_config, checkpoint)` contract and never see private
bindings. Built-in Task wrappers follow the same rule. `open_app` constructs
or accepts exactly one `WebContentClient`, installs it in `BoundaryAdapters`,
and resolves both current and retained web bindings before opening execution.

This is a deliberate authoring-contract change. Existing tests and examples
whose Stage ends at `fetch_webpage`, or whose network webpage Source names no
chain, are updated to declare a named `startup_http` instance and a final
projector. “Existing tests remain green” means their behavioral assertions
continue under the new explicit configuration; it does not require the old
minimal YAML fixtures to remain valid. Direct calls to pure offline helpers
remain supported only where explicitly described for literal Source HTML.

### 6.6 Built-in value kinds

The loader uses one fixed static table for built-in web Tasks:

```text
fetch_webpage     accepts Item or string URL       produces artifact
extract_content  accepts Item, string, artifact   produces artifact
to_markdown      accepts Item, string, artifact   produces json
to_text          accepts Item, string, artifact   produces json
```

This is not a public general Task type system. It exists only to reject a
built-in chain that would finish with a non-JSON Artifact. A Stage may contain
at most one `fetch_webpage`. `extract_content` after `fetch_webpage` is legal:
if the artifact already records a quality-passing extraction under the same
`extract_binding_digest`, it returns the artifact unchanged; otherwise it
continues only the declared local extractor chain. Before a different chain is
run, the Task clears `selected_format`, `quality`,
`extract_binding_digest`, and any field produced by the prior non-identity
extractor while retaining fetch-time `raw_html` and provider-original text or
Markdown. `identity` may reuse only those provider-original representations;
it may not re-accept output created by the previous extractor chain.
`to_markdown` and `to_text` perform the sole final metadata projection.

### 6.7 Exact fallback algorithm

`FetchRequest.mode` selects one of three closed resolution modes:

| Mode | Required winner representation | Extract chain | `identity` |
| --- | --- | --- | --- |
| `article` | any requested `raw_html`, `text`, or `markdown` representation that yields a Section 9 quality pass | required | allowed only for acceptable provider-original text/Markdown |
| `list_html` | valid non-challenge `raw_html`; selector semantics decide matched vs valid-empty | forbidden | forbidden |
| `raw_source` | `raw_source` bytes that pass Section 13.2 feed validation | absent | forbidden |

`acceptable` constrains both provider eligibility and the representation that
may win. An adapter may return extra representations for provenance or local
recovery, but an extra Markdown field cannot win a `list_html` or `raw_source`
request.

For one Item, `WebContentClient.resolve` executes this common prefix:

1. Iterate provider instances in declared order.
2. Intersect `acceptable` with the provider's static or conditional
   capabilities. If empty, record `missing_capability` without a call.
3. Acquire the instance limiter within the effective deadline, check the Stage
   attempt cancellation probe, and perform one logical provider operation.
4. Reject bad status/MIME, oversized bodies, challenges, and empty shells
   before mode-specific winner selection.

In `article` mode, provider-original acceptable Markdown/text is offered to
`identity` first. If it fails quality and raw HTML is acceptable and present,
the remaining extractors run in declared order. Each `NO_MATCH`, sanitized
extractor error, or failed candidate advances to the next extractor; exhausting
the chain advances to the next provider. The first quality-passing candidate
wins.

In `list_html` mode, no extractor runs. The Source applies its declared CSS
selector to valid raw HTML. The loader validates and canonicalizes that
selector into `FetchRequest.list_selector`; it is an error in either other
mode or when absent in `list_html`. During the provider loop the coordinator
calls the same pure `select_list_records(raw_html, final_url, selector)` helper
used by the Source's final record projection. A page whose helper result is
non-empty wins immediately. A valid page with an empty result is retained as
an empty candidate while later raw-HTML providers run; after exhaustion, the
earliest valid empty candidate becomes a successful empty publication. The
coordinator returns the winning/candidate Artifact, and the Source invokes the
same helper to construct records; it does not make a selection decision only
after resolution. A challenge, invalid page, or selector error is never an
empty candidate.

In `raw_source` mode, no extractor runs. A provider sets `raw_source` only for
an exact response representation that passes the bounded `defusedxml` feed
validation in Section 13.2. The first valid feed, including a valid zero-entry
feed, wins; other returned Markdown, HTML, or text cannot win.

Exhausting every eligible article provider raises `FetchChainExhausted`.
Exhausting list providers returns the earliest valid-empty candidate when one
exists, otherwise raises Source exhaustion. Exhausting raw-source providers
raises ordinary Source exhaustion. All exception payloads contain only
redacted ordered attempt summaries.

Quality is evaluated within each provider attempt, and providers are never
fetched speculatively. A provider-supplied short Markdown value does not
discard acceptable raw HTML from the same article response; local extractors
may still recover a passing candidate from that HTML.

The historical `type: trafilatura` composite performs HTTPX transport and one
offline Trafilatura extraction inside the provider, returning both raw HTML and
provider-original Markdown. It never replaces or reorders the declared
extract chain; that chain's `identity` may accept the Markdown. Within one Item
attempt, an equivalent preceding HTTPX raw response may be reused by normalized
URL and sanitized transport digest rather than requested twice. This
attempt-local reuse is discarded when the Stage attempt ends and is not a
content cache.

## 7. Provider Semantics

Provider service status and target-page status are distinct. An adapter first
validates the service API response; a non-success service status produces a
sanitized provider attempt failure and no Artifact. `DocumentArtifact.status_code`
is the inner target status when the service reports it. Direct providers
always report target status. A remote provider that returns usable content but
omits target status sets it to null; quality then relies on representation,
challenge, and content checks rather than pretending the service's 200 is the
target's 200.

For provider fetches, `DocumentArtifact.final_url` is always present. Direct providers use the last
validated target redirect URL. Remote providers use a normalized reported
target final URL when present; if omitted, they use the normalized request
URL. An invalid, unsafe, or non-HTTP(S) reported target URL fails the provider
attempt rather than falling back silently. Service API redirect URLs are never
used as target final URLs.

Static capability declarations used for pre-call eligibility are:

| Type | Capabilities |
| --- | --- |
| `httpx` | `raw_html`, `text`, `raw_source` |
| `curl_cffi` | `raw_html`, `text`, `raw_source` |
| `trafilatura` provider profile | `raw_html`, `text`, `markdown` |
| `crawl4ai` | `raw_html`, `text`, `markdown`, `rendered_js`, conditional `raw_source` |
| `firecrawl` | `raw_html`, `markdown`, `rendered_js`, conditional `raw_source` |
| `jina_reader` | `markdown` |
| `zyte` | `raw_html`, `text`, `rendered_js` |
| `bright_data` | configured `raw_html` or `markdown`, plus `rendered_js` |
| `startup_http` | request-selected `raw_html`, `text`, or `raw_source` |

A static declaration only establishes eligibility. The returned artifact must
still contain at least one member of the request's `acceptable` set and pass
validation; otherwise the attempt records `missing_capability` or its more
specific failure reason and falls through. Article resolution accepts any of
raw HTML, text, or Markdown and prefers Markdown plus raw HTML in one call;
list-page resolution accepts only raw HTML; RSS fallback accepts only raw
source.

`raw_source` is unconditional only for direct byte-preserving transports.
Crawl4AI and Firecrawl are statically eligible for a raw-source request, but
set that capability only when the exact same operation returns a body or
`rawHtml` value that passes the feed validation in Section 13.2. A normalized
DOM serialization, extracted article HTML, or Markdown never becomes
`raw_source`; a response lacking qualifying bytes records
`missing_capability` and advances the chain.

### 7.1 HTTPX

HTTPX is a core dependency and the standards-compliant baseline transport. A
shared synchronous `httpx.Client` provides connection pooling and is reused
across Stage worker threads. Redirects are followed manually under the SSRF
rules in Section 11.

The provider returns raw HTML, plain text, or raw source based on MIME and the
requested capability. It does not claim JavaScript-rendered capability.

### 7.2 curl_cffi

`curl_cffi` is the low-cost browser TLS/HTTP fingerprint provider. It supports
a declared impersonation profile, proxy, target headers, cookies, and the same
response and SSRF limits as HTTPX. It does not execute JavaScript and is not
considered a browser renderer.

### 7.3 Trafilatura compatibility profile

Historical `type: trafilatura` configuration remains accepted as a composite
profile. Its provider implementation performs one HTTPX fetch and one local
call to `trafilatura.extract()` over that fetched body, then exposes the raw
HTML and resulting Markdown as provider-original representations. It does not
insert, replace, or reorder an authored `extract_chain`, and does not cause an
additional request after an equivalent raw HTTP artifact already exists in
the same Item attempt.

Trafilatura extraction uses Markdown output, excludes comments, and includes
tables. Failure to derive useful content is `no_match`, allowing the next
extractor or fetch provider.

### 7.4 Crawl4AI

The initial adapter targets the current self-hosted synchronous API. It sends
one `POST /crawl` request with the typed browser/crawler configuration and
`cache_mode: bypass`, then captures raw HTML and Markdown returned by that
response. A configured API key becomes a Bearer service header; deployments
without API authentication may omit it.

Crawl4AI's asynchronous `POST /crawl/job` plus `GET /job/{task_id}` flow is
outside this design. Supporting it later requires durable provider-job state;
the adapter must not silently poll historical `/task/{id}` or `/jobs/{id}`
paths. Arbitrary named local instances and an authored `base_url` remain
supported.

### 7.5 Firecrawl

The initial Firecrawl adapter supports only the v2 scrape contract at
`POST /v2/scrape`, on either the official cloud endpoint or a self-hosted
instance that exposes the same contract. Firecrawl v1 request fields and
actions are not silently translated; authoring `api_version: v1` is a startup
error. The adapter supports service authentication, wait selectors, and only
the validated browser actions in Section 4.2.

For an article request it asks for `markdown` and `rawHtml` in one scrape. For
list-page and raw-source requests it asks only for `rawHtml`; cleaned `html`
is not treated as DOM-preserving input. Every request sets `maxAge: 0`,
`storeInCache: false`, and `skipTlsVerification: false`, and defaults
`onlyMainContent` to false. Firecrawl-native actions are translated only from
the validated subset in Section 4.2.

A self-hosted deployment is not assumed to provide cloud Fire-engine anti-bot
behavior. Its returned artifact must pass the same quality and capability
checks, and a `rawHtml` value counts as raw feed source only if Section 13.2
accepts it.

### 7.6 Jina Reader

Jina Reader supplies Markdown and optional reader metadata. It is a reader
fallback, not a guaranteed anti-bot provider, and normally appears after
browser or unlocker providers. It is ineligible for requests requiring raw
HTML or raw feed content.

### 7.7 Zyte

The Zyte adapter sends one Basic-authenticated `POST /v1/extract`. Auto mode
requests `browserHtml` and, when `automatic_content` is true, `pageContent`;
HTTP mode requests `httpResponseBody` and sets
`pageContentOptions.extractFrom=httpResponseBody`; browser mode requests
`browserHtml` and sets `pageContentOptions.extractFrom=browserHtml`. The
validated action subset maps to Zyte `click`, `type`, `keyPress`, `scrollTo`,
and `waitForTimeout` actions with CSS selectors encoded as
`{type: css, value: <selector>}`. Returned target status/final URL, HTML, page
content, title, and bounded structured fields are normalized into one
artifact.

### 7.8 Bright Data Web Unlocker

The Bright Data adapter uses Web Unlocker, not the interactive Browser API.
It sends a Bearer-authenticated request to the configured zone with
`format: raw`; configured Markdown output additionally sends
`data_format: markdown`, while HTML output omits `data_format`. It supports
zone selection, country selection, and provider concurrency limits. Service
envelopes are removed by the adapter and never mistaken for target content.

Only the synchronous request path is in scope. Async response IDs, callbacks,
and multi-hour polling require new durable provider-job state and are excluded
from this design.

## 8. Extractor and Projector Semantics

The initial extractor chain supports:

1. `identity`: accepts quality-passing provider-supplied Markdown or text;
2. `trafilatura`: general article extraction with tables;
3. `newspaper4k`: article-oriented text and publication metadata;
4. `resiliparse`: fast rule-based main-content and boilerplate removal;
5. `beautifulsoup`: deterministic visible-content fallback.

Extractor outputs are fixed and never inferred by the projector:

| Extractor | Output field | `selected_format` | Network rule |
| --- | --- | --- | --- |
| `identity` | the acceptable provider-original `markdown` or `text` field | matching field | no call |
| `trafilatura` | sanitized Markdown | `markdown` | call `trafilatura.extract()` on supplied HTML only; never `fetch_url` |
| `newspaper4k` | normalized article text plus bounded metadata | `text` | supply HTML with `set_html`/equivalent and force `fetch_images=false`; never `download` |
| `resiliparse` | normalized main-content text | `text` | local extraction only |
| `beautifulsoup` | normalized visible text | `text` | local parsing only |

Any attempted network access by an extractor is an invariant violation mapped
to `extract_failed`; it is not recorded as a provider attempt. A projector uses
only `selected_format` and the corresponding field and does not guess from
other populated fields.

BeautifulSoup is a core dependency and replaces the hand-written HTML parser
as the final supported fallback. It removes script, style, template, noscript,
SVG, and non-content nodes, preserves block boundaries, decodes entities, and
normalizes whitespace. Article mode prefers `article` and `main`, then scored
content containers; plain mode retains navigation and ordinary visible text.

`html-to-markdown` is the Markdown projector. It is not a main-content
extractor and does not compete in the extractor chain. It converts the chosen
main HTML when no provider supplied Markdown and also extracts bounded title,
links, OpenGraph, Twitter card, and JSON-LD metadata.

All Markdown paths enforce `allow_raw_html=false`. The converter unwraps
unsupported ordinary elements to their text and drops ignored/dangerous
elements rather than emitting HTML. Provider-supplied Markdown and converted
RSS entry Markdown pass through the same Markdown-AST sanitizer, which removes
raw HTML block/inline nodes while preserving normal Markdown links and
autolinks. A sanitizer failure rejects that candidate; raw HTML is never
accepted merely because it is embedded inside a Markdown string.

The sanitizer uses `markdown-it-py` 4.2.x with HTML tokenization enabled for
inspection but rendering disabled. It removes `html_block` and `html_inline`
tokens before canonical Markdown rendering. Provider Markdown is sanitized
before identity quality/challenge evaluation and before metadata extraction;
converted HTML and RSS Markdown are sanitized before their quality check.
Unsanitized text never determines a winning candidate.

Markdown links and autolinks allow only normalized HTTP(S), `mailto`, and
same-document fragment destinations. Relative HTTP links are resolved against
Artifact `final_url`, or for a direct Item against its normalized payload
`url` and then `link`; a bare string has no base. Without a base they are
unwrapped to visible link text.
`javascript`, `data`, file, and unknown schemes are unwrapped to visible text.
Images allow only normalized HTTP(S) sources; unsafe images become their alt
text. This sanitizer runs before any sink renderer, which must still perform
its own channel-specific escaping.

`to_markdown` and `to_text` are the only built-in web Artifact projectors.
They return a JSON string payload plus `NEW.metadata.web`.

For an Item input, a projector accepts either a string payload or an object
payload whose `content` field is a string. Other Item payload shapes are an
execution error. It preserves the Item's existing metadata, then
deterministically adds or replaces the `web` subobject from an Artifact or
from available Item `url`, `title`, and content fields. For a bare string,
existing metadata is empty. This keeps existing one-Task `to_markdown` Stages
over Source Items valid while giving Artifact paths the richer provenance
contract.

Projectors accept `input_format: auto|html|text|markdown`, default `auto`.
Auto treats input as HTML only when BeautifulSoup finds an actual start tag;
otherwise `to_markdown` preserves the string and `to_text` normalizes its
whitespace. Explicit Markdown is identity for `to_markdown` and plain text for
`to_text`; explicit HTML uses the corresponding converter.

Every listed scalar key in `metadata.web` is present. For a non-Artifact Item
or bare string, unavailable provenance is JSON null: `source_url` and
`final_url` use a string Item payload `url` when present, title uses Item
payload title when present, provider instance/type are null, extractor is
`html_to_markdown`, `text_normalizer`, or `identity` according to the selected
path, and quality character/word counts are computed from the final payload.
`output_format` is always `markdown` or `text`; collection fields are empty
objects/lists rather than null. Required scalar keys are therefore never
omitted even when their values are null.

`input_format` applies to bare strings and Item content. For an Artifact it
must remain `auto`; the projector uses `selected_format`, and missing selection
is an execution error. `to_markdown` sanitizes selected Markdown, converts
selected raw HTML, and escapes Markdown control characters in selected plain
text so it remains literal. `to_text` normalizes selected text, extracts
visible text from selected raw HTML, and renders selected sanitized Markdown
AST to visible text without link destinations or formatting markers.

```text
metadata.web
  source_url
  final_url
  title
  provider_instance
  provider_type
  extractor
  output_format
  quality_chars
  quality_words
  open_graph          bounded
  twitter_card        bounded
  json_ld             bounded
  links               bounded ordered list of {text, url}
  authors             bounded
  published_at
  warnings            bounded
```

`open_graph` and `twitter_card` are mappings and default to `{}`. `json_ld`,
`links`, `authors`, and `warnings` are ordered lists and default to `[]`.
Unavailable scalar provenance values are JSON null as defined above.
`published_at` is either JSON null or an RFC 3339 UTC string with `Z` suffix.
`authors` contains whitespace-normalized non-empty strings, deduplicated by
exact normalized value while retaining first-seen order.

Raw HTML, response headers, cookies, screenshots, challenge bodies, provider
credentials, and complete attempt error bodies are never materialized.

Canonical `metadata.web` is limited to 65536 UTF-8 bytes. Each string is
limited to 8192 Unicode code points, each list to 100 entries, and warnings to
20 stable codes. If the total remains too large after per-field limits, the
projector deterministically omits fields in this order: `json_ld`,
`open_graph`, `twitter_card`, `links`, then optional authors; it adds
`metadata_truncated` when space permits. Required scalar provenance fields are
never omitted.

Metadata is also structurally sanitized before bounding. OpenGraph retains
only title, type, URL, plain-text description, site name, locale, and image
URLs. Twitter card metadata retains only card, title, plain-text description,
site, creator, and normalized HTTP(S) image URLs. `links` preserves encounter
order, deduplicates by normalized destination, and contains only sanitizer-
approved destinations plus plain visible text. JSON-LD retains only `@type`,
headline/name, plain-text description,
author names, publication/modification dates, canonical URL, and image URLs;
fields such as `articleBody`, embedded HTML, scripts, arbitrary extension
objects, and unknown keys are discarded. Every retained descriptive string is
HTML-parsed to visible text and entity-decoded; URL fields must pass URL
normalization. Link collections contain normalized URLs only. Thus bounded
metadata never becomes a side channel for raw page markup.

The initial closed warning-code set is `metadata_truncated`,
`sanitizer_removed_html`, `sanitizer_rejected_link`,
`sanitizer_rejected_image`, `provider_metadata_ignored`, and
`publication_date_invalid`. Adding or changing a warning that affects
persisted metadata requires a `WebBinding.version` bump.

The loader rejects a built-in Stage whose final value kind is Artifact. A
Stage containing `fetch_webpage` or Artifact-producing `extract_content` must
end with the built-in `to_markdown` or `to_text`. Custom Artifact projectors
and a general Task value-kind protocol are outside this design.

## 9. Quality Policy

Quality is evaluated against normalized visible content, never raw markup
length. The default policy is:

```text
min_chars             500
min_words             disabled
max_link_ratio        disabled
reject_challenges     true
max_body_bytes        10 MiB
```

For HTML quality, Sluice parses with BeautifulSoup, removes non-visible
script/style/template/noscript/SVG content, decodes entities, converts NBSP to
space, preserves one newline at block boundaries, collapses other Unicode
whitespace, and strips the result. `quality_chars` is the number of Unicode
code points in that normalized string; `quality_words` is the count of
Unicode alphanumeric runs. Link ratio is normalized visible characters inside
anchors divided by total normalized visible characters, with zero for empty
content.

Direct target responses accept status 200-299 except 204. Raw HTML accepts
`text/html` and `application/xhtml+xml`; plain text accepts `text/plain`; raw
feed parsing accepts the types in Section 13. Provider service JSON is parsed
only by that provider adapter and its inner artifact must declare a target
representation. A missing or malformed MIME may proceed only when deterministic
content sniffing recognizes HTML or XML; other binary types fail.

Challenge detection is versioned with `WebBinding.version`. Version 1 rejects
the closed selector/path set `#challenge-running`, `#challenge-stage`,
`.cf-challenge-running`, `.cf-turnstile`, `.g-recaptcha`, `.h-captcha`,
`iframe[src*="challenges.cloudflare.com"]`, and script URLs containing
`/cdn-cgi/challenge-platform/`; a `form` containing
`input[type="password"]` is a login wall only when no qualifying article
content exists. It also rejects the case-folded, whitespace-normalized phrase
set `verify you are human`, `checking your browser`, `access denied`, and
`enable javascript and cookies to continue`.
An empty shell is raw HTML with fewer than 100 normalized visible characters
and either a root application container with script resources or no content
element containing at least 80 characters. These checks run before ordinary
`min_chars`; a chain may lower `min_chars` for legitimate short pages but may
not accidentally accept a recognized challenge.

Each Task or Source may override `min_chars`, enable `min_words`, or enable a
maximum link ratio. Challenge rejection, MIME validation, and response-size
limits cannot be disabled. A provider instance may lower its positive body
limit but may not make it unlimited. The effective decompressed response limit
is `min(provider.max_body_bytes, 10485760)` bytes for every representation,
including service-envelope decoding; no provider option can raise the fixed
10 MiB ceiling.

Stable failure reasons include:

- `bad_url`
- `ssrf_blocked`
- `timeout`
- `network`
- `rate_limited`
- `provider_busy`
- `http_status`
- `bad_content_type`
- `body_too_large`
- `unsafe_xml`
- `challenge`
- `empty_shell`
- `empty`
- `too_short`
- `extract_failed`
- `missing_capability`
- `provider_error`

For RSS, `unsafe_xml` from the direct response activates the declared raw
fallback chain. The same reason from a fallback candidate rejects only that
candidate and advances to the next eligible provider; exhaustion then fails
the Source attempt under normal Source retry policy.

The coordinator records the provider instance, elapsed time, retry count,
reason, and sanitized status for each attempt. It does not expose raw vendor
responses in ordinary logs.

`WebAttemptFailure` is a frozen internal record containing only provider
instance, provider type, attempt ordinal, stable reason, optional numeric HTTP
status plus `status_kind=service|target`, elapsed milliseconds, and a bounded safe detail code. Vendor response
bodies and raw exception strings are never fields. `FetchChainExhausted`
contains an ordered tuple of these records and formats a bounded canonical
summary.

Before Stage or Source runtime writes `executions.error`, emits a log event, or
wraps an exception, `sanitize_web_error` maps known provider exceptions to
that stable representation. Unknown exceptions become `provider_error`
without their string value. The detailed original exception remains only as
an in-memory cause for local debugging and is never serialized.

An environment reference is exactly one complete scalar of the form
`env:[A-Z_][A-Z0-9_]*`; interpolation such as `prefix-${SECRET}` is rejected.
Resolution recursively covers declared service headers, target headers,
cookies, proxy, API key, zone, and other secret-capable fields. Redaction is
performed from the parsed schema rather than by searching strings after an
error has already been constructed.

## 10. Retry, Drop, and Recovery

One Stage attempt executes the complete ordered fetch/extract process once.
The provider layer may perform bounded transport retry only for transient
connect, read, 429, and selected 5xx failures.

Safe GET providers default to one transport retry. Paid or side-effect-like
POST scrape providers default to zero automatic resubmissions unless their
instance configuration explicitly enables it. `Retry-After` is honored
within the provider deadline.

If every eligible provider fails, `fetch_webpage` raises a typed
`FetchChainExhausted`. The Stage runtime follows the node's durable retry
policy and reuses the exact retained Stage configuration and Item envelope.
`retries: 2` means one initial attempt plus at most two queued retry attempts.

The terminal conversion is exact:

1. `FetchChainExhausted` is raised only by built-in `fetch_webpage`; throwing
   stops the Task chain, so a following projector is not invoked.
2. The rule applies only to an `each` Stage whose failing Task is its one
   built-in `fetch_webpage`, which must be the Stage's first
   Artifact-producing Task. After it, the Stage may contain at most one
   built-in `extract_content` and must end with exactly one built-in
   `to_markdown` or `to_text` projector. A terminal-drop-eligible Stage may not
   contain a custom Task after `fetch_webpage`; this keeps exhaustion scoped to
   one known Item fetch path while preserving the legal
   fetch/extract/projector composition from Section 6.6.
3. While `retry_count < retries`, the current attempt becomes `failed` with a
   sanitized exhaustion summary and the existing guarded `_fail_execution`
   path transfers the same materialization claim to one queued retry.
4. When `retry_count >= retries`, the still-current owner calls the guarded
   finalizer with `StageOutcome(outputs=(), status="succeeded")`. Its Execution
   becomes `succeeded`, `error` is JSON/SQL null, and an empty `succeeded`
   StageResult becomes `stage_result_current`.
5. This exception never uses the `InvalidStructuredOutput`/`invalid: drop`
   branch and never publishes a `dropped` StageResult.
6. Any other exception, `extract_content` exhaustion, batch Stage, custom Task,
   or invalid web configuration remains `failed` and publishes no StageResult.

The input Item is therefore dropped only after durable retry exhaustion and no
downstream member is admitted. Earlier attempts remain `failed`; a queued
retry becomes the terminal successful attempt only after it owns the same
materialization claim and exhausts the persisted retry count.

The empty StageResult is the current cache result for that lookup key. Shared
waiters attach the same empty result without calling providers. A forced
rebuild that exhausts its retries intentionally publishes a new empty current
sequence and shared-consumer healing withdraws the previously visible output.
This follows the approved retry-then-drop policy rather than preserving stale
content after an explicit refresh.

The guarded state/attempt-token update is atomic with checking retry
exhaustion, so recovery or timeout cannot both queue another retry and publish
the terminal empty result.

Timeout, startup recovery, stale generation, and late worker completion use
the existing Stage claim and attempt-token guards. A timed-out or superseded
worker may finish its network call but cannot publish an Artifact or
StageResult.

## 11. Concurrency and Provider Limits

Node `policy.concurrency` remains the only Item worker concurrency setting.
One worker owns one complete Item attempt.

Each provider instance may additionally configure a process-wide semaphore
and an in-memory cooldown. The semaphore caps simultaneous calls to that
instance across Pipelines without changing Item ordering. A 429 may place the
instance in cooldown until `Retry-After`; another provider in the same chain
may still be tried.

The runtime catalog keys a limiter and client by a full runtime digest that
includes provider type, endpoint, secret-reference identity, concurrency, and
cooldown configuration. Current and retained bindings with the same runtime
digest share one limiter; different operational settings receive distinct
limiters even when their semantic Stage-key projection is otherwise equal.

Semaphore acquisition consumes the provider's total deadline. Failure to
acquire records `provider_busy` and advances to the next provider without a
network call. An active cooldown records `rate_limited` and likewise advances
rather than sleeping one Item worker for the entire cooldown.

Provider semaphores and cooldowns are operational controls, not persisted
business state. Restart clears their live occupancy and timers. Their declared
configuration affects Stage keys and Source bindings because contention and
skip behavior can affect the winning provider.

Every provider operation has explicit connect, read, and total deadlines. For
a Stage Task, the absolute Stage attempt deadline is
`execution.started_at + policy.timeout`. Immediately before limiter
acquisition for each provider, the coordinator records monotonic
`provider_attempt_started`; the effective provider deadline is the earlier of
the Stage timestamp and
`provider_attempt_started + provider.timeout`. Limiter acquisition,
`Retry-After`, connect, read, body streaming, and any bounded provider
operation are clamped to the remaining effective duration, so limiter waiting
cannot reset or extend the provider budget.
For a Source, which has no Stage policy timeout, the provider's own deadline
is the effective deadline. If one provider's deadline expires while Stage time
remains, the attempt records `timeout` and may advance. Once the Stage deadline
expires, the coordinator raises internal `WebStageDeadlineExpired` and never
starts another provider or extractor.

The Stage timeout remains the durable publication watchdog and also supplies
that cooperative upper bound; it is not a mechanism for killing a blocked
native or Python thread. `_execute_claimed_stage` handles
`WebStageDeadlineExpired` through the same guarded timeout transition used by
the timeout scan. Whichever path first changes the attempt token wins, so the
other becomes a no-op and cannot queue a duplicate retry.

For built-in web Tasks, Stage runtime installs a private immutable
`WebExecutionProbe` in a context variable before invoking the Task chain. The
probe checks that the owning Execution is still `running` with the same
attempt token. The coordinator checks it before limiter acquisition, after
acquisition, immediately before every network send or poll, after every
response/extractor, and before advancing to another provider. A stale probe
raises internal `WebAttemptCancelled`, which is not a chain exhaustion and
cannot create a retry or terminal drop beyond the timeout/recovery decision
already recorded by Stage runtime. An in-flight synchronous request may still
return late, but the worker cannot issue the next paid fallback call or
publish its response.

`WebAttemptCancelled` subclasses `ExecutionError` so `_task_run` does not wrap
it. `_execute_claimed_stage` catches it before the generic failure path,
rechecks the durable Execution state and attempt token, and returns without
calling `_fail_execution` when ownership is already lost. If ownership is
unexpectedly still current, cancellation is an invariant violation and the
ordinary guarded failure path applies; it is never converted to
`FetchChainExhausted`.

The runtime catalog owns all provider clients. `App.close()` is synchronous
and idempotent; callers must not run it concurrently with `tick`, reconcile,
or rebuild. It first stops admission and joins Stage workers, then closes the
web runtime catalog and its HTTPX/other provider resources, and finally closes
SQLite. Shared clients are never created or destroyed per Item.

HTTPX-backed direct and vendor adapters share one documented thread-safe
`httpx.Client` per runtime digest. curl_cffi uses one session per worker thread
behind the shared instance limiter rather than sharing a mutable session.
Local extractor objects are immutable or constructed per call; no extractor
stores per-document mutable state globally. An injected `HTTPClient` is
serialized by the `startup_http` limiter unless the operator explicitly raises
that instance's concurrency and thereby accepts responsibility for its thread
safety.

## 12. Network and Secret Safety

Target URLs allow only HTTP and HTTPS. The default target policy rejects
loopback, private, link-local, multicast, reserved, carrier-grade NAT, IPv4-
mapped private, and cloud metadata addresses.

URL normalization rejects userinfo, invalid percent encoding, non-canonical
numeric hosts, and ports outside 1-65535. Internationalized hostnames are
converted to lowercase IDNA ASCII and trailing dots are removed before policy
evaluation. Host allowlist entries are exact normalized hostnames; CIDR
entries are parsed networks. No wildcard or substring matching is used.

Built-in direct providers (`httpx`, `curl_cffi`, and the Trafilatura profile):

- resolve every hostname and reject the target if any selected address is
  disallowed;
- reject a mixed DNS answer when even one returned address is disallowed;
- pin the approved address set for the actual connection (`CURLOPT_RESOLVE`
  for curl_cffi and an equivalent pinned HTTPX transport) while preserving
  the original Host header and TLS server name;
- follow redirects manually for at most ten hops;
- guard each redirect target;
- validate the final response URL and available peer address;
- count decompressed streaming bytes and stop at `max_body_bytes`, rather than
  checking only compressed bytes after unlimited buffering.

Every redirect is normalized, resolved, checked, and pinned independently.
DNS answers are not reused across redirect hosts. Literal IP targets follow
the same address policy and require a matching CIDR allowlist when non-public.

Provider service endpoints are operator-controlled and may use private
addresses for self-hosted Crawl4AI or Firecrawl. Service endpoint permission
does not grant the remote service permission to target arbitrary private
addresses. Target private access requires an explicit host or CIDR allowlist
in the provider instance.

Service API calls do not follow redirects. A 3xx from `base_url` is a
`provider_error`; this prevents Authorization or API-key forwarding to an
unexpected service host. `base_url` is normalized once at startup, rejects
userinfo, and must use HTTP(S). HTTPS is required for non-loopback cloud
service endpoints unless an explicitly private self-hosted endpoint is used.

Hosted and self-hosted remote-render providers fetch the target from another
network, so Sluice cannot observe or pin their intermediate target redirects
or peer addresses. Sluice still validates the submitted target URL and any
reported final URL. The operator explicitly trusts the provider's egress
policy; self-hosted services must enforce equivalent target restrictions at
their own network boundary. `target_allow_hosts` and `target_allow_cidrs` are
sent to a provider only when its API has an equivalent supported policy and
are never treated as proof that an opaque hosted service enforced them.

When `proxy` is configured, Sluice still performs local target validation. A
proxy mode that resolves target DNS remotely is rejected unless
`allow_remote_dns_proxy: true`; enabling it explicitly accepts that peer and
DNS pinning cannot be locally verified, while redirect URLs remain locally
validated before each subsequent request.

Service authentication, service headers, target headers, target cookies, and
proxy credentials are separate configuration fields. Service credentials are
never forwarded to the target site. Target credentials are opt-in and use
environment references.

Header names are normalized case-insensitively. User configuration may not
set `Host`, `:authority`, `Connection`, `Content-Length`, `Transfer-Encoding`,
`Proxy-Authorization`, `Upgrade`, `Forwarded`, `Via`, `Cookie`, or any
`X-Forwarded-*` header. The transport owns Host/authority, framing, connection,
proxy, and cookie headers so DNS pinning and virtual-host TLS cannot be
overridden. `User-Agent`, `Accept`, language, referer, and ordinary application
headers remain configurable. A provider instance may use either typed
`api_key` authentication or an explicit sensitive `Authorization` service
header, but not both.

Diagnostic URLs always remove userinfo and fragments and redact every query
value while retaining query keys for correlation. Persisted Item URLs retain
the normalized target query because it can identify distinct content. Such
URLs are input business data, not Sluice secret fields; operators must place
signed or credential-bearing target URLs in Source payloads only when that
persistence is acceptable, and should prefer declared target headers or
cookies for credentials.

The no-secret invariant applies to credentials Sluice resolves from declared
`env:` provider, proxy, header, cookie, zone, and authentication fields. The
runtime cannot infer whether an arbitrary pre-existing Item URL or payload
contains confidential business data and does not rewrite that input.

## 13. Source Integration

### 13.1 Webpage Source

The webpage Source uses `WebContentClient` when `fetch_chain` is configured.
A list selector requests raw HTML capability and parses the chosen final HTML
with BeautifulSoup/SoupSieve CSS selection. Providers without raw HTML
capability are skipped. `extract_chain` is forbidden together with
`list_selector` because main-content extraction would destroy the list DOM.

Matches are processed in document order. A match with `href` uses the
absolute URL resolved against the provider's `final_url` as both payload URL
and logical identity. Duplicate resolved URLs keep only the first match. A
match without `href` uses `<final_url>#match-<zero-based-index>` as logical
identity. Payload content is normalized visible text and title is that text
when non-empty. An empty match sequence is a valid empty Source publication.

Each list match produces exactly:

```text
SourceRecord.logical_id = resolved URL or final_url#match-index
SourceRecord.payload    = {url, content, optional title}
SourceRecord.metadata   = {web: bounded list-record projector metadata}
```

For each match, `metadata.web.source_url` is the requested list URL,
`final_url` is the winning page URL, provider instance/type come from that
artifact, `extractor` is `list_selector`, and `output_format` is `text`.
`quality_chars` and `quality_words` are computed from that record's normalized
content, not from the whole page. Article metadata collections are empty; page
title and sanitizer-approved bounded provenance may be retained under the same
Section 8 limits and pruning order.

The payload URL equals the logical ID for an `href` match and equals the page
`final_url` for a no-`href` match. Provider redirects never leave a relative
URL in an Item.

List-page quality is structural, not article length: the response must be
valid non-empty HTML within the body limit and the selector operation must
complete. SSRF, target status, MIME, body-size, and challenge checks run before
selection. `min_chars: 500` does not apply. A valid non-challenge page with no
matches is retained as an empty candidate while the coordinator tries later
raw-HTML providers, allowing a browser renderer to reveal JavaScript content.
If no provider yields matches but at least one valid empty candidate exists,
the final outcome is a successful empty Source publication; challenge pages
never qualify as empty candidates.

Extracted hrefs are URL-joined, normalized, and restricted to HTTP(S). URL
userinfo, invalid ports/encoding, localhost names, and disallowed literal IPs
are skipped with a redacted diagnostic. DNS is not resolved merely to persist
a discovered link; the complete DNS/peer policy runs if a later Task fetches
it. `javascript:`, `data:`, file, and other schemes never become
SourceRecords.

A single-page Source must name an `extract_chain` and `output: markdown` or
`output: text`. It projects a normal SourceRecord payload with final URL,
title, and extracted content. The built-in webpage Source no longer
materializes raw HTML. The no-raw persistence rule applies to built-in web
Tasks and Sources; unrelated custom Sources retain the ordinary JSON Source
contract. Source collection remains serialized; Stage Item concurrency does
not apply inside one Source collect.

The single-page record shape is exact:

```text
SourceRecord.logical_id = normalized final_url
SourceRecord.payload    = {url: final_url, content, optional title}
SourceRecord.metadata   = {web: the same bounded projector metadata}
```

`metadata.web` follows the 65536-byte and per-field limits from Section 8.
Source projectors apply the same deterministic pruning order as Stage
projectors.

For App/network collection, `fetch_chain` is required. The existing pure
Python helper that receives literal HTML remains available only as a direct
function call for deterministic unit tests; it is not accepted by
`load_definition`, creates no Source Execution or WebBinding, and never writes
SQLite. It follows the same list/single-page projection rules in memory.

Source binding activation, checkpoint CAS, atomic Item/publication creation,
revision-zero behavior, and binding-switch edge reset remain unchanged.

### 13.2 RSS Source

The pure Python RSS helper may receive literal XML only when called directly
outside `open_app`; `load_definition` rejects literal `xml`/`content`, so this
test/offline helper creates no Source Execution, WebBinding, or persisted raw
document.

The RSS Source first performs its normal direct request. It uses the injected
legacy `HTTPClient` when one is supplied for compatibility tests; otherwise it
uses the core HTTPX direct transport with the same safety and body limits. The
direct transport is not repeated inside `raw_fallback_chain`. If the request fails,
returns an unusable MIME/body, or feed parsing rejects the response, it invokes
the declared `raw_fallback_chain` with `raw_source` capability.

HTTPX and curl_cffi satisfy raw feed capability from their byte-preserving
target responses. Crawl4AI and Firecrawl are conditional candidates only: the
same operation must expose a body/`rawHtml` representation that independently
passes the Section 13.2 feed validator before the adapter sets `raw_source`.
Jina Reader and providers that return only normalized article text, cleaned
DOM HTML, or Markdown are skipped.

The first valid feed document wins. If all fallback providers fail, the Source
attempt fails normally and follows Source retry policy; Source failures are
not converted into per-record drops.

Raw feed validation does not use article `min_chars`. A body is usable when it
is within the byte limit and the feed parser recognizes an RSS or Atom root,
even when it contains zero entries. MIME is advisory: XML/feed MIME is
accepted immediately for parsing, while text or generic MIME is accepted only
when parsing recognizes the feed. HTML, Markdown-only, or rewritten article
content is never accepted as raw feed input.

Accepted roots are local name `rss` for RSS 2.x, local name `feed` for Atom,
or RDF namespace root `{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF` for
RSS 1.0. An RSS root must contain a channel element; Atom and RSS 1.0 may be
valid with zero entries/items. Other XML roots are rejected even when they
contain nested elements named `item` or `entry`.

RSS parsing uses `defusedxml` over the already bounded in-memory byte body.
Any DTD, entity declaration/reference, external entity, XInclude, or parser
limit violation rejects the document with stable reason `unsafe_xml`; the
parser performs no network or filesystem resolution. This applies to both the
direct response and every raw fallback candidate before root recognition.

Entries retain document order. Logical identity is the first non-empty value
of GUID/Atom ID, absolute link resolved against the final feed URL, title, or
`entry-<zero-based-index>`. Duplicate logical IDs keep only the first entry.
Each record has this exact shape, omitting optional payload keys whose source
value is absent:

```text
SourceRecord.logical_id = guid/id, resolved link, title, or entry-index
SourceRecord.payload    = {optional title, link, content, published, updated}
SourceRecord.metadata   = {web: bounded feed fetch provenance}
```

Embedded entry description/content is never stored as raw HTML. HTML-looking
entry content is converted with the deterministic BeautifulSoup visible-text
path when `entry_output: text`, or with `html-to-markdown` when
`entry_output: markdown`; ordinary text is normalized or preserved as
Markdown respectively. Feed fetch provenance uses provider instance
`__rss_direct__` for the direct path or the winning fallback instance,
extractor `feed_parser`, final feed URL, output format, and computed content
quality. Raw feed bytes are discarded after all SourceRecords are validated.

RSS entry links use the same static normalization as webpage hrefs: only
HTTP(S), no userinfo, valid encoding/port, no localhost name, and no
disallowed literal IP. An invalid link is treated as absent for both payload
and logical-ID fallback; GUID/ID, title, or entry index may still identify the
record. DNS/peer validation waits until a later network fetch.

## 14. Startup Resolution, Retention, and Rebuild

Provider and extractor implementations are resolved from fixed built-in maps
at startup. No provider is imported by arbitrary configuration type, Python
package entry point, or runtime plugin discovery.

Current definitions and executable retained Stage snapshots are validated
before the database is made available for execution. Old Stage snapshots do
not depend on the current named chain still existing in YAML because their
expanded sanitized configuration is complete. A retained Source execution is
executable only while its binding digest still equals the current Source
binding; otherwise startup recovery supersedes it without resolving its old
providers or secrets.

Stage retries and rebuilds use the exact retained provider order, endpoints,
quality options, extractor order, and projector options. Environment secret
references for queued/running work and waiters are resolved at startup. A
missing secret for a terminal historical snapshot fails only an explicit
rebuild of that snapshot; it never silently falls back to current
configuration.

Shared Stage materialization remains keyed by Stage name, canonical expanded
configuration, cache version, current Item snapshots, and recorded history.
Two Pipelines using the same Stage and lookup key share one web resolution.
Forced rebuild creates a new StageResult sequence and heals all active shared
consumers through the existing sequence reconciliation.

## 15. Dependencies and Supported Tool Set

Core dependencies:

- HTTPX for pooled direct HTTP;
- Beautiful Soup 4 for supported HTML parsing;
- `defusedxml>=0.7,<1` for non-resolving bounded RSS/XML parsing;
- `markdown-it-py>=4.2,<5` for deterministic raw-HTML Markdown sanitization;
- `html-to-markdown>=3.10,<4` for the always-available built-in Markdown projector and
  RSS Markdown projection.

The `web` dependency extra installs:

- curl_cffi;
- Trafilatura;
- Newspaper4k;
- Resiliparse.

Crawl4AI, Firecrawl, Zyte, Bright Data, and Jina use Sluice-owned HTTP adapters
rather than vendor SDKs.

The initial built-in set is deliberately limited to the stable combination
approved in this design. Scrapling, Magic-HTML, Pydoll, Crawlberg, MinerU-HTML,
Unstructured, browser agents, and asynchronous provider job APIs are not
initial built-ins. They may be considered later through an explicit design
that addresses maturity, licensing, dependency weight, and durable job state.

## 16. Observability and Management

Structured execution logs include:

- pipeline, node, Stage, execution, and Item identity;
- fetch and extract chain names and canonical digests;
- provider instance and type;
- ordered attempt number;
- elapsed time, sanitized status, quality metrics, and stable failure reason;
- terminal winner or exhaustion/drop outcome.

Logs never include resolved secrets, cookies, authorization headers, complete
HTML, complete Markdown, or vendor error bodies.

The management read model continues to expose Stage execution state rather
than introducing provider attempt rows. A failed web attempt is visible
through the Execution error summary. Provider attempts are diagnostic log
events, not a new durable table or queue.

## 17. Acceptance Criteria

The design is complete when implementation demonstrates all of the following:

1. A configured chain may contain local-a, local-b, and official cloud
   instances of the same provider type, and calls them in exact order.
2. Different Items run concurrently according to node policy, while providers
   for one Item never race.
3. HTTP 200 challenge, empty shell, short content, missing capability, and
   extractor failure each fall through to the next provider.
4. Provider-supplied quality-passing Markdown uses identity and is not fetched
   or extracted twice.
5. Firecrawl obtains all supported requested formats from one scrape operation.
6. Webpage list Source uses only raw-HTML-capable providers and preserves
   deterministic selector order and record identity.
7. RSS direct failure can recover through a raw-capable configured chain, and
   non-raw providers are skipped.
8. `fetch_webpage` exhaustion follows durable node retry count exactly and
   terminally drops only that each Item.
9. Stage timeout, restart recovery, stale generation, and late completion
   cannot publish an obsolete result.
10. A shared Stage performs one winning resolution for identical lookup keys;
    cache hit, retry, rebuild, and shared-consumer healing retain exact config.
11. Provider order, endpoint, result-affecting options, extractor order, or
    quality threshold changes alter the Stage key or Source binding.
12. Built-in web raw HTML and every credential resolved from a declared
    `env:` secret field do not appear in SQLite, Item payloads, metadata,
    logs, management output, tracked documentation, or exceptions; arbitrary
    input Item business data remains outside automatic secret classification.
13. Built-in direct URL handling rejects unsafe schemes, DNS answers,
    redirects, peers, and oversized responses while permitting explicitly
    allowlisted targets; `startup_http` is separately identified as an
    operator-trusted compatibility exception and never counted as evidence of
    these built-in protections.
14. Final Stage Items contain Markdown or text payloads and bounded deterministic
    `metadata.web`; no Artifact reaches the persistence boundary.
15. Current unit, integration, smoke, DDL, foreign-key, canonicalization,
    source-binding, shared-materialization, rebuild, Delivery, CLI, and Prefect
    tests remain green.

## 18. Git and Review Constraints

Implementation work remains on the current `v4-cleanroom` branch. It does not
modify `master`, the legacy package, or files owned by unrelated agents.

The hidden `.superpowers/` directory is permanently ignored and must not
appear in any commit or reachable branch history. Design and implementation
documents live only under tracked `docs/superpowers/` paths.

Implementation review occurs after every three implementation Tasks, not after
each Task. Each three-Task wave receives parallel architecture/contract/scope
and SQLite/concurrency/test-topology adversarial review. Review fixes receive
their own commits; mechanical Task commits may be consolidated within a wave
so the final history remains readable. Unrelated files are never staged or
committed.

## 19. Research Basis

The tool selection was checked against current official project or provider
documentation on 2026-08-18:

- HTTPX clients: <https://www.python-httpx.org/advanced/clients/>
- curl_cffi: <https://curl-cffi.readthedocs.io/en/stable/>
- Crawl4AI self-hosting: <https://docs.crawl4ai.com/core/self-hosting/>
- Firecrawl API: <https://docs.firecrawl.dev/api-reference/v2-introduction>
- Firecrawl self-hosting: <https://github.com/firecrawl/firecrawl/blob/main/SELF_HOST.md>
- Jina Reader: <https://jina.ai/reader/>
- Zyte browser API: <https://docs.zyte.com/zyte-api/usage/browser.html>
- Bright Data Web Unlocker: <https://docs.brightdata.com/scraping-automation/web-unlocker/introduction>
- Newspaper4k: <https://newspaper4k.readthedocs.io/>
- Resiliparse extraction: <https://resiliparse.chatnoir.eu/en/stable/man/extract.html>
- html-to-markdown: <https://docs.html-to-markdown.xberg.io/>
