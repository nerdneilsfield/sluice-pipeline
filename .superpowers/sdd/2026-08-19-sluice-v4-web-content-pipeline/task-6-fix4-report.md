# Task 6 fix round 4 report

## Confirmed regressions

- The configured `curl_cffi` adapter discarded guarded DNS addresses and used
  eager `Session.get`, so it neither pinned the approved peer nor capped the
  decompressed body while streaming.
- Curl proxy configuration was rejected wholesale instead of supporting the
  Design's locally validated proxy modes and explicit remote-DNS opt-in.
- Configured Trafilatura providers and optional extractors were not validated
  at startup; optional extractor runtime failures raised instead of producing
  `NO_MATCH`.
- A literal private IP could use `allow_hosts` as a policy bypass, catalog
  transport construction could race catalog close and leak a newly owned
  client, close diagnostics attached exception details, extractor runtime
  options had a mutable top-level dictionary, and secret resolution traversed
  ordinary option/action text.

## Fix

- The catalog now owns one `curl_cffi.Session` per worker thread. Each guarded
  hop uses `Session.request(..., content_callback=...)`, disables redirects,
  pins the exact approved address set with `CURLOPT_RESOLVE`, prevents stale
  connection reuse, and aborts at the decompressed body limit. A callback
  abort wrapped by curl is still classified as `body_too_large`. Startup fails
  with an instance-naming `DefinitionError` if the installed curl API lacks
  the required safe primitives.
- Curl supports declared proxies. Target URL/DNS policy is always evaluated
  locally; HTTP(S) and `socks5h` remote-DNS modes require
  `allow_remote_dns_proxy: true` and emit a fixed, safe warning that peer
  pinning cannot be verified. Local-DNS `socks5` retains target pinning.
- Configured Trafilatura providers and trafilatura/newspaper4k/resiliparse
  extractors validate their optional runtime APIs at catalog startup and name
  the configured instance on failure. Once validated, extraction exceptions
  return `NO_MATCH`. Terminal retained snapshots remain structural-only and
  do not construct optional runtimes or resolve secrets until explicit
  rebuild.
- Literal non-public targets now require a matching CIDR; exact hostname
  allowlists retain their DNS-host semantics. Catalog transport publication
  rechecks close state under the final lock and safely closes a newly created
  loser. Close logs contain only fixed codes. Extractor options are recursively
  immutable, and runtime secret resolution touches only schema-declared
  option/credential fields, preserving literal action text such as
  `env:NAME`.

## TDD and verification

- Initial focused red run: 10 failed, 3 passed (12 deselected), covering curl
  pinning/streaming/session ownership, proxy behavior, and optional runtime
  validation/failure semantics.
- Wave 2 focused red run: 5 failed, 2 passed, covering literal-IP policy,
  close races/logging, option immutability, and schema-aware secret resolution.
- Curl callback-wrapping regression was observed failing before the overflow
  classification fix.
- `python3 -m pytest -q tests/unit/test_web_providers.py tests/unit/test_web_safety.py tests/unit/test_web_runtime.py tests/integration/test_web_retention.py`
  → 87 passed.
- `python3 -m pytest -q tests/unit` → 193 passed.
- `python3 -m compileall -q src tests` → passed.
- `git diff --check` → passed.
