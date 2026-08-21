# Task 6 fix round 2 report

## Confirmed regressions

- Catalog registration created a plain `httpx.Client` and passed it through
  `bounded_manual_get` as an injected `.get`, which allowed HTTPX to resolve
  the target again and buffer the response before the decompressed cap.
- The Trafilatura compatibility provider converted import/extraction failure
  into BeautifulSoup visible text, masking a provider `NO_MATCH`.
- Direct provider bindings accepted `proxy` but never used it.

## Fix

- Catalog HTTPX/Trafilatura transports now own one reusable HTTPX client with a
  dynamic `_PinnedHTTPTransport`; each guarded DNS answer set gets a pooled
  connection pool, requests carry the approved addresses, and `send(...,
  stream=True)` enforces the decompressed body cap before detaching responses.
  Catalog close retains ownership and remains idempotent.
- Trafilatura import, extraction, and empty-output failures return `NO_MATCH`;
  no visible-text fallback is performed.
- Configured proxies are rejected for fixed direct providers at provider build
  and retained-binding validation, rather than silently ignored.

## TDD and verification

- Red regressions were added for pooled pinning/streaming and cap enforcement,
  Trafilatura missing/failing extraction, and configured proxy rejection; the
  pre-fix run failed all four.
- `python3 -m pytest -q tests/unit/test_web_providers.py tests/unit/test_web_safety.py tests/unit/test_web_runtime.py tests/integration/test_web_retention.py`
  → 70 passed.
- `python3 -m pytest -q tests/unit` → 179 passed.
- `python3 -m compileall -q src tests` → passed.

The broader integration suite still contains unrelated baseline Task 7 and
legacy fixture failures; no later coordinator behavior or fixture migration was
changed here.
