# Crawl4AI Fetcher & API Header Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `crawl4ai` fetcher to the existing FetcherChain, and add `api_headers` support (with `env:` resolution) to both Firecrawl and Crawl4AI fetchers.

**Architecture:** Three changes in sequence: (1) a builder helper resolves `api_headers` env vars before fetcher construction — tested directly against `_resolve_api_headers()`; (2) the Firecrawl fetcher gains `api_version` and `api_headers`; (3) a new `Crawl4AIFetcher` handles sync-or-poll extraction. All three are wired by existing `FetcherImplConfig(extra="allow")` machinery — no config.py changes needed.

**Tech Stack:** httpx (existing), respx (test mocking, existing dev dep), `unittest.mock.AsyncMock` for async sleep.

---

## Shared test fixture: SSRF monkeypatch

Every test that calls `fetcher.extract("https://...")` must monkeypatch `socket.getaddrinfo` to avoid real DNS lookups through the SSRF guard. Copy this fixture exactly into each new test file:

```python
import socket

@pytest.fixture
def allow_public_dns(monkeypatch):
    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]
    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
```

Use it on every test that calls `extract()`:

```python
async def test_something(allow_public_dns):
    ...
```

---

## File Map

| File | Change |
|------|--------|
| `sluice/builders.py` | Add `_resolve_api_headers()` helper; call it in `build_fetcher_chain()` |
| `sluice/fetchers/firecrawl.py` | Add `api_version`, `api_headers`; fix endpoint path + validation |
| `sluice/fetchers/crawl4ai.py` | **New** — full Crawl4AI fetcher with sync+poll |
| `sluice/cli.py` | Add `crawl4ai` to explicit import list (line 28) |
| `tests/test_builders.py` | Add `_resolve_api_headers` unit test |
| `tests/fetchers/test_firecrawl.py` | Extend with `api_headers` and `api_version` tests |
| `tests/fetchers/test_crawl4ai.py` | **New** — full test coverage |
| `configs/sluice.toml.example` | Add crawl4ai + update firecrawl examples |

---

## Task 1: `_resolve_api_headers()` builder helper

**Files:**
- Modify: `sluice/builders.py`
- Test: `tests/test_builders.py`

- [ ] **Step 1: Write the failing test in `tests/test_builders.py`**

Add at the bottom of the existing file:

```python
def test_resolve_api_headers_resolves_env_values(monkeypatch):
    """_resolve_api_headers substitutes env: prefixed values."""
    from sluice.builders import _resolve_api_headers

    monkeypatch.setenv("TEST_TOKEN", "secret-value")
    result = _resolve_api_headers({
        "Authorization": "env:TEST_TOKEN",
        "X-Static": "literal",
    })
    assert result == {"Authorization": "secret-value", "X-Static": "literal"}


def test_resolve_api_headers_passthrough_non_env():
    from sluice.builders import _resolve_api_headers

    result = _resolve_api_headers({"Authorization": "Bearer abc"})
    assert result == {"Authorization": "Bearer abc"}
```

- [ ] **Step 2: Run to confirm fail**

```bash
uv run pytest tests/test_builders.py::test_resolve_api_headers_resolves_env_values -v
```

Expected: FAIL — `ImportError: cannot import name '_resolve_api_headers'`

- [ ] **Step 3: Add helper and call it in `build_fetcher_chain()`**

In `sluice/builders.py`, add this function near the top (after imports):

```python
def _resolve_api_headers(headers: dict) -> dict:
    """Resolve env: references in header value strings."""
    from sluice.loader import resolve_env
    return {
        k: resolve_env(v) if isinstance(v, str) and v.startswith("env:") else v
        for k, v in headers.items()
    }
```

Then in `build_fetcher_chain()`, extend the kwargs resolution block (currently resolves only `api_key`):

```python
        kwargs = impl.model_dump(exclude={"type", "extra"}, exclude_none=True)
        if kwargs.get("api_key") and kwargs["api_key"].startswith("env:"):
            from sluice.loader import resolve_env
            kwargs["api_key"] = resolve_env(kwargs["api_key"])
        if "api_headers" in kwargs and isinstance(kwargs["api_headers"], dict):
            kwargs["api_headers"] = _resolve_api_headers(kwargs["api_headers"])
        fetchers.append(cls(**kwargs))
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_builders.py::test_resolve_api_headers_resolves_env_values tests/test_builders.py::test_resolve_api_headers_passthrough_non_env -v
```

Expected: both PASS.

- [ ] **Step 5: Run full suite to confirm no regressions**

```bash
uv run pytest -q
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add sluice/builders.py tests/test_builders.py
git commit -m "feat(builders): resolve api_headers env: values before fetcher construction"
```

---

## Task 2: Update Firecrawl fetcher

**Files:**
- Modify: `sluice/fetchers/firecrawl.py`
- Test: `tests/fetchers/test_firecrawl.py`

- [ ] **Step 1: Write failing tests**

Replace `tests/fetchers/test_firecrawl.py` with:

```python
import socket
import httpx
import pytest
import respx

from sluice.core.errors import ConfigError
from sluice.fetchers.firecrawl import FirecrawlFetcher


@pytest.fixture
def allow_public_dns(monkeypatch):
    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]
    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)


# ── endpoint routing ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_v2_default_endpoint(allow_public_dns):
    respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "hello"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.example")
    out = await f.extract("https://example.com/article")
    assert out == "hello"


@pytest.mark.asyncio
@respx.mock
async def test_v1_explicit(allow_public_dns):
    respx.post("https://fc.example/v1/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "v1 text"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.example", api_version="v1")
    out = await f.extract("https://example.com/article")
    assert out == "v1 text"


@pytest.mark.asyncio
@respx.mock
async def test_versioned_base_url_uses_scrape_only(allow_public_dns):
    respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "ok"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.example/v2")
    out = await f.extract("https://example.com/article")
    assert out == "ok"


def test_versioned_base_url_conflicts_with_api_version_raises():
    with pytest.raises(ConfigError, match="conflict"):
        FirecrawlFetcher(base_url="https://fc.example/v1", api_version="v2")


def test_invalid_api_version_raises():
    with pytest.raises(ConfigError, match="api_version"):
        FirecrawlFetcher(base_url="https://fc.example", api_version="v3")


# ── api_headers ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_api_headers_sent(allow_public_dns):
    route = respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "x"}})
    )
    f = FirecrawlFetcher(
        base_url="https://fc.example",
        api_headers={"Authorization": "Bearer tok", "X-Custom": "val"},
    )
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["Authorization"] == "Bearer tok"
    assert route.calls[0].request.headers["X-Custom"] == "val"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_fallback_when_no_auth_header(allow_public_dns):
    route = respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "x"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.example", api_key="mykey")
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["Authorization"] == "Bearer mykey"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_skipped_when_auth_header_present(allow_public_dns):
    route = respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "x"}})
    )
    f = FirecrawlFetcher(
        base_url="https://fc.example",
        api_key="ignored",
        api_headers={"Authorization": "Bearer explicit"},
    )
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["Authorization"] == "Bearer explicit"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_skipped_when_auth_header_lowercase(allow_public_dns):
    """Case-insensitive check: authorization= should block api_key fallback."""
    route = respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "x"}})
    )
    f = FirecrawlFetcher(
        base_url="https://fc.example",
        api_key="ignored",
        api_headers={"authorization": "Bearer lower"},
    )
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["authorization"] == "Bearer lower"
    assert "Bearer ignored" not in str(route.calls[0].request.headers)


# ── legacy api_key config still works ────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_legacy_api_key_config(allow_public_dns):
    respx.post("https://fc.local/v1/scrape").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "ok"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.local", api_key="fc-key", api_version="v1")
    out = await f.extract("https://example.com/")
    assert out == "ok"


# ── existing failure behavior ─────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_firecrawl_failure_raises(allow_public_dns):
    respx.post("https://fc.example/v2/scrape").mock(
        return_value=httpx.Response(500)
    )
    f = FirecrawlFetcher(base_url="https://fc.example")
    with pytest.raises(Exception):
        await f.extract("https://example.com/article")
```

- [ ] **Step 2: Run to confirm they fail**

```bash
uv run pytest tests/fetchers/test_firecrawl.py -v
```

Expected: multiple FAIL — `api_version`, `api_headers` not accepted; wrong endpoint.

- [ ] **Step 3: Rewrite `sluice/fetchers/firecrawl.py`**

```python
import httpx
from fake_useragent import UserAgent

from sluice.core.errors import ConfigError
from sluice.fetchers._ssrf import guard
from sluice.fetchers.base import register_fetcher

_ua = UserAgent()
_VALID_VERSIONS = {"v1", "v2"}


def _has_auth_header(headers: dict) -> bool:
    return any(k.lower() == "authorization" for k in headers)


def _build_endpoint(base_url: str, api_version: str) -> str:
    """Return the full scrape endpoint URL, enforcing version consistency."""
    if api_version not in _VALID_VERSIONS:
        raise ConfigError(
            f"firecrawl api_version={api_version!r} is invalid; must be one of {sorted(_VALID_VERSIONS)}"
        )
    stripped = base_url.rstrip("/")
    for ver in ("v1", "v2"):
        if stripped.endswith(f"/{ver}"):
            if ver != api_version:
                raise ConfigError(
                    f"firecrawl base_url ends with /{ver} but api_version={api_version!r} — conflict"
                )
            return f"{stripped}/scrape"
    return f"{stripped}/{api_version}/scrape"


@register_fetcher("firecrawl")
class FirecrawlFetcher:
    name = "firecrawl"

    def __init__(
        self,
        *,
        base_url: str,
        api_version: str = "v2",
        api_key: str | None = None,
        api_headers: dict | None = None,
        timeout: float = 60.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self.api_key = api_key
        self.api_headers = dict(api_headers) if api_headers else {}
        self.timeout = timeout
        self._endpoint = _build_endpoint(self.base_url, api_version)

    def _build_headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        headers.update(self.api_headers)
        if self.api_key and not _has_auth_header(headers):
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def extract(self, url: str) -> str:
        guard(url)
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.post(
                self._endpoint,
                headers=self._build_headers(),
                json={"url": url, "formats": ["markdown"]},
            )
            r.raise_for_status()
            data = r.json()
        return data.get("data", {}).get("markdown", "") or ""
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/fetchers/test_firecrawl.py -v
```

Expected: all PASS.

- [ ] **Step 5: Run full suite**

```bash
uv run pytest -q
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add sluice/fetchers/firecrawl.py tests/fetchers/test_firecrawl.py
git commit -m "feat(fetchers): add api_version and api_headers to FirecrawlFetcher"
```

---

## Task 3: New Crawl4AI fetcher

**Files:**
- Create: `sluice/fetchers/crawl4ai.py`
- Create: `tests/fetchers/test_crawl4ai.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/fetchers/test_crawl4ai.py`:

```python
import asyncio
import socket
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from sluice.fetchers.crawl4ai import Crawl4AIFetcher, _extract_markdown


@pytest.fixture
def allow_public_dns(monkeypatch):
    def fake_getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]
    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr("sluice.fetchers.crawl4ai.asyncio.sleep", AsyncMock())


# ── _extract_markdown unit tests ──────────────────────────────────────────────

def test_extract_results_list():
    assert _extract_markdown({"results": [{"markdown": "hello"}]}) == "hello"


def test_extract_result_dict():
    assert _extract_markdown({"result": {"markdown": "world"}}) == "world"


def test_extract_data_dict():
    assert _extract_markdown({"data": {"markdown": "data-md"}}) == "data-md"


def test_extract_top_level():
    assert _extract_markdown({"markdown": "top"}) == "top"


def test_extract_nested_dict_raw_markdown():
    assert _extract_markdown({"markdown": {"raw_markdown": "raw", "fit_markdown": "fit"}}) == "raw"


def test_extract_nested_dict_fit_fallback():
    assert _extract_markdown({"markdown": {"fit_markdown": "fit"}}) == "fit"


def test_extract_empty_string_skipped_tries_next():
    data = {"results": [{"markdown": ""}], "markdown": "top"}
    assert _extract_markdown(data) == "top"


def test_extract_no_markdown_returns_none():
    assert _extract_markdown({"foo": "bar"}) is None


# ── sync response ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_sync_results_response(allow_public_dns):
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "article text"}]})
    )
    f = Crawl4AIFetcher()
    out = await f.extract("https://example.com/article")
    assert out == "article text"


@pytest.mark.asyncio
@respx.mock
async def test_sync_data_response(allow_public_dns):
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"data": {"markdown": "data md"}})
    )
    f = Crawl4AIFetcher()
    out = await f.extract("https://example.com/article")
    assert out == "data md"


# ── api_headers / api_key ─────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_api_headers_on_initial_request(allow_public_dns):
    route = respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher(api_headers={"Authorization": "Bearer secret"})
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["Authorization"] == "Bearer secret"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_fallback(allow_public_dns):
    route = respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher(api_key="mykey")
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["Authorization"] == "Bearer mykey"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_skipped_when_lowercase_auth_present(allow_public_dns):
    route = respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher(api_key="ignored", api_headers={"authorization": "Bearer lower"})
    await f.extract("https://example.com/article")
    assert "Bearer ignored" not in str(route.calls[0].request.headers)


# ── polling: task_id from response ───────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_poll_task_id(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t1"})
    )
    respx.get("http://localhost:11235/crawl/job/t1").mock(
        return_value=httpx.Response(200, json={
            "status": "completed",
            "results": [{"markdown": "polled"}],
        })
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)
    assert await f.extract("https://example.com/article") == "polled"


@pytest.mark.asyncio
@respx.mock
async def test_poll_job_id_field(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"job_id": "j1"})
    )
    respx.get("http://localhost:11235/crawl/job/j1").mock(
        return_value=httpx.Response(200, json={
            "status": "success",
            "results": [{"markdown": "job polled"}],
        })
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)
    assert await f.extract("https://example.com/article") == "job polled"


@pytest.mark.asyncio
@respx.mock
async def test_poll_fallback_path_on_404(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t2"})
    )
    respx.get("http://localhost:11235/crawl/job/t2").mock(
        return_value=httpx.Response(404)
    )
    respx.get("http://localhost:11235/task/t2").mock(
        return_value=httpx.Response(200, json={
            "status": "done",
            "results": [{"markdown": "fallback path"}],
        })
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)
    assert await f.extract("https://example.com/article") == "fallback path"


@pytest.mark.asyncio
@respx.mock
async def test_poll_failure_status_raises(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t3"})
    )
    respx.get("http://localhost:11235/crawl/job/t3").mock(
        return_value=httpx.Response(200, json={"status": "failed"})
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)
    with pytest.raises(RuntimeError, match="failed"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_poll_timeout_raises(allow_public_dns, no_sleep, monkeypatch):
    """Poll timeout: patch the module-level time used by the fetcher."""
    tick = [0.0]

    def fake_monotonic():
        tick[0] += 0.1
        return tick[0]

    monkeypatch.setattr("sluice.fetchers.crawl4ai.time.monotonic", fake_monotonic)
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t4"})
    )
    respx.get("http://localhost:11235/crawl/job/t4").mock(
        return_value=httpx.Response(200, json={"status": "running"})
    )
    f = Crawl4AIFetcher(poll_interval=0.0, poll_timeout=0.05)
    with pytest.raises(RuntimeError, match="timeout"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_no_markdown_and_no_task_id_raises(allow_public_dns):
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"status": "queued"})
    )
    f = Crawl4AIFetcher()
    with pytest.raises(RuntimeError, match="no markdown"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_api_headers_sent_on_poll(allow_public_dns, no_sleep):
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t5"})
    )
    poll_route = respx.get("http://localhost:11235/crawl/job/t5").mock(
        return_value=httpx.Response(200, json={
            "status": "completed",
            "results": [{"markdown": "x"}],
        })
    )
    f = Crawl4AIFetcher(
        api_headers={"Authorization": "Bearer tok"},
        poll_interval=0.01,
        poll_timeout=5.0,
    )
    await f.extract("https://example.com/article")
    assert poll_route.calls[0].request.headers["Authorization"] == "Bearer tok"


@pytest.mark.asyncio
@respx.mock
async def test_poll_inline_markdown_in_poll_response_skips_status(allow_public_dns, no_sleep):
    """If poll response has inline markdown, return it without checking status."""
    respx.post("http://localhost:11235/crawl").mock(
        return_value=httpx.Response(200, json={"task_id": "t6"})
    )
    respx.get("http://localhost:11235/crawl/job/t6").mock(
        return_value=httpx.Response(200, json={
            "status": "unknown_status",
            "results": [{"markdown": "inline from poll"}],
        })
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)
    # Should return the markdown even though status is unknown
    out = await f.extract("https://example.com/article")
    assert out == "inline from poll"
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/fetchers/test_crawl4ai.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'sluice.fetchers.crawl4ai'`

- [ ] **Step 3: Create `sluice/fetchers/crawl4ai.py`**

```python
import asyncio
import time

import httpx

from sluice.fetchers._ssrf import guard
from sluice.fetchers.base import register_fetcher
from sluice.logging_setup import get_logger

log = get_logger(__name__)

_SUCCESS_STATUSES = {"completed", "success", "done"}
_FAILURE_STATUSES = {"failed", "error", "cancelled"}
_DEFAULT_POLL_PATHS = [
    "/crawl/job/{task_id}",
    "/task/{task_id}",
    "/job/{task_id}",
]


def _has_auth_header(headers: dict) -> bool:
    return any(k.lower() == "authorization" for k in headers)


def _extract_markdown(data: dict) -> str | None:
    """Try all documented response shapes; return first non-empty markdown string."""
    candidates = []
    if isinstance(data.get("results"), list) and data["results"]:
        candidates.append(data["results"][0])
    for key in ("result", "data"):
        if isinstance(data.get(key), dict):
            candidates.append(data[key])
    candidates.append(data)

    for source in candidates:
        if not isinstance(source, dict):
            continue
        md = source.get("markdown")
        if isinstance(md, str) and md:
            return md
        if isinstance(md, dict):
            for sub_key in ("raw_markdown", "fit_markdown", "markdown"):
                val = md.get(sub_key)
                if isinstance(val, str) and val:
                    return val
    return None


def _get_task_id(data: dict) -> str | None:
    """Extract polling identifier: task_id → job_id → id."""
    for field in ("task_id", "job_id", "id"):
        val = data.get(field)
        if isinstance(val, str) and val:
            return val
    return None


@register_fetcher("crawl4ai")
class Crawl4AIFetcher:
    name = "crawl4ai"

    def __init__(
        self,
        *,
        base_url: str = "http://localhost:11235",
        api_key: str | None = None,
        api_headers: dict | None = None,
        timeout: float = 120.0,
        poll_interval: float = 2.0,
        poll_timeout: float | None = None,
        poll_paths: list | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.api_headers = dict(api_headers) if api_headers else {}
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.poll_timeout = poll_timeout if poll_timeout is not None else timeout
        self.poll_paths = list(poll_paths) if poll_paths else list(_DEFAULT_POLL_PATHS)

    def _build_headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        headers.update(self.api_headers)
        if self.api_key and not _has_auth_header(headers):
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def _poll(self, client: httpx.AsyncClient, task_id: str) -> str:
        headers = self._build_headers()
        start = time.monotonic()
        working_path: str | None = None

        while True:
            elapsed = time.monotonic() - start
            if elapsed >= self.poll_timeout:
                raise RuntimeError(
                    f"crawl4ai: poll timeout after {elapsed:.1f}s for task {task_id!r}"
                )

            paths_to_try = [working_path] if working_path else self.poll_paths

            found_working = False
            for path_tpl in paths_to_try:
                url = self.base_url + path_tpl.replace("{task_id}", task_id)
                try:
                    resp = await client.get(url, headers=headers)
                except Exception as exc:
                    log.bind(fetcher="crawl4ai", task_id=task_id).debug(
                        f"crawl4ai.poll_request_error: {exc}"
                    )
                    continue

                if resp.status_code == 404:
                    continue

                resp.raise_for_status()
                data = resp.json()

                # Check inline markdown first (compatible with older /task/{id} format)
                md = _extract_markdown(data)
                if md:
                    return md

                status = data.get("status", "").lower()
                if status in _FAILURE_STATUSES:
                    raise RuntimeError(
                        f"crawl4ai: task {task_id!r} ended with status={status!r}"
                    )

                # In-progress — record this path as working, break to sleep
                working_path = path_tpl
                found_working = True
                break

            if not found_working and working_path is None:
                raise RuntimeError(
                    f"crawl4ai: no poll path responded for task {task_id!r}"
                )

            await asyncio.sleep(self.poll_interval)

    async def extract(self, url: str) -> str:
        guard(url)
        headers = self._build_headers()

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/crawl",
                headers=headers,
                json={"urls": [url]},
            )
            resp.raise_for_status()
            data = resp.json()

            # 1. Try inline markdown first
            md = _extract_markdown(data)
            if md:
                return md

            # 2. Look for polling identifier
            task_id = _get_task_id(data)
            if not task_id:
                raise RuntimeError(
                    f"crawl4ai: no markdown and no task id in /crawl response "
                    f"(keys: {list(data.keys())})"
                )

            log.bind(fetcher="crawl4ai", task_id=task_id, url=url).debug(
                "crawl4ai.polling_started"
            )
            return await self._poll(client, task_id)
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/fetchers/test_crawl4ai.py -v
```

Expected: all PASS.

- [ ] **Step 5: Run full suite**

```bash
uv run pytest -q
uv run ruff check .
```

Expected: all pass, no lint errors.

- [ ] **Step 6: Commit**

```bash
git add sluice/fetchers/crawl4ai.py tests/fetchers/test_crawl4ai.py
git commit -m "feat(fetchers): add Crawl4AIFetcher with sync and poll extraction"
```

---

## Task 4: Wire CLI import + verify registry

**Files:**
- Modify: `sluice/cli.py`
- Test: `tests/fetchers/test_crawl4ai.py` (add one more test)

- [ ] **Step 1: Write registry test**

Add to `tests/fetchers/test_crawl4ai.py`:

```python
def test_crawl4ai_registered_via_cli_import_all():
    """_import_all() (the CLI path) must register crawl4ai in the fetcher registry."""
    from sluice.cli import _import_all
    from sluice.registry import get_fetcher
    _import_all()
    cls = get_fetcher("crawl4ai")
    assert cls is Crawl4AIFetcher
```

- [ ] **Step 2: Run to confirm it fails**

```bash
uv run pytest tests/fetchers/test_crawl4ai.py::test_crawl4ai_registered_via_cli_import_all -v
```

Expected: FAIL — `crawl4ai` not in registry yet (cli.py import not updated).

- [ ] **Step 3: Add crawl4ai to CLI explicit import**

In `sluice/cli.py`, find line 28:

```python
from sluice.fetchers import trafilatura_fetcher, firecrawl, jina_reader  # noqa
```

Change to:

```python
from sluice.fetchers import trafilatura_fetcher, firecrawl, jina_reader, crawl4ai  # noqa
```

- [ ] **Step 4: Run the registry test — should now pass**

```bash
uv run pytest tests/fetchers/test_crawl4ai.py::test_crawl4ai_registered_via_cli_import_all -v
```

Expected: PASS.

- [ ] **Step 4b: CLI smoke test**

```bash
uv run python -c "from sluice.cli import _import_all; _import_all(); from sluice.registry import get_fetcher; print('OK:', get_fetcher('crawl4ai'))"
```

Expected output: `OK: <class 'sluice.fetchers.crawl4ai.Crawl4AIFetcher'>`

- [ ] **Step 5: Run full suite**

```bash
uv run pytest -q
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add sluice/cli.py tests/fetchers/test_crawl4ai.py
git commit -m "feat(cli): add crawl4ai to explicit fetcher import list"
```

---

## Task 5: Update example config

**Files:**
- Modify: `configs/sluice.toml.example`

- [ ] **Step 1: Update `configs/sluice.toml.example`**

Replace the Firecrawl fetcher block with one that uses `api_version` and `api_headers`:

```toml
[fetchers.firecrawl]
type        = "firecrawl"
# Self-hosted: base_url = "http://localhost:3002", api_version = "v1"
base_url    = "https://api.firecrawl.dev"
api_version = "v2"
api_key     = "env:FC_KEY"
# api_headers = { Authorization = "env:FC_KEY" }  # alternative to api_key
timeout     = 120
```

Add the crawl4ai block after trafilatura:

```toml
# ── Crawl4AI (self-hosted, optional) ─────────────────────────────────────────
# Requires: docker run -p 11235:11235 unclecode/crawl4ai
#
# [fetchers.crawl4ai]
# type          = "crawl4ai"
# base_url      = "http://localhost:11235"
# api_headers   = { Authorization = "env:CRAWL4AI_AUTH" }  # optional
# timeout       = 120
# poll_interval = 2.0
# poll_timeout  = 120
```

Update the chain example comment:

```toml
# chain = ["trafilatura", "crawl4ai", "firecrawl", "jina_reader"]
```

- [ ] **Step 2: Commit**

```bash
git add configs/sluice.toml.example
git commit -m "docs(configs): add crawl4ai and api_headers examples; fix firecrawl api_version"
```

---

## Self-Review

**Spec coverage check:**

| Requirement | Task |
|-------------|------|
| `crawl4ai` in `fetcher.chain` | Task 3 + Task 4 |
| Sync `results` response | Task 3 (`test_sync_results_response`) |
| Sync `data` response | Task 3 (`test_sync_data_response`) |
| Async `task_id` polling | Task 3 (`test_poll_task_id`) |
| `job_id` field fallback | Task 3 (`test_poll_job_id_field`) |
| Poll path 404 fallback | Task 3 (`test_poll_fallback_path_on_404`) |
| Poll failure status raises | Task 3 (`test_poll_failure_status_raises`) |
| Poll timeout raises | Task 3 (`test_poll_timeout_raises`) |
| Inline markdown check before status | Task 3 (`test_poll_inline_markdown_in_poll_response_skips_status`) |
| Firecrawl `api_headers` sent | Task 2 (`test_api_headers_sent`) |
| Crawl4AI `api_headers` on poll | Task 3 (`test_api_headers_sent_on_poll`) |
| Existing Firecrawl `api_key` works | Task 2 (`test_legacy_api_key_config`) |
| `api_key` skipped when auth header present | Task 2 (`test_api_key_skipped_when_auth_header_present`) |
| Case-insensitive auth header check | Task 2 (`test_api_key_skipped_when_auth_header_lowercase`), Task 3 (`test_api_key_skipped_when_lowercase_auth_present`) |
| No secret values in logs | `_build_headers()` never logged; `_poll()` logs only task_id and URL |
| `env:` resolved before constructor | Task 1 (tests + builder change) |
| `api_version` v1/v2 routing | Task 2 (`test_v1_explicit`, `test_v2_default_endpoint`) |
| Invalid `api_version` raises | Task 2 (`test_invalid_api_version_raises`) |
| Versioned base_url conflict raises | Task 2 (`test_versioned_base_url_conflicts_with_api_version_raises`) |
| SSRF guard on article URL | `guard(url)` in both `extract()` methods |
| CLI resolves `crawl4ai` type | Task 4 (`test_crawl4ai_registered_in_registry` + CLI smoke test) |
| `_extract_markdown` dict-vs-string safe | Task 3 (`_extract_markdown` with `isinstance` checks) |

**Placeholder scan:** None found. All code blocks complete.

**Type consistency:** `_extract_markdown(data: dict) -> str | None` consistent across all callsites. `_get_task_id(data: dict) -> str | None` consistent. `_has_auth_header(headers: dict) -> bool` defined in both files independently (DRY violation is acceptable — these are small independent modules). `api_headers: dict | None` in both constructors.
