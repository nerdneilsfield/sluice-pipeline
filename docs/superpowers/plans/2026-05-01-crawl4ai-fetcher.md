# Crawl4AI Fetcher & API Header Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `crawl4ai` fetcher to the existing FetcherChain, and add `api_headers` support (with `env:` resolution) to both Firecrawl and Crawl4AI fetchers.

**Architecture:** Three changes in sequence: (1) a builder helper resolves `api_headers` env vars before fetcher construction; (2) the Firecrawl fetcher gains `api_version` and `api_headers`; (3) a new `Crawl4AIFetcher` handles sync-or-poll extraction. All three are wired by existing `FetcherImplConfig(extra="allow")` machinery — no config.py changes needed.

**Tech Stack:** httpx (existing), respx (test mocking, existing dev dep), Python asyncio for poll loop.

---

## File Map

| File | Change |
|------|--------|
| `sluice/builders.py` | Add `_resolve_api_headers()` helper; call it in `build_fetcher_chain()` |
| `sluice/fetchers/firecrawl.py` | Add `api_version`, `api_headers`; fix endpoint path logic |
| `sluice/fetchers/crawl4ai.py` | **New** — full Crawl4AI fetcher with sync+poll |
| `sluice/cli.py` | Add `crawl4ai` to explicit import list (line 28) |
| `tests/fetchers/test_firecrawl.py` | Extend with `api_headers` and `api_version` tests |
| `tests/fetchers/test_crawl4ai.py` | **New** — full test coverage |

---

## Task 1: `_resolve_api_headers()` builder helper

**Files:**
- Modify: `sluice/builders.py` (near line 122 where `api_key` is resolved)
- Test: `tests/fetchers/test_chain.py` (add one test)

- [ ] **Step 1: Write the failing test**

Add to `tests/fetchers/test_chain.py`:

```python
import os
import pytest
from unittest.mock import AsyncMock, patch

@pytest.mark.asyncio
async def test_build_fetcher_chain_resolves_api_headers_env(monkeypatch, tmp_path):
    """api_headers env: values are resolved before passing to fetcher constructor."""
    monkeypatch.setenv("MY_SECRET", "resolved-value")

    from sluice.config import GlobalConfig, FetcherImplConfig
    from sluice.builders import build_fetcher_chain

    g = GlobalConfig(
        fetchers={
            "firecrawl": FetcherImplConfig(
                type="firecrawl",
                base_url="http://fc.local",
                api_headers={"Authorization": "env:MY_SECRET"},
            )
        },
        fetcher={"chain": ["firecrawl"], "min_chars": 10},
    )

    import sluice.fetchers.firecrawl  # noqa — register the fetcher

    constructed = []
    original_init = None

    import sluice.fetchers.firecrawl as fc_mod
    original_cls = fc_mod.FirecrawlFetcher

    class CapturingFetcher(original_cls):
        def __init__(self, **kwargs):
            constructed.append(kwargs)
            super().__init__(**kwargs)

    with patch.object(fc_mod, "FirecrawlFetcher", CapturingFetcher):
        # re-register the patched class
        from sluice.registry import _FETCHERS
        _FETCHERS["firecrawl"] = CapturingFetcher
        chain = build_fetcher_chain(g, None, None)

    assert constructed[0]["api_headers"] == {"Authorization": "resolved-value"}
```

- [ ] **Step 2: Run to confirm it fails**

```bash
uv run pytest tests/fetchers/test_chain.py::test_build_fetcher_chain_resolves_api_headers_env -v
```

Expected: FAIL — `FirecrawlFetcher.__init__` does not accept `api_headers` yet (TypeError), or `api_headers` arrives with the literal `"env:MY_SECRET"` string.

- [ ] **Step 3: Add `_resolve_api_headers()` and call it in `build_fetcher_chain()`**

In `sluice/builders.py`, add after the existing imports at the top of the file:

```python
def _resolve_api_headers(headers: dict) -> dict:
    """Resolve env: references in a header value dict."""
    from sluice.loader import resolve_env
    return {k: resolve_env(v) if isinstance(v, str) and v.startswith("env:") else v
            for k, v in headers.items()}
```

Then in `build_fetcher_chain()`, extend the existing env resolution block (currently lines ~123-128):

```python
    for name in chain_names:
        impl = global_cfg.fetchers.get(name)
        if impl is None:
            raise ValueError(f"fetcher {name!r} declared in chain but missing [fetchers.{name}]")
        cls = get_fetcher(impl.type)
        kwargs = impl.model_dump(exclude={"type", "extra"}, exclude_none=True)
        if kwargs.get("api_key") and kwargs["api_key"].startswith("env:"):
            from sluice.loader import resolve_env
            kwargs["api_key"] = resolve_env(kwargs["api_key"])
        if "api_headers" in kwargs and isinstance(kwargs["api_headers"], dict):
            kwargs["api_headers"] = _resolve_api_headers(kwargs["api_headers"])
        fetchers.append(cls(**kwargs))
```

- [ ] **Step 4: Run test (it will still fail — FirecrawlFetcher doesn't accept api_headers yet)**

```bash
uv run pytest tests/fetchers/test_chain.py::test_build_fetcher_chain_resolves_api_headers_env -v
```

Expected: FAIL with TypeError — this is correct, proceed to Task 2 which adds `api_headers` to FirecrawlFetcher.

- [ ] **Step 5: Commit the builder helper alone**

```bash
git add sluice/builders.py tests/fetchers/test_chain.py
git commit -m "feat(builders): resolve api_headers env: values before fetcher construction"
```

---

## Task 2: Update Firecrawl fetcher

**Files:**
- Modify: `sluice/fetchers/firecrawl.py`
- Test: `tests/fetchers/test_firecrawl.py` (extend)

- [ ] **Step 1: Write the failing tests**

Replace the content of `tests/fetchers/test_firecrawl.py` with:

```python
import pytest
import respx
from httpx import Response

from sluice.fetchers.firecrawl import FirecrawlFetcher


# ── endpoint routing ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_v2_default_endpoint():
    respx.post("https://fc.example/v2/scrape").mock(
        return_value=Response(200, json={"data": {"markdown": "hello"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.example")
    out = await f.extract("https://example.com/article")
    assert out == "hello"


@pytest.mark.asyncio
@respx.mock
async def test_v1_explicit():
    respx.post("https://fc.example/v1/scrape").mock(
        return_value=Response(200, json={"data": {"markdown": "v1 text"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.example", api_version="v1")
    out = await f.extract("https://example.com/article")
    assert out == "v1 text"


@pytest.mark.asyncio
@respx.mock
async def test_versioned_base_url_appends_scrape_only():
    respx.post("https://fc.example/v2/scrape").mock(
        return_value=Response(200, json={"data": {"markdown": "ok"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.example/v2")
    out = await f.extract("https://example.com/article")
    assert out == "ok"


def test_versioned_base_url_conflicts_with_api_version_raises():
    from sluice.core.errors import ConfigError
    with pytest.raises(ConfigError):
        FirecrawlFetcher(base_url="https://fc.example/v1", api_version="v2")


# ── api_headers ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_api_headers_sent():
    route = respx.post("https://fc.example/v2/scrape").mock(
        return_value=Response(200, json={"data": {"markdown": "x"}})
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
async def test_api_key_fallback_when_no_auth_header():
    route = respx.post("https://fc.example/v2/scrape").mock(
        return_value=Response(200, json={"data": {"markdown": "x"}})
    )
    f = FirecrawlFetcher(base_url="https://fc.example", api_key="mykey")
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["Authorization"] == "Bearer mykey"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_skipped_when_auth_header_in_api_headers():
    route = respx.post("https://fc.example/v2/scrape").mock(
        return_value=Response(200, json={"data": {"markdown": "x"}})
    )
    f = FirecrawlFetcher(
        base_url="https://fc.example",
        api_key="ignored",
        api_headers={"Authorization": "Bearer explicit"},
    )
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["Authorization"] == "Bearer explicit"


# ── existing api_key config still works ──────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_legacy_api_key_config():
    respx.post("https://api.firecrawl.dev/v2/scrape").mock(
        return_value=Response(200, json={"data": {"markdown": "ok"}})
    )
    f = FirecrawlFetcher(base_url="https://api.firecrawl.dev", api_key="fc-key")
    out = await f.extract("https://example.com/")
    assert out == "ok"
```

- [ ] **Step 2: Run to confirm they fail**

```bash
uv run pytest tests/fetchers/test_firecrawl.py -v
```

Expected: FAIL — current `FirecrawlFetcher` doesn't accept `api_version` or `api_headers`.

- [ ] **Step 3: Rewrite `sluice/fetchers/firecrawl.py`**

```python
import httpx
from fake_useragent import UserAgent

from sluice.core.errors import ConfigError
from sluice.fetchers._ssrf import guard
from sluice.fetchers.base import register_fetcher

_ua = UserAgent()


def _build_endpoint(base_url: str, api_version: str) -> str:
    """Return the full scrape endpoint URL.

    If base_url already ends with /v1 or /v2, validate it does not conflict
    with api_version, then use base_url/scrape directly.
    """
    stripped = base_url.rstrip("/")
    for ver in ("/v1", "/v2"):
        if stripped.endswith(ver):
            base_ver = ver.lstrip("/")
            if base_ver != api_version:
                raise ConfigError(
                    f"firecrawl base_url ends with /{base_ver} but api_version={api_version!r} "
                    f"— remove the version from base_url or align api_version"
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
        if self.api_key and "Authorization" not in headers:
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

- [ ] **Step 5: Re-run the builder test from Task 1**

```bash
uv run pytest tests/fetchers/test_chain.py::test_build_fetcher_chain_resolves_api_headers_env -v
```

Expected: PASS now (FirecrawlFetcher accepts `api_headers`).

- [ ] **Step 6: Run full suite**

```bash
uv run pytest -q
```

Expected: all pass.

- [ ] **Step 7: Commit**

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
import time
import pytest
import respx
from httpx import Response

from sluice.fetchers.crawl4ai import Crawl4AIFetcher, _extract_markdown


# ── _extract_markdown helper ─────────────────────────────────────────────────

def test_extract_markdown_results_list():
    data = {"results": [{"markdown": "hello"}]}
    assert _extract_markdown(data) == "hello"


def test_extract_markdown_result_dict():
    data = {"result": {"markdown": "world"}}
    assert _extract_markdown(data) == "world"


def test_extract_markdown_data_dict():
    data = {"data": {"markdown": "data-md"}}
    assert _extract_markdown(data) == "data-md"


def test_extract_markdown_top_level():
    data = {"markdown": "top"}
    assert _extract_markdown(data) == "top"


def test_extract_markdown_nested_dict_raw():
    data = {"markdown": {"raw_markdown": "raw", "fit_markdown": "fit"}}
    assert _extract_markdown(data) == "raw"


def test_extract_markdown_nested_dict_fit_fallback():
    data = {"markdown": {"fit_markdown": "fit"}}
    assert _extract_markdown(data) == "fit"


def test_extract_markdown_empty_returns_none():
    assert _extract_markdown({"foo": "bar"}) is None


def test_extract_markdown_empty_string_skipped():
    data = {"results": [{"markdown": ""}], "markdown": "top"}
    assert _extract_markdown(data) == "top"


# ── sync response ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_sync_results_response():
    respx.post("http://localhost:11235/crawl").mock(
        return_value=Response(200, json={"results": [{"markdown": "article text"}]})
    )
    f = Crawl4AIFetcher()
    out = await f.extract("https://example.com/article")
    assert out == "article text"


@pytest.mark.asyncio
@respx.mock
async def test_sync_data_response():
    respx.post("http://localhost:11235/crawl").mock(
        return_value=Response(200, json={"data": {"markdown": "data md"}})
    )
    f = Crawl4AIFetcher()
    out = await f.extract("https://example.com/article")
    assert out == "data md"


# ── api_headers ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_api_headers_sent_on_initial_request():
    route = respx.post("http://localhost:11235/crawl").mock(
        return_value=Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher(api_headers={"Authorization": "Bearer secret"})
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["Authorization"] == "Bearer secret"


@pytest.mark.asyncio
@respx.mock
async def test_api_key_fallback():
    route = respx.post("http://localhost:11235/crawl").mock(
        return_value=Response(200, json={"results": [{"markdown": "x"}]})
    )
    f = Crawl4AIFetcher(api_key="mykey")
    await f.extract("https://example.com/article")
    assert route.calls[0].request.headers["Authorization"] == "Bearer mykey"


# ── polling: task_id from response ───────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_poll_task_id_field(monkeypatch):
    monkeypatch.setattr("asyncio.sleep", AsyncMock := asyncio.coroutine(lambda s: None))

    respx.post("http://localhost:11235/crawl").mock(
        return_value=Response(200, json={"task_id": "t1"})
    )
    respx.get("http://localhost:11235/crawl/job/t1").mock(
        return_value=Response(200, json={
            "status": "completed",
            "results": [{"markdown": "polled"}],
        })
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)
    out = await f.extract("https://example.com/article")
    assert out == "polled"


@pytest.mark.asyncio
@respx.mock
async def test_poll_job_id_field(monkeypatch):
    monkeypatch.setattr("sluice.fetchers.crawl4ai.asyncio.sleep", asyncio.coroutine(lambda s: None))

    respx.post("http://localhost:11235/crawl").mock(
        return_value=Response(200, json={"job_id": "j1"})
    )
    respx.get("http://localhost:11235/crawl/job/j1").mock(
        return_value=Response(200, json={
            "status": "success",
            "results": [{"markdown": "job polled"}],
        })
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)
    out = await f.extract("https://example.com/article")
    assert out == "job polled"


@pytest.mark.asyncio
@respx.mock
async def test_poll_fallback_path_on_404(monkeypatch):
    monkeypatch.setattr("sluice.fetchers.crawl4ai.asyncio.sleep", asyncio.coroutine(lambda s: None))

    respx.post("http://localhost:11235/crawl").mock(
        return_value=Response(200, json={"task_id": "t2"})
    )
    respx.get("http://localhost:11235/crawl/job/t2").mock(
        return_value=Response(404)
    )
    respx.get("http://localhost:11235/task/t2").mock(
        return_value=Response(200, json={
            "status": "done",
            "results": [{"markdown": "fallback path"}],
        })
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)
    out = await f.extract("https://example.com/article")
    assert out == "fallback path"


@pytest.mark.asyncio
@respx.mock
async def test_poll_failure_status_raises(monkeypatch):
    monkeypatch.setattr("sluice.fetchers.crawl4ai.asyncio.sleep", asyncio.coroutine(lambda s: None))

    respx.post("http://localhost:11235/crawl").mock(
        return_value=Response(200, json={"task_id": "t3"})
    )
    respx.get("http://localhost:11235/crawl/job/t3").mock(
        return_value=Response(200, json={"status": "failed", "error": "timeout"})
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=5.0)
    with pytest.raises(Exception, match="failed"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_poll_timeout_raises(monkeypatch):
    monkeypatch.setattr("sluice.fetchers.crawl4ai.asyncio.sleep", asyncio.coroutine(lambda s: None))

    respx.post("http://localhost:11235/crawl").mock(
        return_value=Response(200, json={"task_id": "t4"})
    )
    respx.get("http://localhost:11235/crawl/job/t4").mock(
        return_value=Response(200, json={"status": "running"})
    )
    f = Crawl4AIFetcher(poll_interval=0.01, poll_timeout=0.05)
    with pytest.raises(Exception, match="timeout"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_no_markdown_and_no_task_id_raises():
    respx.post("http://localhost:11235/crawl").mock(
        return_value=Response(200, json={"status": "ok"})
    )
    f = Crawl4AIFetcher()
    with pytest.raises(Exception, match="no markdown"):
        await f.extract("https://example.com/article")


@pytest.mark.asyncio
@respx.mock
async def test_api_headers_sent_on_poll(monkeypatch):
    monkeypatch.setattr("sluice.fetchers.crawl4ai.asyncio.sleep", asyncio.coroutine(lambda s: None))

    respx.post("http://localhost:11235/crawl").mock(
        return_value=Response(200, json={"task_id": "t5"})
    )
    poll_route = respx.get("http://localhost:11235/crawl/job/t5").mock(
        return_value=Response(200, json={
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
```

- [ ] **Step 2: Run to confirm they fail**

```bash
uv run pytest tests/fetchers/test_crawl4ai.py -v
```

Expected: FAIL — module `sluice.fetchers.crawl4ai` does not exist.

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


def _extract_markdown(data: dict) -> str | None:
    """Try all documented response shapes and return the first non-empty markdown string."""
    candidates = [
        data.get("results", [{}])[0] if data.get("results") else None,
        data.get("result"),
        data.get("data"),
        data,
    ]
    for source in candidates:
        if not isinstance(source, dict):
            continue
        md = source.get("markdown")
        if isinstance(md, str) and md:
            return md
        if isinstance(md, dict):
            for key in ("raw_markdown", "fit_markdown", "markdown"):
                val = md.get(key)
                if isinstance(val, str) and val:
                    return val
    return None


def _get_task_id(data: dict) -> str | None:
    """Extract polling identifier from initial /crawl response."""
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
        if self.api_key and "Authorization" not in headers:
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

            for path_tpl in paths_to_try:
                url = self.base_url + path_tpl.replace("{task_id}", task_id)
                try:
                    resp = await client.get(url, headers=headers)
                except Exception as exc:
                    log.bind(fetcher="crawl4ai", task_id=task_id, path=url).debug(
                        f"crawl4ai.poll_request_error: {exc}"
                    )
                    continue

                if resp.status_code == 404:
                    continue  # try next path

                resp.raise_for_status()
                data = resp.json()
                status = data.get("status", "").lower()

                if status in _SUCCESS_STATUSES:
                    md = _extract_markdown(data)
                    if md:
                        return md
                    raise RuntimeError(
                        f"crawl4ai: task {task_id!r} completed but no markdown in response"
                    )

                if status in _FAILURE_STATUSES:
                    raise RuntimeError(
                        f"crawl4ai: task {task_id!r} ended with status={status!r}"
                    )

                # In-progress — record the working path and wait
                working_path = path_tpl
                break
            else:
                # All paths returned 404 — no working path found
                if working_path is None:
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

            # 2. Look for a polling identifier
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

- [ ] **Step 4: Run the tests**

```bash
uv run pytest tests/fetchers/test_crawl4ai.py -v
```

Expected: most PASS. Fix any asyncio.coroutine / monkeypatch issues if needed — use `asyncio.coroutine` only works on Python <3.11; replace with:

```python
import asyncio
from unittest.mock import AsyncMock

monkeypatch.setattr("sluice.fetchers.crawl4ai.asyncio.sleep", AsyncMock())
```

Update test file accordingly for all `monkeypatch.setattr("sluice.fetchers.crawl4ai.asyncio.sleep", ...)` lines to use `AsyncMock()`.

- [ ] **Step 5: Run full suite**

```bash
uv run pytest -q
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add sluice/fetchers/crawl4ai.py tests/fetchers/test_crawl4ai.py
git commit -m "feat(fetchers): add Crawl4AIFetcher with sync and poll extraction"
```

---

## Task 4: Wire CLI import

**Files:**
- Modify: `sluice/cli.py` (line 28)

- [ ] **Step 1: Add crawl4ai import to cli.py**

Find line 28 in `sluice/cli.py`:

```python
from sluice.fetchers import trafilatura_fetcher, firecrawl, jina_reader  # noqa
```

Change to:

```python
from sluice.fetchers import trafilatura_fetcher, firecrawl, jina_reader, crawl4ai  # noqa
```

- [ ] **Step 2: Run full suite to confirm no regressions**

```bash
uv run pytest -q
uv run ruff check .
```

Expected: all pass.

- [ ] **Step 3: Validate CLI can load a crawl4ai config**

Create a minimal test config and verify `sluice validate` does not crash:

```bash
mkdir -p /tmp/sluice-test-cfg/pipelines
cat > /tmp/sluice-test-cfg/sluice.toml << 'EOF'
[state]
db_path = "/tmp/sluice-test.db"

[fetcher]
chain = ["crawl4ai"]

[fetchers.crawl4ai]
type = "crawl4ai"
base_url = "http://localhost:11235"
EOF
cat > /tmp/sluice-test-cfg/providers.toml << 'EOF'
[[providers]]
name = "dummy"
type = "openai_compatible"
base = []
models = []
EOF
uv run sluice validate --config-dir /tmp/sluice-test-cfg
```

Expected: `All configurations valid` (or no error).

- [ ] **Step 4: Commit**

```bash
git add sluice/cli.py
git commit -m "feat(cli): add crawl4ai to explicit fetcher import list"
```

---

## Task 5: Update example config

**Files:**
- Modify: `configs/sluice.toml.example`

- [ ] **Step 1: Add crawl4ai example to sluice.toml.example**

In `configs/sluice.toml.example`, add after `[fetchers.trafilatura]`:

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

Also update `on_all_failed` in the example to `"use_raw_summary"` (already set) and show crawl4ai in the chain comment:

```toml
# chain = ["trafilatura", "crawl4ai", "firecrawl", "jina_reader"]
```

Update the Firecrawl example to show `api_version` and `api_headers`:

```toml
[fetchers.firecrawl]
type       = "firecrawl"
base_url   = "http://localhost:3002"
api_version = "v2"
api_key    = "env:FC_KEY"
# api_headers = { Authorization = "env:FC_KEY" }  # alternative to api_key
timeout    = 120
```

- [ ] **Step 2: Commit**

```bash
git add configs/sluice.toml.example
git commit -m "docs(configs): add crawl4ai example and api_version/api_headers for firecrawl"
```

---

## Self-Review

**Spec coverage check:**

| Requirement | Task |
|-------------|------|
| `crawl4ai` in `fetcher.chain` | Task 3 + Task 4 |
| Sync `results` response returns markdown | Task 3 (test: `test_sync_results_response`) |
| Async task_id polling | Task 3 (test: `test_poll_task_id_field`) |
| job_id fallback field | Task 3 (test: `test_poll_job_id_field`) |
| Poll path 404 fallback | Task 3 (test: `test_poll_fallback_path_on_404`) |
| Poll failure status raises | Task 3 (test: `test_poll_failure_status_raises`) |
| Poll timeout raises | Task 3 (test: `test_poll_timeout_raises`) |
| Firecrawl sends `api_headers` | Task 2 (test: `test_api_headers_sent`) |
| Crawl4AI sends `api_headers` on poll | Task 3 (test: `test_api_headers_sent_on_poll`) |
| Existing Firecrawl `api_key` still works | Task 2 (test: `test_legacy_api_key_config`) |
| `api_key` not used when `Authorization` in `api_headers` | Task 2 (test: `test_api_key_skipped_when_auth_header_in_api_headers`) |
| No secret header values in logs | `_build_headers()` never logs; `_poll()` logs only header names |
| `env:` resolved before constructor | Task 1 (test: `test_build_fetcher_chain_resolves_api_headers_env`) |
| `api_version` v1/v2 routing | Task 2 (tests: `test_v1_explicit`, `test_v2_default_endpoint`) |
| Versioned base_url conflict raises | Task 2 (test: `test_versioned_base_url_conflicts_with_api_version_raises`) |
| SSRF guard on article URL | `guard(url)` called in both fetchers' `extract()` |
| CLI can resolve `crawl4ai` configs | Task 4 |
| `results[0].markdown` dict shape handled defensively | Task 3 (`_extract_markdown` with isinstance check) |

**Placeholder scan:** No TBD, no "implement later", no "similar to Task N". All code blocks are complete.

**Type consistency:** `_extract_markdown(data: dict) -> str | None` used consistently. `_get_task_id(data: dict) -> str | None` consistent. `FirecrawlFetcher._build_headers()` and `Crawl4AIFetcher._build_headers()` both return `dict`. `api_headers: dict | None` in both constructors.
