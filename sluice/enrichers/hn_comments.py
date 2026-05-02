from __future__ import annotations

import asyncio
import re
import time
from typing import TYPE_CHECKING

import httpx

from sluice.core.item import Item
from sluice.enrichers.hn_parser import EnricherParseError, parse_hn_api_items
from sluice.logging_setup import get_logger

if TYPE_CHECKING:
    from sluice.fetchers.chain import FetcherChain

log = get_logger(__name__)

_HN_API = "https://hacker-news.firebaseio.com/v0"
_HN_OFFICIAL = "https://news.ycombinator.com"


class _HostBucket:
    def __init__(self, delay_seconds: float):
        self._delay = delay_seconds
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._last + self._delay - now)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()


class HnCommentsEnricher:
    name = "hn_comments"

    def __init__(
        self,
        *,
        url_pattern: str = r"news\.ycombinator\.com/item\?id=(\d+)",
        request_delay_seconds: float = 2.0,
        top_comments: int = 20,
        chain: FetcherChain | None = None,
    ):
        self._pattern = re.compile(url_pattern)
        self._top = top_comments
        self._buckets: dict[str, _HostBucket] = {}
        self._delay = request_delay_seconds
        self._chain = chain
        self._client = httpx.AsyncClient(timeout=20.0)

    def _bucket_for(self, host: str) -> _HostBucket:
        if host not in self._buckets:
            self._buckets[host] = _HostBucket(self._delay)
        return self._buckets[host]

    async def close(self):
        await self._client.aclose()

    async def _get_json_direct(self, url: str) -> dict:
        """Fetch JSON without rate limiting — for official APIs."""
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.json()

    async def _get_raw(self, url: str) -> httpx.Response:
        """Fetch with rate limiting — for scraped pages."""
        await self._bucket_for(httpx.URL(url).host).acquire()
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp

    async def _fetch_via_api(self, item_id: str) -> str:
        """Primary: HN Firebase API — no rate limiting, concurrent comment fetch."""
        data = await self._get_json_direct(f"{_HN_API}/item/{item_id}.json")
        if not data:
            raise EnricherParseError(f"hn api: item {item_id} not found")
        kids = data.get("kids", [])
        if not kids:
            raise EnricherParseError(f"hn api: item {item_id} has no comments")

        async def fetch_comment(kid_id: int) -> dict:
            try:
                return await self._get_json_direct(f"{_HN_API}/item/{kid_id}.json") or {}
            except Exception:
                return {}

        items = await asyncio.gather(*[fetch_comment(k) for k in kids[: self._top]])
        return parse_hn_api_items(list(items), top_n=self._top)

    async def _fetch_via_chain(self, item_id: str) -> str:
        """Fallback: use fetcher chain to extract text from the official HN page."""
        if self._chain is None:
            raise EnricherParseError("hn chain: no fetcher chain configured")
        url = f"{_HN_OFFICIAL}/item?id={item_id}"
        text = await self._chain.fetch(url)
        if not text or len(text) < 50:
            raise EnricherParseError(f"hn chain: too short ({len(text or '')} chars)")
        return text

    async def enrich(self, item: Item) -> str | None:
        candidate = item.url or ""
        m = self._pattern.search(candidate)
        if not m and item.guid:
            m = self._pattern.search(item.guid)
        if not m:
            log.bind(url=item.url, guid=item.guid).debug("hn_comments.no_match")
            return None

        item_id = m.group(1)
        log.bind(item_id=item_id, url=item.url).debug("hn_comments.fetching")

        attempts = [
            ("api", self._fetch_via_api),
            ("chain", self._fetch_via_chain),
        ]
        last_exc: Exception | None = None
        for source, fn in attempts:
            try:
                result = await fn(item_id)
                preview = result[:200].replace("\n", " ")
                log.bind(
                    item_id=item_id,
                    source=source,
                    chars=len(result),
                    preview=preview,
                ).debug("hn_comments.fetched")
                return result
            except Exception as exc:
                log.bind(item_id=item_id, source=source, error=str(exc)).debug(
                    "hn_comments.attempt_failed"
                )
                last_exc = exc

        raise EnricherParseError(f"hn_comments: all sources failed for {item_id}: {last_exc}")
