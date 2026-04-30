from __future__ import annotations

import asyncio
import re
import time
from typing import TYPE_CHECKING

import httpx

from sluice.core.item import Item
from sluice.enrichers.hn_parser import EnricherParseError, parse_hn_official, parse_hn_thread
from sluice.logging_setup import get_logger

if TYPE_CHECKING:
    from sluice.fetchers.chain import FetcherChain

log = get_logger(__name__)


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
        base_url: str = "https://www.hckrnws.com",
        request_delay_seconds: float = 10.0,
        top_comments: int = 20,
        chain: FetcherChain | None = None,
    ):
        self._pattern = re.compile(url_pattern)
        self._base = base_url.rstrip("/")
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

    async def _fetch_html(self, url: str) -> str:
        """Fetch HTML via fetcher chain if available, else direct httpx."""
        if self._chain is not None:
            html = await self._chain.fetch(url)
            if html:
                return html
            # chain returned nothing — fall through to direct httpx
        await self._bucket_for(httpx.URL(url).host).acquire()
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.text

    async def enrich(self, item: Item) -> str | None:
        # HN RSS feeds put the article URL in item.url and the HN
        # discussion URL in item.guid — check both.
        candidate = item.url or ""
        m = self._pattern.search(candidate)
        if not m and item.guid:
            m = self._pattern.search(item.guid)
        if not m:
            log.bind(url=item.url, guid=item.guid).debug("hn_comments.no_match")
            return None

        item_id = m.group(1)
        log.bind(item_id=item_id, url=item.url).debug("hn_comments.fetching")

        # Try hckrnws first, fallback to official HN site
        hckrnws_url = f"{self._base}/stories/{item_id}"
        try:
            html = await self._fetch_html(hckrnws_url)
            result = parse_hn_thread(html, top_n=self._top)
            source = "hckrnws"
        except (EnricherParseError, Exception) as exc:
            log.bind(item_id=item_id, error=str(exc)).debug(
                "hn_comments.hckrnws_failed_fallback_official"
            )
            official_url = f"https://news.ycombinator.com/item?id={item_id}"
            html = await self._fetch_html(official_url)
            result = parse_hn_official(html, top_n=self._top)
            source = "hn_official"

        preview = result[:200].replace("\n", " ") if result else "(empty)"
        log.bind(
            item_id=item_id, source=source, chars=len(result) if result else 0, preview=preview
        ).debug("hn_comments.fetched")
        return result
