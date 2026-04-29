import asyncio
import re
import time

import httpx

from sluice.core.item import Item
from sluice.enrichers.hn_parser import parse_hn_thread


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
    ):
        self._pattern = re.compile(url_pattern)
        self._base = base_url.rstrip("/")
        self._top = top_comments
        self._buckets: dict[str, _HostBucket] = {}
        self._delay = request_delay_seconds
        self._client = httpx.AsyncClient(timeout=20.0)

    def _bucket_for(self, host: str) -> _HostBucket:
        if host not in self._buckets:
            self._buckets[host] = _HostBucket(self._delay)
        return self._buckets[host]

    async def close(self):
        await self._client.aclose()

    async def enrich(self, item: Item) -> str | None:
        m = self._pattern.search(item.url)
        if not m:
            return None
        item_id = m.group(1)
        target = f"{self._base}/stories/{item_id}"
        await self._bucket_for(httpx.URL(target).host).acquire()
        resp = await self._client.get(target)
        resp.raise_for_status()
        return parse_hn_thread(resp.text, top_n=self._top)
