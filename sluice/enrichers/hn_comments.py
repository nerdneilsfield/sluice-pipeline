import asyncio
import re
import time

import httpx

from sluice.core.item import Item
from sluice.enrichers.hn_parser import (
    EnricherParseError,
    parse_hn_api_items,
    parse_hn_official,
    parse_hn_thread,
)
from sluice.logging_setup import get_logger

log = get_logger(__name__)

_HN_API = "https://hacker-news.firebaseio.com/v0"
_HN_OFFICIAL = "https://news.ycombinator.com"
_HCKRNWS = "https://www.hckrnws.com"


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
        base_url: str = _HCKRNWS,
        request_delay_seconds: float = 2.0,
        top_comments: int = 20,
        chain=None,  # accepted for API compat, not used
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

    async def _get(self, url: str) -> httpx.Response:
        await self._bucket_for(httpx.URL(url).host).acquire()
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp

    async def _fetch_via_api(self, item_id: str) -> str:
        """Primary: HN Firebase API — structured JSON, no HTML parsing needed."""
        resp = await self._get(f"{_HN_API}/item/{item_id}.json")
        data = resp.json()
        if not data:
            raise EnricherParseError(f"hn api: item {item_id} not found")
        kids = data.get("kids", [])
        if not kids:
            raise EnricherParseError(f"hn api: item {item_id} has no comments")

        async def fetch_comment(kid_id: int) -> dict:
            try:
                r = await self._get(f"{_HN_API}/item/{kid_id}.json")
                return r.json() or {}
            except Exception:
                return {}

        items = await asyncio.gather(*[fetch_comment(k) for k in kids[: self._top]])
        return parse_hn_api_items(list(items), top_n=self._top)

    async def _fetch_via_hckrnws(self, item_id: str) -> str:
        """Fallback 1: hckrnws reader page (server-side rendered)."""
        resp = await self._get(f"{self._base}/stories/{item_id}")
        return parse_hn_thread(resp.text, top_n=self._top)

    async def _fetch_via_official(self, item_id: str) -> str:
        """Fallback 2: official HN item page."""
        resp = await self._get(f"{_HN_OFFICIAL}/item?id={item_id}")
        return parse_hn_official(resp.text, top_n=self._top)

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
            ("hckrnws", self._fetch_via_hckrnws),
            ("official", self._fetch_via_official),
        ]
        last_exc: Exception | None = None
        for source, fn in attempts:
            try:
                result = await fn(item_id)
                preview = result[:200].replace("\n", " ")
                log.bind(
                    item_id=item_id, source=source,
                    chars=len(result), preview=preview,
                ).debug("hn_comments.fetched")
                return result
            except Exception as exc:
                log.bind(item_id=item_id, source=source, error=str(exc)).debug(
                    "hn_comments.attempt_failed"
                )
                last_exc = exc

        raise EnricherParseError(
            f"hn_comments: all sources failed for {item_id}: {last_exc}"
        )
