from datetime import datetime, timezone, timedelta
from typing import AsyncIterator
import feedparser
import httpx
from sluice.core.item import Item
from sluice.url_canon import canonical_url
from sluice.sources.base import register_source


@register_source("rss")
class RssSource:
    def __init__(
        self,
        *,
        url: str,
        pipeline_id: str,
        source_id: str,
        tag: str | None = None,
        name: str | None = None,
        timeout: float = 30.0,
    ):
        self.url = url
        self.pipeline_id = pipeline_id
        self.source_id = source_id
        self.tags = [tag] if tag else []
        self.name = name or source_id
        self.timeout = timeout

    async def fetch(self, window_start: datetime, window_end: datetime) -> AsyncIterator[Item]:
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.get(self.url)
            r.raise_for_status()
            text = r.text
        feed = feedparser.parse(text)
        future_cap = window_end + timedelta(hours=1)
        for e in feed.entries:
            published = self._parse_date(e)
            if published is not None:
                if published > future_cap:
                    continue
                if published < window_start or published > window_end:
                    continue
            yield Item(
                source_id=self.source_id,
                pipeline_id=self.pipeline_id,
                guid=getattr(e, "id", None) or getattr(e, "guid", None) or None,
                url=canonical_url(getattr(e, "link", "")),
                title=getattr(e, "title", "") or "",
                published_at=published,
                raw_summary=getattr(e, "summary", None) or getattr(e, "description", None),
                tags=list(self.tags),
            )

    @staticmethod
    def _parse_date(e) -> datetime | None:
        for attr in ("published_parsed", "updated_parsed"):
            t = getattr(e, attr, None)
            if t:
                return datetime(*t[:6], tzinfo=timezone.utc)
        return None
