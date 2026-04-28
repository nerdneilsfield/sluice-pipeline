from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

import feedparser
import httpx
from fake_useragent import UserAgent

from sluice.core.item import Attachment, Item
from sluice.sources.base import register_source
from sluice.url_canon import canonical_url

_ua = UserAgent()
_RSS_UA = (
    "Mozilla/5.0 (compatible; Sluice RSS Fetcher/1.0; "
    "+https://github.com/sluice/sluice)"
)


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
        headers = {
            "User-Agent": _RSS_UA,
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
        }
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.get(self.url, headers=headers)
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
                attachments=self._attachments(e),
                tags=list(self.tags),
            )

    @staticmethod
    def _parse_date(e) -> datetime | None:
        for attr in ("published_parsed", "updated_parsed"):
            t = getattr(e, attr, None)
            if t:
                return datetime(*t[:6], tzinfo=timezone.utc)
        return None

    @staticmethod
    def _attachments(e) -> list[Attachment]:
        out = []
        for enc in getattr(e, "enclosures", []) or []:
            url = enc.get("href") or enc.get("url")
            if not url:
                continue
            raw_length = enc.get("length")
            try:
                length = int(raw_length) if raw_length not in (None, "") else None
            except (TypeError, ValueError):
                length = None
            out.append(
                Attachment(
                    url=url,
                    mime_type=enc.get("type"),
                    length=length,
                )
            )
        return out
