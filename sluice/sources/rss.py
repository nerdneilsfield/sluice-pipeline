from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

import feedparser
import httpx
from fake_useragent import UserAgent

from sluice.core.item import Attachment, Item
from sluice.logging_setup import get_logger
from sluice.sources.base import register_source
from sluice.url_canon import canonical_url

log = get_logger(__name__)
_ua = UserAgent()
_RSS_UA = "Mozilla/5.0 (compatible; Sluice RSS Fetcher/1.0; +https://github.com/sluice/sluice)"


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
        log.bind(source_id=self.source_id, url=self.url, timeout=self.timeout).debug(
            "rss.fetch_started"
        )
        try:
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as c:
                r = await c.get(self.url, headers=headers)
                r.raise_for_status()
                text = r.text
        except Exception:
            log.bind(source_id=self.source_id, url=self.url).exception("rss.fetch_failed")
            return
        feed = feedparser.parse(text)
        entries = list(feed.entries)
        skipped_future = 0
        skipped_window = 0
        emitted = 0
        future_cap = window_end + timedelta(hours=1)
        for e in entries:
            published = self._parse_date(e)
            if published is not None:
                if published > future_cap:
                    skipped_future += 1
                    continue
                if published < window_start or published > window_end:
                    skipped_window += 1
                    continue
            emitted += 1
            yield Item(
                source_id=self.source_id,
                pipeline_id=self.pipeline_id,
                guid=getattr(e, "id", None) or getattr(e, "guid", None) or None,
                url=canonical_url(getattr(e, "link", "")),
                title=getattr(e, "title", "") or "",
                published_at=published,
                raw_summary=self._raw_summary(e),
                attachments=self._attachments(e),
                tags=list(self.tags),
            )
        log.bind(
            source_id=self.source_id,
            url=self.url,
            entries=len(entries),
            emitted=emitted,
            skipped_future=skipped_future,
            skipped_window=skipped_window,
        ).info("rss.fetch_done")

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

    @staticmethod
    def _raw_summary(e) -> str | None:
        summary = getattr(e, "summary", None) or getattr(e, "description", None)
        if summary:
            return summary
        for content in getattr(e, "content", []) or []:
            if isinstance(content, dict):
                value = content.get("value")
            else:
                value = getattr(content, "value", None)
            if value:
                return str(value)
        return None
