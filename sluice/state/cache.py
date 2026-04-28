from dataclasses import dataclass
import hashlib
from datetime import datetime, timezone, timedelta
import aiosqlite


def _hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


@dataclass
class CachedExtraction:
    url: str
    fetcher: str
    markdown: str


class UrlCacheStore:
    def __init__(self, db: aiosqlite.Connection):
        self.db = db

    async def get(self, url: str) -> CachedExtraction | None:
        now = datetime.now(timezone.utc).isoformat()
        async with self.db.execute(
            "SELECT url, fetcher, markdown FROM url_cache WHERE url_hash=? AND expires_at > ?",
            (_hash(url), now),
        ) as cur:
            row = await cur.fetchone()
        return CachedExtraction(*row) if row else None

    async def set(self, url: str, fetcher: str, markdown: str, ttl_seconds: int):
        now = datetime.now(timezone.utc)
        await self.db.execute(
            "INSERT OR REPLACE INTO url_cache "
            "(url_hash, url, fetcher, markdown, fetched_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                _hash(url),
                url,
                fetcher,
                markdown,
                now.isoformat(),
                (now + timedelta(seconds=ttl_seconds)).isoformat(),
            ),
        )
        await self.db.commit()
