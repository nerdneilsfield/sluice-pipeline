import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import aiosqlite


def _hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


@dataclass
class CachedExtraction:
    url: str
    fetcher: str
    markdown: str


class UrlCacheStore:
    def __init__(
        self,
        db: aiosqlite.Connection,
        max_rows: int = 50000,
        check_every_n: int = 256,
    ):
        self.db = db
        self._max_rows = max_rows
        self._check_every_n = check_every_n
        self._puts_until_check = check_every_n + 1

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
        await self._maybe_evict()

    async def _maybe_evict(self) -> None:
        self._puts_until_check = max(0, self._puts_until_check - 1)
        if self._puts_until_check > 0:
            return
        self._puts_until_check = self._check_every_n
        async with self.db.execute("SELECT COUNT(*) FROM url_cache") as cur:
            n = (await cur.fetchone())[0]
        threshold = int(self._max_rows * 1.1)
        if n <= threshold:
            return
        excess = n - self._max_rows
        await self.db.execute(
            "DELETE FROM url_cache WHERE url_hash IN ("
            "  SELECT url_hash FROM url_cache "
            "  ORDER BY fetched_at ASC LIMIT ?)",
            (excess,),
        )
        await self.db.commit()
