import pytest

from sluice.state.cache import UrlCacheStore
from sluice.state.db import open_db


@pytest.mark.asyncio
async def test_size_cap_evicts_oldest_when_check_triggers(tmp_path):
    async with open_db(tmp_path / "s.db") as db:
        store = UrlCacheStore(db, max_rows=10, check_every_n=1)
        for i in range(22):
            await store.set(f"http://x/{i}", "trafilatura", "md", ttl_seconds=3600)
        cur = await db.execute("SELECT COUNT(*) FROM url_cache")
        n = (await cur.fetchone())[0]
        assert n <= 11
        cur = await db.execute("SELECT url FROM url_cache ORDER BY url")
        urls = {r[0] for r in await cur.fetchall()}
        assert "http://x/0" not in urls


@pytest.mark.asyncio
async def test_size_cap_skips_when_under_threshold(tmp_path):
    async with open_db(tmp_path / "s.db") as db:
        store = UrlCacheStore(db, max_rows=100, check_every_n=1)
        for i in range(5):
            await store.set(f"http://x/{i}", "trafilatura", "md", ttl_seconds=3600)
        cur = await db.execute("SELECT COUNT(*) FROM url_cache")
        assert (await cur.fetchone())[0] == 5
