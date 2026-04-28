import pytest
from datetime import datetime, timezone, timedelta
from sluice.state.db import open_db
from sluice.state.cache import UrlCacheStore

@pytest.mark.asyncio
async def test_get_set_expire(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        c = UrlCacheStore(db)
        await c.set("https://x/a", "trafilatura", "# md", ttl_seconds=3600)
        hit = await c.get("https://x/a")
        assert hit and hit.markdown == "# md"
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        await db.execute("UPDATE url_cache SET expires_at=?", (past,))
        await db.commit()
        assert await c.get("https://x/a") is None
