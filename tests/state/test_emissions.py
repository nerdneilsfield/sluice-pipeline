import pytest

from sluice.state.db import open_db
from sluice.state.emissions import EmissionStore


@pytest.mark.asyncio
async def test_lookup_insert(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        assert await e.lookup("p", "p/2026-04-28", "notion_main") is None
        await e.insert("p", "p/2026-04-28", "notion_main", "notion", "page-abc")
        rec = await e.lookup("p", "p/2026-04-28", "notion_main")
        assert rec.external_id == "page-abc"
        assert rec.sink_type == "notion"


@pytest.mark.asyncio
async def test_two_sinks_same_type_no_collision(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        await e.insert("p", "rk", "fm_a", "file_md", "/a.md")
        await e.insert("p", "rk", "fm_b", "file_md", "/b.md")
        a = await e.lookup("p", "rk", "fm_a")
        b = await e.lookup("p", "rk", "fm_b")
        assert a.external_id == "/a.md" and b.external_id == "/b.md"
