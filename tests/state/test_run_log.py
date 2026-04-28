import pytest
from sluice.state.db import open_db
from sluice.state.run_log import RunLog


@pytest.mark.asyncio
async def test_lifecycle(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        r = RunLog(db)
        await r.start("p", "p/2026-04-28")
        await r.update_stats(
            "p", "p/2026-04-28", items_in=10, items_out=8, llm_calls=12, est_cost_usd=0.04
        )
        await r.finish("p", "p/2026-04-28", status="success")
        rows = await r.list("p")
        assert rows[0]["status"] == "success"
        assert rows[0]["items_out"] == 8
