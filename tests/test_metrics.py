import pytest

from sluice.metrics import RUN_DURATION_BUCKETS, SluiceCollector
from sluice.state.db import open_db


@pytest.mark.asyncio
async def test_collector_emits_run_total(tmp_path):
    db_path = tmp_path / "s.db"
    async with open_db(db_path) as db:
        await db.execute(
            "INSERT INTO run_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "p1",
                "p1/x",
                "2026-04-29T00:00:00+00:00",
                "2026-04-29T00:00:30+00:00",
                "success",
                10,
                9,
                5,
                0.01,
                None,
            ),
        )
        await db.commit()
    collector = SluiceCollector(str(db_path))
    families = list(collector.collect())
    names = [f.name for f in families]
    assert "sluice_run" in names
    assert "sluice_run_duration_seconds" in names


def test_run_duration_buckets_constant():
    assert RUN_DURATION_BUCKETS[0] == 1.0
    assert RUN_DURATION_BUCKETS[-1] == float("inf")
