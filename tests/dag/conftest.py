from __future__ import annotations

from pathlib import Path

import pytest

from sluice.dag import db


@pytest.fixture
def tmp_task_dir(tmp_path: Path) -> Path:
    d = tmp_path / "task"
    d.mkdir()
    return d


@pytest.fixture
async def active_db(tmp_path: Path):
    async with db.open_active_db(str(tmp_path / "active.db")) as conn:
        yield conn


@pytest.fixture
async def seen_db(tmp_path: Path):
    async with db.open_seen_db(str(tmp_path / "seen.db")) as conn:
        yield conn
