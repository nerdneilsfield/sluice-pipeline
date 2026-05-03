from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def tmp_task_dir(tmp_path: Path) -> Path:
    d = tmp_path / "task"
    d.mkdir()
    return d
