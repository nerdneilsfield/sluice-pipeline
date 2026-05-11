from __future__ import annotations

from pathlib import Path

from sluice.dag.config import TaskConfig
from sluice.dag.db import open_active_db
from sluice.dag.dynamic_tables import migrate_task_dynamic_tables


async def build_new_active(new_path: str | Path, task: TaskConfig) -> None:
    path = Path(new_path)
    if path.exists():
        raise FileExistsError(f"refuse to overwrite existing file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    async with open_active_db(str(path)) as conn:
        await migrate_task_dynamic_tables(conn, task)
