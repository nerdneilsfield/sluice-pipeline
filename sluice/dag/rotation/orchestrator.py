from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sluice.dag.config import TaskConfig
from sluice.dag.db import open_active_db
from sluice.dag.rotation.copy import copy_carryover
from sluice.dag.rotation.drain import drain_running_pipelines
from sluice.dag.rotation.gate import close_gate, open_gate
from sluice.dag.rotation.lock import RotationLock
from sluice.dag.rotation.new_active import build_new_active
from sluice.dag.rotation.partition import partition_items
from sluice.dag.rotation.seen_reconcile import reconcile_seen_for_archive
from sluice.dag.rotation.swap import swap_active_file
from sluice.dag.sluice_config import SluiceConfig
from sluice.dag.validator import validate_task

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RotationResult:
    task_name: str
    status: str
    carryover_count: int
    archive_count: int
    archive_path: str | None
    error_msg: str | None = None


def _unlink_db_family(path: str) -> None:
    base = Path(path)
    base.unlink(missing_ok=True)
    Path(path + "-wal").unlink(missing_ok=True)
    Path(path + "-shm").unlink(missing_ok=True)


async def rotate_task(
    task: TaskConfig,
    *,
    config: SluiceConfig,
    now: datetime | None = None,
) -> RotationResult:
    validate_task(task)
    now = now or datetime.now(timezone.utc)
    anchor_iso = now.isoformat()

    lock = RotationLock(task.db_path)
    try:
        lock.acquire()
    except RuntimeError as exc:
        return RotationResult(
            task_name=task.name,
            status="lock_busy",
            carryover_count=0,
            archive_count=0,
            archive_path=None,
            error_msg=str(exc),
        )

    swap_succeeded = False
    try:
        await close_gate(task.db_path)
        drained = await drain_running_pipelines(
            task.db_path,
            task.name,
            timeout_seconds=config.rotation.drain_timeout_seconds,
        )
        if not drained:
            await open_gate(task.db_path)
            return RotationResult(
                task_name=task.name,
                status="drain_timeout",
                carryover_count=0,
                archive_count=0,
                archive_path=None,
                error_msg="drain timed out; will retry next cycle",
            )

        async with open_active_db(task.db_path) as conn_old:
            partition = await partition_items(conn_old, task)

        if not partition.carryover_ids and not partition.archive_ids:
            await open_gate(task.db_path)
            return RotationResult(
                task_name=task.name,
                status="no_op",
                carryover_count=0,
                archive_count=0,
                archive_path=None,
            )

        new_path = str(Path(str(task.db_path) + ".new"))
        _unlink_db_family(new_path)

        try:
            await build_new_active(new_path, task)
            async with open_active_db(new_path) as conn_new:
                await copy_carryover(
                    conn_new,
                    old_path=task.db_path,
                    partition=partition,
                    task=task,
                )
                await conn_new.commit()

            await reconcile_seen_for_archive(
                old_active_path=task.db_path,
                seen_path=task.seen_db,
                archive_ids=partition.archive_ids,
                archived_at=anchor_iso,
            )

            archive_path = await swap_active_file(
                old_active_path=task.db_path,
                new_active_path=new_path,
                archive_dir=config.rotation.archive_dir,
                task_name=task.name,
                cycle_anchor_iso=anchor_iso,
            )
            swap_succeeded = True
        except Exception:
            # If this runs after swap_active_file installed the new DB, new_path is
            # already gone. _unlink_db_family is missing-ok by design.
            _unlink_db_family(new_path)
            raise

        return RotationResult(
            task_name=task.name,
            status="rotated",
            carryover_count=len(partition.carryover_ids),
            archive_count=len(partition.archive_ids),
            archive_path=archive_path,
        )
    except Exception:
        if not swap_succeeded and Path(task.db_path).exists():
            try:
                await open_gate(task.db_path)
            except Exception:
                logger.error(
                    "failed to reopen rotation_gate; manual reset is required",
                    exc_info=True,
                )
        elif not swap_succeeded:
            logger.error(
                "rotation failed after active DB path disappeared; manual recovery is required"
            )
        raise
    finally:
        lock.release()
