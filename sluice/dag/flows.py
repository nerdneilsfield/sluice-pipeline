from __future__ import annotations

from pathlib import Path
from typing import Any

from prefect import flow

from sluice.dag.loader import load_task
from sluice.dag.rotation.lock import RotationLock, check_rotation_fast_path
from sluice.dag.rotation.orchestrator import RotationResult, rotate_task
from sluice.dag.run_pipeline import RunResult, ensure_dynamic_tables, run_pipeline
from sluice.dag.sluice_config import load_sluice_config
from sluice.dag.writer import WriterThread


def _serialize_run_result(result: RunResult) -> dict[str, Any]:
    return {
        "pipeline_name": result.pipeline_name,
        "run_key": result.run_key,
        "status": result.status,
        "items_in": result.items_in,
        "items_out": result.items_out,
        "error_msg": result.error_msg,
    }


def _serialize_rotation_result(result: RotationResult) -> dict[str, Any]:
    return {
        "task_name": result.task_name,
        "status": result.status,
        "carryover_count": result.carryover_count,
        "archive_count": result.archive_count,
        "archive_path": result.archive_path,
        "error_msg": result.error_msg,
    }


@flow(name="sluice-run-pipeline")
async def run_pipeline_flow(
    task: str,
    pipeline: str,
    config_dir: str = "./configs",
) -> dict[str, Any]:
    cfg = load_task(Path(config_dir) / "tasks" / task)
    lock = RotationLock(cfg.db_path)
    try:
        lock.acquire()
    except RuntimeError:
        fast = check_rotation_fast_path(cfg, pipeline)
        if fast is not None:
            return _serialize_run_result(fast)
        return {"status": "lock_busy", "pipeline_name": pipeline}

    try:
        await ensure_dynamic_tables(cfg)
        writer = WriterThread(
            active_path=cfg.db_path,
            seen_path=cfg.seen_db,
            hold_rotation_lock=False,
        )
        await writer.start()
        try:
            result = await run_pipeline(cfg, pipeline, writer=writer)
        finally:
            await writer.stop()
    finally:
        lock.release()

    return _serialize_run_result(result)


@flow(name="sluice-rotate-task")
async def rotate_task_flow(
    task: str,
    config_dir: str = "./configs",
    sluice_config: str | None = None,
) -> dict[str, Any]:
    cfg = load_task(Path(config_dir) / "tasks" / task)
    sluice_cfg = load_sluice_config(Path(sluice_config) if sluice_config else None)
    result = await rotate_task(cfg, config=sluice_cfg)
    return _serialize_rotation_result(result)
