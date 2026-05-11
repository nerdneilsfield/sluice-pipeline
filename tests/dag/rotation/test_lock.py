from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from sluice.dag.config import DeploymentsDef, TaskConfig, WindowDef
from sluice.dag.rotation.lock import RotationLock, check_rotation_fast_path


def _path(tmp_path: Path) -> Path:
    return tmp_path / "x.db"


def _task(tmp_path: Path) -> TaskConfig:
    return TaskConfig(
        name="t",
        db_path=str(_path(tmp_path)),
        seen_db=str(tmp_path / "seen.db"),
        enabled=True,
        timezone="UTC",
        window=WindowDef(anchor="0 8 * * *", length="24h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[],
        sinks=[],
        stages=[],
        pipelines=[],
    )


def test_lock_not_held_initially(tmp_path):
    lock = RotationLock(_path(tmp_path))
    assert not lock.is_held()


def test_acquire_release_cycle(tmp_path):
    lock = RotationLock(_path(tmp_path))
    lock.acquire()
    assert lock.is_held()
    lock.release()
    assert not lock.is_held()


def test_double_acquire_raises(tmp_path):
    lock = RotationLock(_path(tmp_path))
    lock.acquire()
    try:
        with pytest.raises(RuntimeError, match="already held"):
            lock.acquire()
    finally:
        lock.release()


def test_lock_inprocess_set_blocks_second_instance(tmp_path):
    db = _path(tmp_path)
    first = RotationLock(db)
    second = RotationLock(db)
    first.acquire()
    try:
        assert second.is_held()
        with pytest.raises(RuntimeError, match="already held"):
            second.acquire()
    finally:
        first.release()
    assert not second.is_held()


def test_dead_pid_lock_is_cleared(tmp_path):
    db = _path(tmp_path)
    lock_file = Path(str(db) + ".rotation.lock")
    lock_file.write_text("99999999\t2026-01-01T00:00:00+00:00")

    lock = RotationLock(db, stale_after_seconds=0)

    assert not lock.is_held()
    assert not lock_file.exists()


def test_lock_held_by_live_process_blocks(tmp_path):
    db = _path(tmp_path)
    lock_file = Path(str(db) + ".rotation.lock")
    lock_file.write_text(f"{os.getpid()}\t2099-01-01T00:00:00+00:00")

    lock = RotationLock(db, stale_after_seconds=999_999)

    assert lock.is_held()


def test_malformed_recent_lock_is_held(tmp_path):
    db = _path(tmp_path)
    lock_file = Path(str(db) + ".rotation.lock")
    lock_file.write_text("truncated")

    lock = RotationLock(db, stale_after_seconds=999_999)

    assert lock.is_held()
    assert lock_file.exists()


def test_malformed_stale_lock_is_cleared(tmp_path):
    db = _path(tmp_path)
    lock_file = Path(str(db) + ".rotation.lock")
    lock_file.write_text("truncated")
    os.utime(lock_file, (1, 1))

    lock = RotationLock(db, stale_after_seconds=0)

    assert not lock.is_held()
    assert not lock_file.exists()


def test_acquire_translates_o_excl_race(tmp_path):
    db = _path(tmp_path)
    lock_file = Path(str(db) + ".rotation.lock")
    lock_file.write_text(f"{os.getpid()}\t{datetime.now(timezone.utc).isoformat()}")

    lock = RotationLock(db, stale_after_seconds=999_999)

    with pytest.raises(RuntimeError, match="already held"):
        lock.acquire()


def test_context_manager_releases_on_exception(tmp_path):
    lock = RotationLock(_path(tmp_path))

    with pytest.raises(ValueError):
        with lock:
            raise ValueError("boom")

    assert not lock.is_held()


def test_check_rotation_fast_path_returns_populated_run_result(tmp_path):
    task = _task(tmp_path)
    lock = RotationLock(task.db_path)
    lock.acquire()
    try:
        result = check_rotation_fast_path(task, "P")
    finally:
        lock.release()

    assert result is not None
    assert result.pipeline_name == "P"
    assert result.run_key == ""
    assert result.status == "skipped_rotation"
    assert result.items_in == 0
    assert result.items_out == 0
    assert result.error_msg == "rotation in progress for 't'"
