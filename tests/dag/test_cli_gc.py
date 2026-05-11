from __future__ import annotations

import shutil
from pathlib import Path

from typer.testing import CliRunner

from tests.dag.test_cli import FIXTURE, _cli_app


def _copy_task(tmp_path: Path) -> Path:
    config_dir = tmp_path / "configs"
    (config_dir / "tasks").mkdir(parents=True)
    task_dir = config_dir / "tasks" / "example"
    shutil.copytree(FIXTURE, task_dir)
    text = (task_dir / "task.toml").read_text()
    text = text.replace(
        'db_path  = "data/example.db"',
        f'db_path  = "{tmp_path}/active.db"',
    )
    text = text.replace(
        'seen_db  = "data/example_seen.db"',
        f'seen_db  = "{tmp_path}/seen.db"',
    )
    (task_dir / "task.toml").write_text(text)
    return config_dir


def test_gc_rotate_dry_run_empty_task(tmp_path: Path):
    config_dir = _copy_task(tmp_path)
    result = CliRunner().invoke(
        _cli_app(),
        ["gc-rotate", "-t", "example", "--config-dir", str(config_dir), "--dry-run"],
    )
    assert result.exit_code == 0, result.output
    assert '"status": "dry_run"' in result.output
    assert '"carryover_count": 0' in result.output
    assert '"archive_count": 0' in result.output


def test_gc_rotate_dry_run_reports_lock_busy(tmp_path: Path):
    config_dir = _copy_task(tmp_path)
    from sluice.dag.rotation.lock import RotationLock

    lock = RotationLock(tmp_path / "active.db")
    lock.acquire()
    try:
        result = CliRunner().invoke(
            _cli_app(),
            ["gc-rotate", "-t", "example", "--config-dir", str(config_dir), "--dry-run"],
        )
    finally:
        lock.release()

    assert result.exit_code == 0, result.output
    assert '"status": "lock_busy"' in result.output


def test_gc_rotate_real_empty_task(tmp_path: Path):
    config_dir = _copy_task(tmp_path)
    result = CliRunner().invoke(
        _cli_app(),
        ["gc-rotate", "-t", "example", "--config-dir", str(config_dir)],
    )
    assert result.exit_code == 0, result.output
    assert '"status": "no_op"' in result.output
