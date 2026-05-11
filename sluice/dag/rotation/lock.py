from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sluice.dag.config import TaskConfig
    from sluice.dag.run_pipeline import RunResult


def _is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class RotationLock:
    _inprocess_holds: set[str] = set()

    def __init__(self, active_path: str | Path, *, stale_after_seconds: int = 3 * 3600):
        self._lock_file = Path(str(active_path) + ".rotation.lock").resolve()
        self._stale_after = stale_after_seconds
        self._holding = False

    @property
    def _key(self) -> str:
        return str(self._lock_file)

    def is_held(self) -> bool:
        if self._holding or self._key in RotationLock._inprocess_holds:
            return True
        if not self._lock_file.exists():
            return False
        try:
            text = self._lock_file.read_text().strip()
            pid_str, ts_str = text.split("\t", 1)
            pid = int(pid_str)
            datetime.fromisoformat(ts_str)
        except (OSError, ValueError):
            try:
                age = datetime.now(timezone.utc).timestamp() - self._lock_file.stat().st_mtime
                if age > self._stale_after:
                    self._lock_file.unlink(missing_ok=True)
                    return False
            except OSError:
                pass
            return True
        if not _is_pid_alive(pid):
            self._lock_file.unlink(missing_ok=True)
            return False
        return True

    def acquire(self) -> None:
        if self._key in RotationLock._inprocess_holds:
            raise RuntimeError(f"rotation lock already held: {self._lock_file}")
        if self._lock_file.exists() and not self.is_held():
            pass
        self._lock_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self._lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError as exc:
            raise RuntimeError(f"rotation lock already held: {self._lock_file}") from exc
        try:
            payload = f"{os.getpid()}\t{datetime.now(timezone.utc).isoformat()}"
            os.write(fd, payload.encode("utf-8"))
        finally:
            os.close(fd)
        RotationLock._inprocess_holds.add(self._key)
        self._holding = True

    def release(self) -> None:
        if not self._holding:
            return
        self._lock_file.unlink(missing_ok=True)
        RotationLock._inprocess_holds.discard(self._key)
        self._holding = False

    def __enter__(self) -> RotationLock:
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


def check_rotation_fast_path(task: TaskConfig, pipeline_name: str) -> RunResult | None:
    from sluice.dag.run_pipeline import RunResult

    if RotationLock(task.db_path).is_held():
        return RunResult(
            pipeline_name=pipeline_name,
            run_key="",
            status="skipped_rotation",
            items_in=0,
            items_out=0,
            error_msg=f"rotation in progress for {task.name!r}",
        )
    return None
