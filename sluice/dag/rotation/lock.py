from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from sluice.dag.config import TaskConfig
    from sluice.dag.run_pipeline import RunResult

ProcessIdentity: TypeAlias = tuple[str, str]


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


def _read_proc_starttime(pid: int) -> str | None:
    try:
        text = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    comm_end = text.rfind(") ")
    if comm_end == -1:
        return None
    fields = text[comm_end + 2 :].split()
    if len(fields) <= 19:
        return None
    return fields[19]


def _read_boot_id() -> str | None:
    try:
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return None
    return boot_id or None


def _get_process_identity(pid: int) -> ProcessIdentity | None:
    starttime = _read_proc_starttime(pid)
    boot_id = _read_boot_id()
    if starttime is None or boot_id is None:
        return None
    return starttime, boot_id


def _parse_lock_text(text: str) -> tuple[int, ProcessIdentity | None]:
    parts = text.split("\t")
    if len(parts) not in {2, 4}:
        raise ValueError("invalid rotation lock format")
    pid = int(parts[0])
    datetime.fromisoformat(parts[1])
    if len(parts) == 2:
        return pid, None
    starttime, boot_id = parts[2], parts[3]
    if not starttime or not boot_id:
        raise ValueError("invalid rotation lock identity")
    return pid, (starttime, boot_id)


def _format_lock_payload(pid: int, timestamp: datetime) -> str:
    identity = _get_process_identity(pid)
    if identity is None:
        return f"{pid}\t{timestamp.isoformat()}"
    starttime, boot_id = identity
    return f"{pid}\t{timestamp.isoformat()}\t{starttime}\t{boot_id}"


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
            pid, recorded_identity = _parse_lock_text(text)
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
        current_identity = _get_process_identity(pid)
        if (
            recorded_identity is not None
            and current_identity is not None
            and current_identity != recorded_identity
        ):
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
            payload = _format_lock_payload(os.getpid(), datetime.now(timezone.utc))
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
