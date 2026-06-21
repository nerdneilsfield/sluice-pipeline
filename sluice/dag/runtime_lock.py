from __future__ import annotations

import errno
import fcntl
import os
from pathlib import Path

from sluice.core.errors import SluiceError

_LOCK_FILENAME = "runtime-core.lock"


class RuntimeLockHeldError(SluiceError):
    """Raised when a runtime lock cannot be acquired because it is held."""


class RuntimeLock:
    """Cross-process shared/exclusive runtime lock.

    Backed by ``fcntl.flock(LOCK_SH|LOCK_EX|LOCK_NB)`` on a stable file under
    ``<task-dir>/.sluice/runtime-core.lock``. Shared mode is held for the full
    lifetime of a runner or admin operation. Exclusive mode is held for the
    full duration of upgrade / restore / recover.

    The lock is process-local: a single process may not hold both shared and
    exclusive at the same time. Reentrant acquisition of the same mode within
    one process is not supported; convert sub-operations to a single acquire.
    """

    def __init__(self, task_dir: Path | str) -> None:
        self._lock_path = Path(task_dir) / ".sluice" / _LOCK_FILENAME
        self._fd: int | None = None
        self._mode: str | None = None

    @property
    def path(self) -> Path:
        return self._lock_path

    def _ensure_parent(self) -> None:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)

    def acquire_shared(self) -> None:
        self._acquire(fcntl.LOCK_SH | fcntl.LOCK_NB, "shared")

    def acquire_exclusive(self) -> None:
        self._acquire(fcntl.LOCK_EX | fcntl.LOCK_NB, "exclusive")

    def _acquire(self, flag: int, mode: str) -> None:
        if self._fd is not None:
            raise RuntimeLockHeldError(
                f"runtime lock already acquired as {self._mode!r} in this process"
            )
        self._ensure_parent()
        fd = os.open(str(self._lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, flag)
        except OSError as exc:
            os.close(fd)
            if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                raise RuntimeLockHeldError(
                    f"runtime lock at {self._lock_path} is held by another process"
                ) from exc
            raise
        self._fd = fd
        self._mode = mode

    def release(self) -> None:
        if self._fd is None:
            return
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None
        self._mode = None

    def __enter__(self) -> "RuntimeLock":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
