from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)

_SIDECAR_DIRNAME = ".sluice"
_MANIFEST_FILENAME = "rotation.manifest.json"
_FAILED_FILENAME = "rotation.failed"
_LOCK_FILENAME = "rotation.lock"


class RotationPhase(str, Enum):
    COPYING = "copying"
    COPIED = "copied"
    ARCHIVING_OLD = "archiving_old"
    INSTALLING_NEW = "installing_new"
    INSTALLED = "installed"
    SEEN_RECONCILING = "seen_reconciling"
    SEEN_RECONCILED = "seen_reconciled"
    GZIPPING = "gzipping"
    DONE = "done"


@dataclass(frozen=True)
class ManifestRecord:
    phase: RotationPhase
    recorded_at: str
    payload: dict

    def to_dict(self) -> dict:
        return {
            "phase": self.phase.value,
            "recorded_at": self.recorded_at,
            "payload": self.payload,
        }


def sidecar_dir(task_dir: Path | str) -> Path:
    return Path(task_dir) / _SIDECAR_DIRNAME / "rotation"


def manifest_path(task_dir: Path | str) -> Path:
    return sidecar_dir(task_dir) / _MANIFEST_FILENAME


def failed_marker_path(task_dir: Path | str) -> Path:
    return sidecar_dir(task_dir) / _FAILED_FILENAME


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _fsync_file(path: Path) -> None:
    """Open, fsync, close. Raises OSError on failure."""
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_parent(path: Path) -> None:
    parent = path.parent
    fd = os.open(str(parent), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_json(path: Path, payload: dict) -> None:
    """Write JSON to ``path`` atomically.

    Sequence: write temp → fsync temp → os.replace → fsync parent dir.
    If any step fails, the temp is removed and the original is untouched.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        _fsync_file(tmp)
        os.replace(tmp, path)
        _fsync_parent(path)
    except BaseException:
        try:
            tmp.unlink(missing_ok=True)
        except OSError as cleanup_exc:
            logger.warning("could not clean up manifest tmp %s: %s", tmp, cleanup_exc)
        raise


def append_phase(
    task_dir: Path | str,
    phase: RotationPhase,
    *,
    payload: dict | None = None,
) -> None:
    """Append a phase record to the sidecar manifest.

    Reads the existing manifest (if any), appends the new phase, and writes
    atomically. The manifest is the source of truth for crash recovery.
    """
    path = manifest_path(task_dir)
    existing = read_manifest(task_dir) or {"phases": []}
    record = ManifestRecord(
        phase=phase,
        recorded_at=_utc_now_iso(),
        payload=payload or {},
    )
    existing["phases"].append(record.to_dict())
    atomic_write_json(path, existing)


def read_manifest(task_dir: Path | str) -> dict | None:
    path = manifest_path(task_dir)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"corrupt rotation sidecar at {path}: {exc}"
        ) from exc


def clear_manifest(task_dir: Path | str) -> None:
    path = manifest_path(task_dir)
    path.unlink(missing_ok=True)


def mark_failed(
    task_dir: Path | str, *, reason: str, details: dict | None = None
) -> None:
    """Write a .failed marker so startup refuses runner actions until recovery."""
    marker = failed_marker_path(task_dir)
    payload = {
        "reason": reason,
        "details": details or {},
        "recorded_at": _utc_now_iso(),
    }
    atomic_write_json(marker, payload)


def clear_failed(task_dir: Path | str) -> None:
    failed_marker_path(task_dir).unlink(missing_ok=True)


def has_failed_marker(task_dir: Path | str) -> bool:
    return failed_marker_path(task_dir).exists()


def latest_phase(task_dir: Path | str) -> RotationPhase | None:
    manifest = read_manifest(task_dir)
    if manifest is None:
        return None
    phases = manifest.get("phases") or []
    if not phases:
        return None
    try:
        return RotationPhase(phases[-1]["phase"])
    except (KeyError, ValueError) as exc:
        raise RuntimeError(f"corrupt manifest: bad phase entry {phases[-1]!r}") from exc


def fsync_db_file(path: Path | str) -> None:
    """fsync a DB file after writes. Raises OSError on failure."""
    _fsync_file(Path(path))
