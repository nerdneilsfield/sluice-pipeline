from __future__ import annotations

import enum
import hashlib
import json
import secrets
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

from sluice.core.errors import UpgradeError
from sluice.dag import db
from sluice.dag.dynamic_tables import upgrade_task_dynamic_tables
from sluice.dag.loader import load_task
from sluice.dag.runtime_lock import RuntimeLock

_MARKER_FILENAME = "runtime-core.json"
_PHASE_MANIFEST_FILENAME = "upgrade-phase.json"
_BACKUP_MANIFEST_FILENAME = "backup_manifest.json"


class UpgradePhase(str, enum.Enum):
    BACKUP_WRITTEN = "backup_written"
    FIXED_MIGRATED = "fixed_migrated"
    DYNAMIC_MIGRATED = "dynamic_migrated"
    MARKER_WRITTEN = "marker_written"

    RESTORE_VALIDATING = "validating"
    RESTORE_RESTORING_ACTIVE = "restoring_active"
    RESTORE_RESTORING_SEEN = "restoring_seen"
    RESTORE_RESTORING_SIDECAR = "restoring_sidecar"
    RESTORE_MARKER_WRITTEN = "marker_written"


@dataclass(frozen=True)
class UpgradeResult:
    ok: bool
    error: str | None = None
    phase: UpgradePhase | None = None


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def read_runtime_marker(task_dir: Path | str) -> dict | None:
    marker_path = Path(task_dir) / ".sluice" / _MARKER_FILENAME
    if not marker_path.exists():
        return None
    try:
        return json.loads(marker_path.read_text())
    except json.JSONDecodeError as exc:
        raise UpgradeError(f"corrupt runtime marker at {marker_path}: {exc}") from exc


def write_runtime_marker(
    task_dir: Path | str,
    *,
    runtime_core: str,
    schema_version: int,
    upgrade_id: str,
    backup_manifest_hash: str,
) -> None:
    marker_path = Path(task_dir) / ".sluice" / _MARKER_FILENAME
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "runtime_core": runtime_core,
        "schema_version": schema_version,
        "upgrade_id": upgrade_id,
        "backup_manifest_hash": backup_manifest_hash,
        "written_at": _utc_now_iso(),
    }
    tmp = marker_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
    tmp.replace(marker_path)


def _read_phase_manifest(task_dir: Path | str) -> list[dict] | None:
    path = Path(task_dir) / ".sluice" / _PHASE_MANIFEST_FILENAME
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise UpgradeError(f"corrupt phase manifest at {path}: {exc}") from exc


def _append_phase_manifest(
    task_dir: Path | str, phase: UpgradePhase, **extra: object
) -> None:
    path = Path(task_dir) / ".sluice" / _PHASE_MANIFEST_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _read_phase_manifest(task_dir) or []
    existing.append({"phase": phase.value, "recorded_at": _utc_now_iso(), **extra})
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(existing, indent=2))
    tmp.replace(path)


def _clear_phase_manifest(task_dir: Path | str) -> None:
    path = Path(task_dir) / ".sluice" / _PHASE_MANIFEST_FILENAME
    if path.exists():
        path.unlink()


class UpgradeService:
    """Orchestrates the v3 upgrade workflow: backup, migrate, mark, restore.

    Each phase is recorded in an upgrade-phase.json sidecar so an interrupted
    upgrade can be recovered safely. The service takes an exclusive runtime
    lock around each top-level operation to forbid concurrent runners/admin.
    """

    def __init__(
        self,
        *,
        task_dir: Path,
        db_path: Path,
        seen_path: Path,
    ) -> None:
        self.task_dir = Path(task_dir)
        self.db_path = Path(db_path)
        self.seen_path = Path(seen_path)
        self._lock = RuntimeLock(self.task_dir)

    # ------------------------------------------------------------------
    # Phase manifest accessors
    # ------------------------------------------------------------------

    def read_phase_manifest(self) -> list[dict] | None:
        return _read_phase_manifest(self.task_dir)

    # ------------------------------------------------------------------
    # dry-run
    # ------------------------------------------------------------------

    async def dry_run(self) -> UpgradeResult:
        """Validate upgrade preconditions without mutating anything."""
        try:
            self._lock.acquire_exclusive()
        except Exception as exc:
            return UpgradeResult(ok=False, error=str(exc))
        try:
            await self._validate_preconditions()
            return UpgradeResult(ok=True)
        except UpgradeError as exc:
            return UpgradeResult(ok=False, error=str(exc))
        finally:
            self._lock.release()

    async def _validate_preconditions(self) -> None:
        if not self.db_path.exists():
            raise UpgradeError(f"active DB not found: {self.db_path}")
        if not self.seen_path.exists():
            raise UpgradeError(f"seen DB not found: {self.seen_path}")
        async with aiosqlite.connect(str(self.db_path)) as conn:
            cur = await conn.execute("PRAGMA integrity_check")
            row = await cur.fetchone()
            if row is None or row[0] != "ok":
                raise UpgradeError(f"active DB integrity check failed: {row}")
        async with aiosqlite.connect(str(self.seen_path)) as conn:
            cur = await conn.execute("PRAGMA integrity_check")
            row = await cur.fetchone()
            if row is None or row[0] != "ok":
                raise UpgradeError(f"seen DB integrity check failed: {row}")
        try:
            load_task(self.task_dir)
        except Exception as exc:
            raise UpgradeError(f"task config invalid: {exc}") from exc

    # ------------------------------------------------------------------
    # backup
    # ------------------------------------------------------------------

    async def backup(self, backup_dir: Path) -> UpgradeResult:
        try:
            self._lock.acquire_exclusive()
        except Exception as exc:
            return UpgradeResult(ok=False, error=str(exc))
        try:
            backup_dir = Path(backup_dir)
            backup_dir.mkdir(parents=True, exist_ok=True)
            await self._validate_preconditions()

            # Checkpoint active DB so WAL is merged into the main file before copy.
            async with aiosqlite.connect(str(self.db_path)) as conn:
                await conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                await conn.commit()

            active_dest = backup_dir / "active.db"
            seen_dest = backup_dir / "seen.db"
            shutil.copyfile(self.db_path, active_dest)
            shutil.copyfile(self.seen_path, seen_dest)

            manifest = {
                "task_dir": str(self.task_dir),
                "created_at": _utc_now_iso(),
                "active": {
                    "path": str(active_dest.name),
                    "hash": _sha256(active_dest),
                    "schema": await self._read_schema_version(active_dest),
                },
                "seen": {
                    "path": str(seen_dest.name),
                    "hash": _sha256(seen_dest),
                    "schema": await self._read_schema_version(seen_dest),
                },
                "restore_instructions": (
                    "Stop all runners, then run: "
                    "sluice upgrade v3 --task <T> --restore <this-backup-dir>"
                ),
            }
            manifest_path = backup_dir / _BACKUP_MANIFEST_FILENAME
            manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))

            _clear_phase_manifest(self.task_dir)
            _append_phase_manifest(
                self.task_dir,
                UpgradePhase.BACKUP_WRITTEN,
                backup_dir=str(backup_dir),
                manifest_hash=_sha256(manifest_path),
            )
            return UpgradeResult(ok=True, phase=UpgradePhase.BACKUP_WRITTEN)
        except UpgradeError as exc:
            return UpgradeResult(ok=False, error=str(exc))
        finally:
            self._lock.release()

    async def _read_schema_version(self, path: Path) -> str:
        async with aiosqlite.connect(str(path)) as conn:
            try:
                cur = await conn.execute("SELECT MAX(version) FROM schema_version")
                row = await cur.fetchone()
                if row and row[0] is not None:
                    return f"v{row[0]}"
            except aiosqlite.OperationalError:
                return "unknown"
        return "unknown"

    # ------------------------------------------------------------------
    # apply fixed + dynamic migrations
    # ------------------------------------------------------------------

    async def apply_fixed_migration(self) -> UpgradeResult:
        try:
            self._lock.acquire_exclusive()
        except Exception as exc:
            return UpgradeResult(ok=False, error=str(exc))
        try:
            async with aiosqlite.connect(str(self.db_path)) as conn:
                await conn.execute("PRAGMA foreign_keys = ON")
                await db.apply_v3_migrations(conn)
                await conn.commit()
            _append_phase_manifest(self.task_dir, UpgradePhase.FIXED_MIGRATED)
            return UpgradeResult(ok=True, phase=UpgradePhase.FIXED_MIGRATED)
        except Exception as exc:
            return UpgradeResult(ok=False, error=str(exc))
        finally:
            self._lock.release()

    async def apply_dynamic_migration(self) -> UpgradeResult:
        try:
            self._lock.acquire_exclusive()
        except Exception as exc:
            return UpgradeResult(ok=False, error=str(exc))
        try:
            task = load_task(self.task_dir)
            async with aiosqlite.connect(str(self.db_path)) as conn:
                await conn.execute("PRAGMA foreign_keys = ON")
                await upgrade_task_dynamic_tables(conn, task)
                await conn.commit()
            _append_phase_manifest(self.task_dir, UpgradePhase.DYNAMIC_MIGRATED)
            return UpgradeResult(ok=True, phase=UpgradePhase.DYNAMIC_MIGRATED)
        except Exception as exc:
            return UpgradeResult(ok=False, error=str(exc))
        finally:
            self._lock.release()

    async def write_runtime_marker(self, *, runtime_core: str = "v3") -> UpgradeResult:
        try:
            self._lock.acquire_exclusive()
        except Exception as exc:
            return UpgradeResult(ok=False, error=str(exc))
        try:
            phase_manifest = _read_phase_manifest(self.task_dir) or []
            backup_entry = next(
                (p for p in phase_manifest if p.get("phase") == UpgradePhase.BACKUP_WRITTEN.value),
                None,
            )
            backup_hash = backup_entry.get("manifest_hash", "") if backup_entry else ""
            async with aiosqlite.connect(str(self.db_path)) as conn:
                cur = await conn.execute("SELECT MAX(version) FROM schema_version")
                row = await cur.fetchone()
                schema_version = int(row[0]) if row and row[0] else 0
            write_runtime_marker(
                self.task_dir,
                runtime_core=runtime_core,
                schema_version=schema_version,
                upgrade_id=secrets.token_hex(8),
                backup_manifest_hash=backup_hash,
            )
            _append_phase_manifest(self.task_dir, UpgradePhase.MARKER_WRITTEN)
            _clear_phase_manifest(self.task_dir)
            return UpgradeResult(ok=True, phase=UpgradePhase.MARKER_WRITTEN)
        except Exception as exc:
            return UpgradeResult(ok=False, error=str(exc))
        finally:
            self._lock.release()

    # ------------------------------------------------------------------
    # restore
    # ------------------------------------------------------------------

    async def restore(self, backup_dir: Path) -> UpgradeResult:
        try:
            self._lock.acquire_exclusive()
        except Exception as exc:
            return UpgradeResult(ok=False, error=str(exc))
        try:
            backup_dir = Path(backup_dir)
            manifest_path = backup_dir / _BACKUP_MANIFEST_FILENAME
            if not manifest_path.exists():
                raise UpgradeError(f"backup manifest missing: {manifest_path}")
            manifest = json.loads(manifest_path.read_text())

            _clear_phase_manifest(self.task_dir)
            _append_phase_manifest(self.task_dir, UpgradePhase.RESTORE_VALIDATING)

            for key in ("active", "seen"):
                entry = manifest[key]
                src = backup_dir / entry["path"]
                if not src.exists():
                    raise UpgradeError(f"backup file missing: {src}")
                actual_hash = _sha256(src)
                if actual_hash != entry["hash"]:
                    raise UpgradeError(
                        f"{key} backup hash mismatch: expected {entry['hash']}, "
                        f"got {actual_hash}"
                    )
                # Integrity check
                async with aiosqlite.connect(str(src)) as conn:
                    cur = await conn.execute("PRAGMA integrity_check")
                    row = await cur.fetchone()
                    if row is None or row[0] != "ok":
                        raise UpgradeError(f"{key} backup integrity check failed: {row}")

            _append_phase_manifest(
                self.task_dir, UpgradePhase.RESTORE_RESTORING_ACTIVE
            )
            shutil.copyfile(backup_dir / manifest["active"]["path"], self.db_path)
            _append_phase_manifest(
                self.task_dir, UpgradePhase.RESTORE_RESTORING_SEEN
            )
            shutil.copyfile(backup_dir / manifest["seen"]["path"], self.seen_path)
            _append_phase_manifest(
                self.task_dir, UpgradePhase.RESTORE_RESTORING_SIDECAR
            )

            # Write a fresh marker reflecting the restored DB.
            restored_schema = await self._read_schema_version(self.db_path)
            schema_int = (
                int(restored_schema[1:]) if restored_schema.startswith("v") else 0
            )
            write_runtime_marker(
                self.task_dir,
                runtime_core=("v3" if schema_int >= 3 else "v2"),
                schema_version=schema_int,
                upgrade_id=secrets.token_hex(8),
                backup_manifest_hash=manifest.get("active", {}).get("hash", ""),
            )
            _append_phase_manifest(self.task_dir, UpgradePhase.RESTORE_MARKER_WRITTEN)
            _clear_phase_manifest(self.task_dir)
            return UpgradeResult(ok=True, phase=UpgradePhase.RESTORE_MARKER_WRITTEN)
        except UpgradeError as exc:
            return UpgradeResult(ok=False, error=str(exc))
        finally:
            self._lock.release()

    # ------------------------------------------------------------------
    # recover (best-effort completion of an interrupted upgrade)
    # ------------------------------------------------------------------

    async def recover(self, backup_dir: Path | None = None) -> UpgradeResult:
        try:
            self._lock.acquire_exclusive()
        except Exception as exc:
            return UpgradeResult(ok=False, error=str(exc))
        try:
            phase_manifest = _read_phase_manifest(self.task_dir)
            if not phase_manifest:
                return UpgradeResult(
                    ok=False,
                    error="no interrupted phase manifest; nothing to recover",
                )
            phases = [p["phase"] for p in phase_manifest]
            if UpgradePhase.MARKER_WRITTEN.value in phases:
                # Already finished; just clear the manifest.
                _clear_phase_manifest(self.task_dir)
                return UpgradeResult(ok=True, phase=UpgradePhase.MARKER_WRITTEN)

            # For anything ambiguous, require explicit --restore.
            if backup_dir is None:
                raise UpgradeError(
                    "upgrade recovery requires explicit --restore <backup_dir> "
                    "because the interrupted phase is ambiguous"
                )
            # If a backup dir was provided, do a full restore.
            self._lock.release()
            self._lock = RuntimeLock(self.task_dir)
            return await self.restore(backup_dir)
        except UpgradeError as exc:
            return UpgradeResult(ok=False, error=str(exc))
        finally:
            try:
                self._lock.release()
            except Exception:
                pass
