from __future__ import annotations

import json
from pathlib import Path

import pytest

from sluice.dag.rotation.manifest import (
    RotationPhase,
    append_phase,
    atomic_write_json,
    clear_failed,
    clear_manifest,
    failed_marker_path,
    fsync_db_file,
    has_failed_marker,
    latest_phase,
    manifest_path,
    mark_failed,
    read_manifest,
    sidecar_dir,
)


def test_atomic_write_json_creates_file(tmp_path: Path):
    target = tmp_path / "out.json"
    atomic_write_json(target, {"a": 1, "b": [2, 3]})
    assert json.loads(target.read_text()) == {"a": 1, "b": [2, 3]}


def test_atomic_write_json_overwrites_atomically(tmp_path: Path):
    target = tmp_path / "out.json"
    atomic_write_json(target, {"v": 1})
    atomic_write_json(target, {"v": 2})
    assert json.loads(target.read_text()) == {"v": 2}
    # No leftover temp file
    assert not (tmp_path / "out.json.tmp").exists()


def test_append_phase_creates_manifest_with_first_phase(tmp_path: Path):
    append_phase(tmp_path, RotationPhase.COPYING, payload={"old_size": 1234})
    manifest = read_manifest(tmp_path)
    assert manifest is not None
    assert len(manifest["phases"]) == 1
    assert manifest["phases"][0]["phase"] == "copying"
    assert manifest["phases"][0]["payload"] == {"old_size": 1234}


def test_append_phase_appends_in_order(tmp_path: Path):
    for phase in [RotationPhase.COPYING, RotationPhase.COPIED, RotationPhase.INSTALLING_NEW]:
        append_phase(tmp_path, phase)
    manifest = read_manifest(tmp_path)
    assert manifest is not None
    phases = [p["phase"] for p in manifest["phases"]]
    assert phases == ["copying", "copied", "installing_new"]


def test_latest_phase_returns_last_appended(tmp_path: Path):
    append_phase(tmp_path, RotationPhase.COPYING)
    append_phase(tmp_path, RotationPhase.COPIED)
    assert latest_phase(tmp_path) == RotationPhase.COPIED


def test_latest_phase_returns_none_when_no_manifest(tmp_path: Path):
    assert latest_phase(tmp_path) is None


def test_read_manifest_raises_on_corrupt_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    sidecar_dir(tmp_path).mkdir(parents=True)
    manifest_path(tmp_path).write_text("not valid json {")
    with pytest.raises(RuntimeError, match="corrupt rotation sidecar"):
        read_manifest(tmp_path)


def test_mark_failed_creates_marker(tmp_path: Path):
    mark_failed(tmp_path, reason="swap_failed", details={"phase": "installing_new"})
    assert has_failed_marker(tmp_path) is True
    payload = json.loads(failed_marker_path(tmp_path).read_text())
    assert payload["reason"] == "swap_failed"
    assert payload["details"] == {"phase": "installing_new"}


def test_clear_failed_removes_marker(tmp_path: Path):
    mark_failed(tmp_path, reason="x")
    clear_failed(tmp_path)
    assert has_failed_marker(tmp_path) is False


def test_clear_manifest_removes_manifest(tmp_path: Path):
    append_phase(tmp_path, RotationPhase.DONE)
    clear_manifest(tmp_path)
    assert read_manifest(tmp_path) is None


def test_fsync_db_file_succeeds_for_real_file(tmp_path: Path):
    db = tmp_path / "x.db"
    db.write_bytes(b"sqlite")
    fsync_db_file(db)  # should not raise


def test_atomic_write_json_cleans_up_temp_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """If fsync fails, the temp file must be removed."""
    target = tmp_path / "out.json"
    target.write_text("existing")

    def failing_replace(src, dst):
        raise OSError("injected replace failure")

    monkeypatch.setattr("sluice.dag.rotation.manifest.os.replace", failing_replace)
    with pytest.raises(OSError, match="injected"):
        atomic_write_json(target, {"v": 1})
    # Temp must be cleaned up
    assert not (tmp_path / "out.json.tmp").exists()
    # Original untouched
    assert target.read_text() == "existing"
