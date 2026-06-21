from __future__ import annotations

from pathlib import Path

import pytest

from sluice.dag.rotation.manifest import RotationPhase, append_phase, clear_manifest, mark_failed
from sluice.dag.rotation.startup_guard import (
    RotationRecoveryRequired,
    assert_clean_rotation_state,
)


def test_clean_dir_passes(tmp_path: Path):
    # No manifest, no .failed — startup OK
    assert_clean_rotation_state(tmp_path)


def test_done_phase_passes(tmp_path: Path):
    append_phase(tmp_path, RotationPhase.DONE)
    assert_clean_rotation_state(tmp_path)


def test_failed_marker_blocks(tmp_path: Path):
    mark_failed(tmp_path, reason="x")
    with pytest.raises(RotationRecoveryRequired, match="failed marker"):
        assert_clean_rotation_state(tmp_path)


def test_incomplete_manifest_blocks(tmp_path: Path):
    append_phase(tmp_path, RotationPhase.INSTALLING_NEW)
    with pytest.raises(RotationRecoveryRequired, match="installing_new"):
        assert_clean_rotation_state(tmp_path)


def test_cleared_manifest_passes(tmp_path: Path):
    append_phase(tmp_path, RotationPhase.DONE)
    clear_manifest(tmp_path)
    assert_clean_rotation_state(tmp_path)
