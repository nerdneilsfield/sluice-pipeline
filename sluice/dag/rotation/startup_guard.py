from __future__ import annotations

import logging
from pathlib import Path

from sluice.dag.rotation.manifest import (
    RotationPhase,
    has_failed_marker,
    latest_phase,
    mark_failed,
)

logger = logging.getLogger(__name__)


class RotationRecoveryRequired(RuntimeError):
    """Raised when startup cannot proceed safely without operator action."""


def assert_clean_rotation_state(task_dir: Path | str) -> None:
    """Startup guard. Raises RotationRecoveryRequired if:
    - a .failed marker exists (rotation previously crashed)
    - a sidecar manifest exists but is not at the 'done' phase (interrupted
      rotation in progress)

    Call this before any runner/admin action that opens the active DB. The
    active DB might not exist or might be mid-swap; refusing early prevents
    silent creation of an empty DB that overwrites rotation state.

    Normal startup with no manifest and no .failed is allowed.
    """
    if has_failed_marker(task_dir):
        raise RotationRecoveryRequired(
            f"rotation failed marker present in {task_dir}; "
            "operator must run `sluice upgrade v3 --recover` or `--restore` "
            "before any runner or admin action"
        )
    phase = latest_phase(task_dir)
    if phase is None:
        return
    if phase == RotationPhase.DONE:
        # Manifest should be cleared after DONE, but tolerate it.
        return
    raise RotationRecoveryRequired(
        f"rotation sidecar at {task_dir} is in phase {phase.value!r}; "
        "rotation was interrupted. Operator must run "
        "`sluice upgrade v3 --recover` or `--restore`."
    )


def safe_mark_failed(
    task_dir: Path | str, *, reason: str, details: dict | None = None
) -> None:
    """Best-effort wrapper that logs if marking failed itself fails."""
    try:
        mark_failed(task_dir, reason=reason, details=details or {})
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "could not write rotation failed marker for %s: %s",
            task_dir,
            exc,
        )
