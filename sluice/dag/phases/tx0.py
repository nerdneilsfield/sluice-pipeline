from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Any

from sluice.dag.config import PipelineDef, TaskConfig
from sluice.dag.stores.pipelines import PipelinesStore
from sluice.dag.writer import WriteJob


def build_tx0_job(
    *,
    task: TaskConfig,
    pipeline: PipelineDef,
    run_key: str,
    cycle_anchor: str,
    trigger: str,
    started_at: str,
    lease_owner: str,
    lease_duration_seconds: float,
    out: dict[str, Any],
) -> WriteJob:
    """Create the pipeline run row with lease fields before stage execution.

    Atomic: lease fields are written in the same INSERT as the run row, so
    there is no TOCTOU window between INSERT and lease claim.

    Returns via ``out`` dict: ``pipeline_run_id``, ``cycle_id``,
    ``lease_version``.

    Fail-closed: if the run_key already exists with a different lease_owner,
    raises RuntimeError so the caller knows it lost the race.
    """

    async def job(active, seen) -> None:  # noqa: ARG001
        store = PipelinesStore(active)
        lease_until = _lease_until(started_at, lease_duration_seconds)
        run_id, actual_owner, actual_version = await store.upsert_running_v3(
            run_key=run_key,
            task=task.name,
            pipeline_name=pipeline.name,
            cycle_anchor=cycle_anchor,
            cycle_id=cycle_anchor,
            trigger=trigger,
            started_at=started_at,
            lease_owner=lease_owner,
            lease_until=lease_until,
            lease_version=1,
        )
        if actual_owner is not None and actual_owner != lease_owner:
            raise RuntimeError(
                f"run_key {run_key!r} already held by owner={actual_owner!r}; "
                f"cannot attach as {lease_owner!r}"
            )
        # If we won the race (insert), actual_owner == lease_owner and
        # actual_version == 1. If we reattached to our own row, the stored
        # version is whatever we set previously. Refresh the lease with a
        # fenced update so concurrent writers see the new until/heartbeat.
        refreshed = await store.set_lease(
            run_id=run_id,
            lease_owner=lease_owner,
            lease_until=lease_until,
            heartbeat_at=started_at,
            lease_version=max(actual_version, 1),
            expected_lease_version=actual_version or 0,
        )
        if not refreshed:
            raise RuntimeError(
                f"run_key {run_key!r} lease fenced during tx0 attach; aborting"
            )
        out["pipeline_run_id"] = run_id
        out["cycle_id"] = cycle_anchor
        out["lease_version"] = max(actual_version, 1)

    return WriteJob(run=job)


def _lease_until(started_at_iso: str, duration_seconds: float) -> str:
    from datetime import timedelta

    started = datetime.fromisoformat(started_at_iso)
    return (started + timedelta(seconds=duration_seconds)).isoformat()


def make_lease_owner() -> str:
    """Generate a per-process lease owner identifier."""
    return f"runner-{secrets.token_hex(4)}"
