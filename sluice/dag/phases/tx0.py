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

    Idempotent on run_key: if a row already exists with a non-expired lease
    owned by ``lease_owner``, the same run_id is returned. If a row exists but
    is owned by a different lease owner or has been fenced (lease_version
    mismatch), the job raises so the caller fails closed.
    """

    async def job(active, seen) -> None:  # noqa: ARG001
        store = PipelinesStore(active)
        existing_id = await store.get_run_id(run_key)
        lease_until = _lease_until(started_at, lease_duration_seconds)
        if existing_id is not None:
            state = await store.get_lease_state(existing_id)
            if state is None:
                raise RuntimeError(f"pipeline row {existing_id} vanished mid-tx0")
            if state["lease_owner"] != lease_owner or state["status"] != "running":
                raise RuntimeError(
                    f"run_key {run_key!r} already held by owner={state['lease_owner']!r} "
                    f"status={state['status']!r}; cannot attach"
                )
            updated = await store.set_lease(
                run_id=existing_id,
                lease_owner=lease_owner,
                lease_until=lease_until,
                heartbeat_at=started_at,
                lease_version=state["lease_version"] or 0,
            )
            if not updated:
                raise RuntimeError(
                    f"run_key {run_key!r} lease fenced during tx0 attach; aborting"
                )
            out["pipeline_run_id"] = existing_id
            out["cycle_id"] = cycle_anchor
            out["lease_version"] = state["lease_version"] or 0
            return

        run_id = await store.upsert_running_v3(
            run_key=run_key,
            task=task.name,
            pipeline_name=pipeline.name,
            cycle_anchor=cycle_anchor,
            cycle_id=cycle_anchor,
            trigger=trigger,
            started_at=started_at,
        )
        # Initial lease_version is 1 (0 means "no lease" / legacy).
        await store.set_lease(
            run_id=run_id,
            lease_owner=lease_owner,
            lease_until=lease_until,
            heartbeat_at=started_at,
            lease_version=1,
        )
        out["pipeline_run_id"] = run_id
        out["cycle_id"] = cycle_anchor
        out["lease_version"] = 1

    return WriteJob(run=job)


def _lease_until(started_at_iso: str, duration_seconds: float) -> str:
    started = datetime.fromisoformat(started_at_iso)
    lease_until = started + _seconds(duration_seconds)
    return lease_until.isoformat()


def _seconds(value: float) -> datetime:
    return datetime.fromtimestamp(value, tz=timezone.utc) - datetime.fromtimestamp(
        0, tz=timezone.utc
    )


def make_lease_owner() -> str:
    """Generate a per-process lease owner identifier."""
    return f"runner-{secrets.token_hex(4)}"
