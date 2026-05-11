from __future__ import annotations

from datetime import datetime, timezone

from sluice.dag.writer import WriteJob


class RotationGateClosed(RuntimeError):
    """Raised when a pipeline cannot enter because rotation is closing the DB."""


def build_admission_job(*, admission_id: str, task_name: str, pipeline_name: str) -> WriteJob:
    registered_at = datetime.now(timezone.utc).isoformat()

    async def job(active, seen) -> None:  # noqa: ARG001
        cur = await active.execute("SELECT closed FROM rotation_gate WHERE id = 1")
        row = await cur.fetchone()
        if row is None:
            raise RotationGateClosed(
                f"rotation_gate row missing for task {task_name!r}; "
                "refusing admission (fail-closed)"
            )
        if row[0]:
            raise RotationGateClosed(f"rotation gate closed for task {task_name!r}")
        await active.execute(
            "INSERT INTO pipeline_admissions"
            "(admission_id, task, pipeline_name, registered_at) "
            "VALUES (?, ?, ?, ?)",
            (admission_id, task_name, pipeline_name, registered_at),
        )

    return WriteJob(run=job)


def build_deadmission_job(admission_id: str) -> WriteJob:
    async def job(active, seen) -> None:  # noqa: ARG001
        await active.execute(
            "DELETE FROM pipeline_admissions WHERE admission_id = ?",
            (admission_id,),
        )

    return WriteJob(run=job)
