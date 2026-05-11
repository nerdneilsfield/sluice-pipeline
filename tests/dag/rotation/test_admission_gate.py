from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from sluice.dag import db
from sluice.dag.rotation.admission import (
    RotationGateClosed,
    build_admission_job,
    build_deadmission_job,
)
from sluice.dag.rotation.gate import admissions_count, close_gate, open_gate
from sluice.dag.writer import WriterThread


async def _gate_closed(active_path: str) -> int:
    async with db.open_active_db(active_path) as conn:
        cur = await conn.execute("SELECT closed FROM rotation_gate WHERE id = 1")
        row = await cur.fetchone()
        return int(row[0])


async def _admission_rows(active_path: str) -> list[tuple[str, str, str]]:
    async with db.open_active_db(active_path) as conn:
        cur = await conn.execute(
            "SELECT admission_id, task, pipeline_name FROM pipeline_admissions "
            "ORDER BY admission_id"
        )
        return [(row[0], row[1], row[2]) for row in await cur.fetchall()]


@pytest.mark.asyncio
async def test_migration_adds_rotation_gate_schema(tmp_path: Path):
    active_path = str(tmp_path / "active.db")

    async with db.open_active_db(active_path) as conn:
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('rotation_gate', 'pipeline_admissions')"
        )
        tables = {row[0] for row in await cur.fetchall()}
        cur = await conn.execute("SELECT MAX(version) FROM schema_version")
        version = int((await cur.fetchone())[0])
        cur = await conn.execute("SELECT closed FROM rotation_gate WHERE id=1")
        closed = int((await cur.fetchone())[0])

    assert tables == {"rotation_gate", "pipeline_admissions"}
    assert version == 2
    assert closed == 0


@pytest.mark.asyncio
async def test_admission_job_inserts_when_gate_open(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    writer = WriterThread(active_path=active_path, seen_path=str(tmp_path / "seen.db"))
    await writer.start()
    try:
        await (
            await writer.submit(
                build_admission_job(
                    admission_id="aid-1", task_name="t", pipeline_name="P"
                )
            )
        )
    finally:
        await writer.stop()

    assert await _admission_rows(active_path) == [("aid-1", "t", "P")]


@pytest.mark.asyncio
async def test_admission_job_raises_and_rolls_back_when_gate_closed(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    await close_gate(active_path)
    writer = WriterThread(active_path=active_path, seen_path=str(tmp_path / "seen.db"))
    await writer.start()
    try:
        with pytest.raises(RotationGateClosed):
            await (
                await writer.submit(
                    build_admission_job(
                        admission_id="aid-1", task_name="t", pipeline_name="P"
                    )
                )
            )
    finally:
        await writer.stop()

    assert await _admission_rows(active_path) == []


@pytest.mark.asyncio
async def test_deadmission_job_is_idempotent(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    writer = WriterThread(active_path=active_path, seen_path=str(tmp_path / "seen.db"))
    await writer.start()
    try:
        await (
            await writer.submit(
                build_admission_job(
                    admission_id="aid-1", task_name="t", pipeline_name="P"
                )
            )
        )
        await (await writer.submit(build_deadmission_job("aid-1")))
        await (await writer.submit(build_deadmission_job("aid-1")))
    finally:
        await writer.stop()

    assert await _admission_rows(active_path) == []


@pytest.mark.asyncio
async def test_close_and_open_gate_control_admission(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    await close_gate(active_path)
    assert await _gate_closed(active_path) == 1
    assert await admissions_count(active_path, "t") == 0

    writer = WriterThread(active_path=active_path, seen_path=str(tmp_path / "seen.db"))
    await writer.start()
    try:
        with pytest.raises(RotationGateClosed):
            await (
                await writer.submit(
                    build_admission_job(
                        admission_id="aid-1", task_name="t", pipeline_name="P"
                    )
                )
            )
    finally:
        await writer.stop()

    await open_gate(active_path)
    assert await _gate_closed(active_path) == 0
    writer = WriterThread(active_path=active_path, seen_path=str(tmp_path / "seen.db"))
    await writer.start()
    try:
        await (
            await writer.submit(
                build_admission_job(
                    admission_id="aid-1", task_name="t", pipeline_name="P"
                )
            )
        )
    finally:
        await writer.stop()
    assert await admissions_count(active_path, "t") == 1


@pytest.mark.asyncio
async def test_admission_fails_closed_when_gate_row_missing(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    async with db.open_active_db(active_path) as conn:
        await conn.execute("DELETE FROM rotation_gate WHERE id = 1")
        await conn.commit()

    writer = WriterThread(active_path=active_path, seen_path=str(tmp_path / "seen.db"))
    await writer.start()
    try:
        with pytest.raises(RotationGateClosed, match="fail-closed"):
            await (
                await writer.submit(
                    build_admission_job(
                        admission_id="aid-1", task_name="t", pipeline_name="P"
                    )
                )
            )
    finally:
        await writer.stop()

    assert await _admission_rows(active_path) == []


@pytest.mark.asyncio
async def test_gate_operations_are_idempotent_and_recreate_missing_row(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    await close_gate(active_path)
    await close_gate(active_path)
    assert await _gate_closed(active_path) == 1

    async with db.open_active_db(active_path) as conn:
        await conn.execute("DELETE FROM rotation_gate WHERE id = 1")
        await conn.commit()

    await close_gate(active_path)
    assert await _gate_closed(active_path) == 1
    await open_gate(active_path)
    assert await _gate_closed(active_path) == 0


@pytest.mark.asyncio
async def test_admission_and_close_gate_race_has_consistent_outcome(tmp_path: Path):
    active_path = str(tmp_path / "active.db")
    async with db.open_active_db(active_path):
        pass
    writer = WriterThread(active_path=active_path, seen_path=str(tmp_path / "seen.db"))
    await writer.start()

    async def admit() -> Exception | None:
        try:
            await (
                await writer.submit(
                    build_admission_job(
                        admission_id="aid-1", task_name="t", pipeline_name="P"
                    )
                )
            )
        except Exception as exc:  # noqa: BLE001
            return exc
        return None

    try:
        admission_exc, _ = await asyncio.gather(admit(), close_gate(active_path))
    finally:
        await writer.stop()

    assert await _gate_closed(active_path) == 1
    count = await admissions_count(active_path, "t")
    if admission_exc is None:
        assert count == 1
    else:
        assert isinstance(admission_exc, RotationGateClosed)
        assert count == 0
