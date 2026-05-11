from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest


def _install_prefect_stub() -> None:
    prefect_mod = types.ModuleType("prefect")
    prefect_mod.flow = lambda **kwargs: lambda fn: fn
    prefect_mod.serve = lambda *args, **kwargs: None
    prefect_client_mod = types.ModuleType("prefect.client")
    prefect_client_schemas_mod = types.ModuleType("prefect.client.schemas")
    prefect_schedules_mod = types.ModuleType("prefect.client.schemas.schedules")

    class CronSchedule:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    prefect_schedules_mod.CronSchedule = CronSchedule
    sys.modules["prefect"] = prefect_mod
    sys.modules["prefect.client"] = prefect_client_mod
    sys.modules["prefect.client.schemas"] = prefect_client_schemas_mod
    sys.modules["prefect.client.schemas.schedules"] = prefect_schedules_mod
    if "croniter" not in sys.modules:
        croniter_mod = types.ModuleType("croniter")

        class croniter:
            def __init__(self, *args, **kwargs):
                pass

            def get_next(self, type_):
                raise AssertionError("croniter stub should not be exercised")

        croniter_mod.croniter = croniter
        sys.modules["croniter"] = croniter_mod


def _import_flows(monkeypatch: pytest.MonkeyPatch):
    _install_prefect_stub()
    sys.modules.pop("sluice.dag.flows", None)
    return importlib.import_module("sluice.dag.flows")


@pytest.mark.asyncio
async def test_run_pipeline_flow_success_path_returns_serialized_result(
    monkeypatch: pytest.MonkeyPatch,
):
    flows = _import_flows(monkeypatch)
    cfg = SimpleNamespace(
        db_path="/tmp/active.db",
        seen_db="/tmp/seen.db",
    )
    calls: list[object] = []

    class FakeLock:
        def __init__(self, db_path):
            self.db_path = db_path
            calls.append(("lock_init", db_path))

        def acquire(self):
            calls.append("acquire")

        def release(self):
            calls.append("release")

    class FakeWriter:
        def __init__(
            self,
            *,
            active_path: str,
            seen_path: str,
            hold_rotation_lock: bool = True,
        ):
            self.kwargs = {
                "active_path": active_path,
                "seen_path": seen_path,
                "hold_rotation_lock": hold_rotation_lock,
            }
            calls.append(("writer_init", self.kwargs))

        async def start(self):
            calls.append("writer_start")

        async def stop(self):
            calls.append("writer_stop")

    async def fake_ensure(task_cfg):
        calls.append(("ensure", task_cfg))

    async def fake_run_pipeline(task_cfg, pipeline_name, *, writer):
        calls.append(("run_pipeline", task_cfg, pipeline_name, writer.kwargs))
        return SimpleNamespace(
            pipeline_name=pipeline_name,
            run_key="task__pipe__2026",
            status="success",
            items_in=3,
            items_out=2,
            error_msg=None,
        )

    monkeypatch.setattr(
        flows,
        "load_task",
        lambda path: calls.append(("load_task", path)) or cfg,
    )
    monkeypatch.setattr(flows, "RotationLock", FakeLock)
    monkeypatch.setattr(flows, "ensure_dynamic_tables", fake_ensure)
    monkeypatch.setattr(flows, "WriterThread", FakeWriter)
    monkeypatch.setattr(flows, "run_pipeline", fake_run_pipeline)

    result = await flows.run_pipeline_flow("example", "fetch_blog", "/configs")

    assert result == {
        "pipeline_name": "fetch_blog",
        "run_key": "task__pipe__2026",
        "status": "success",
        "items_in": 3,
        "items_out": 2,
        "error_msg": None,
    }
    assert calls == [
        ("load_task", Path("/configs/tasks/example")),
        ("lock_init", "/tmp/active.db"),
        "acquire",
        ("ensure", cfg),
        (
            "writer_init",
            {
                "active_path": "/tmp/active.db",
                "seen_path": "/tmp/seen.db",
                "hold_rotation_lock": False,
            },
        ),
        "writer_start",
        (
            "run_pipeline",
            cfg,
            "fetch_blog",
            {
                "active_path": "/tmp/active.db",
                "seen_path": "/tmp/seen.db",
                "hold_rotation_lock": False,
            },
        ),
        "writer_stop",
        "release",
    ]


@pytest.mark.asyncio
async def test_run_pipeline_flow_busy_lock_uses_rotation_fast_path(
    monkeypatch: pytest.MonkeyPatch,
):
    flows = _import_flows(monkeypatch)
    cfg = SimpleNamespace(db_path="/tmp/active.db", seen_db="/tmp/seen.db")
    calls: list[object] = []

    class BusyLock:
        def __init__(self, db_path):
            calls.append(("lock_init", db_path))

        def acquire(self):
            calls.append("acquire")
            raise RuntimeError("busy")

        def release(self):
            calls.append("release")

    def fake_fast_path(task_cfg, pipeline_name):
        calls.append(("fast_path", task_cfg, pipeline_name))
        return SimpleNamespace(
            pipeline_name=pipeline_name,
            run_key="",
            status="skipped_rotation",
            items_in=0,
            items_out=0,
            error_msg="rotation in progress",
        )

    async def fail_ensure(task_cfg):
        raise AssertionError("ensure_dynamic_tables should not run")

    class FailWriter:
        def __init__(self, **kwargs):
            raise AssertionError("WriterThread should not be instantiated")

    async def fail_run_pipeline(*args, **kwargs):
        raise AssertionError("run_pipeline should not run")

    monkeypatch.setattr(
        flows,
        "load_task",
        lambda path: calls.append(("load_task", path)) or cfg,
    )
    monkeypatch.setattr(flows, "RotationLock", BusyLock)
    monkeypatch.setattr(flows, "check_rotation_fast_path", fake_fast_path)
    monkeypatch.setattr(flows, "ensure_dynamic_tables", fail_ensure)
    monkeypatch.setattr(flows, "WriterThread", FailWriter)
    monkeypatch.setattr(flows, "run_pipeline", fail_run_pipeline)

    result = await flows.run_pipeline_flow("example", "fetch_blog")

    assert result["status"] == "skipped_rotation"
    assert result["pipeline_name"] == "fetch_blog"
    assert calls == [
        ("load_task", Path("configs/tasks/example")),
        ("lock_init", "/tmp/active.db"),
        "acquire",
        ("fast_path", cfg, "fetch_blog"),
    ]


@pytest.mark.asyncio
async def test_run_pipeline_flow_busy_lock_fallback_returns_serialized_dict(
    monkeypatch: pytest.MonkeyPatch,
):
    flows = _import_flows(monkeypatch)
    cfg = SimpleNamespace(db_path="/tmp/active.db", seen_db="/tmp/seen.db")

    class BusyLock:
        def __init__(self, db_path):
            pass

        def acquire(self):
            raise RuntimeError("busy")

        def release(self):
            raise AssertionError("lock was never acquired")

    monkeypatch.setattr(flows, "load_task", lambda path: cfg)
    monkeypatch.setattr(flows, "RotationLock", BusyLock)
    monkeypatch.setattr(flows, "check_rotation_fast_path", lambda cfg, pipeline: None)

    result = await flows.run_pipeline_flow("example", "fetch_blog")

    assert result == {
        "pipeline_name": "fetch_blog",
        "run_key": "",
        "status": "lock_busy",
        "items_in": 0,
        "items_out": 0,
        "error_msg": "rotation lock already held",
    }


@pytest.mark.asyncio
async def test_run_pipeline_flow_stops_writer_and_releases_lock_on_pipeline_exception(
    monkeypatch: pytest.MonkeyPatch,
):
    flows = _import_flows(monkeypatch)
    cfg = SimpleNamespace(db_path="/tmp/active.db", seen_db="/tmp/seen.db")
    calls: list[str] = []

    class FakeLock:
        def __init__(self, db_path):
            pass

        def acquire(self):
            calls.append("acquire")

        def release(self):
            calls.append("release")

    class FakeWriter:
        def __init__(
            self,
            *,
            active_path: str,
            seen_path: str,
            hold_rotation_lock: bool = True,
        ):
            calls.append(
                ("writer_init", active_path, seen_path, hold_rotation_lock)
            )

        async def start(self):
            calls.append("writer_start")

        async def stop(self):
            calls.append("writer_stop")

    async def fake_ensure(task_cfg):
        calls.append("ensure")

    async def fake_run_pipeline(*args, **kwargs):
        calls.append("run_pipeline")
        raise RuntimeError("boom")

    monkeypatch.setattr(flows, "load_task", lambda path: cfg)
    monkeypatch.setattr(flows, "RotationLock", FakeLock)
    monkeypatch.setattr(flows, "ensure_dynamic_tables", fake_ensure)
    monkeypatch.setattr(flows, "WriterThread", FakeWriter)
    monkeypatch.setattr(flows, "run_pipeline", fake_run_pipeline)

    with pytest.raises(RuntimeError, match="boom"):
        await flows.run_pipeline_flow("example", "fetch_blog")

    assert calls == [
        "acquire",
        "ensure",
        ("writer_init", "/tmp/active.db", "/tmp/seen.db", False),
        "writer_start",
        "run_pipeline",
        "writer_stop",
        "release",
    ]


@pytest.mark.asyncio
async def test_rotate_task_flow_success_path_serializes_result(
    monkeypatch: pytest.MonkeyPatch,
):
    flows = _import_flows(monkeypatch)
    cfg = SimpleNamespace(name="example")
    sluice_cfg = SimpleNamespace(name="sluice")
    calls: list[object] = []

    async def fake_rotate_task(task_cfg, *, config):
        calls.append(("rotate_task", task_cfg, config))
        return SimpleNamespace(
            task_name="example",
            status="rotated",
            carryover_count=4,
            archive_count=5,
            archive_path="/archive/example.db.gz",
            error_msg=None,
        )

    monkeypatch.setattr(
        flows,
        "load_task",
        lambda path: calls.append(("load_task", path)) or cfg,
    )
    monkeypatch.setattr(
        flows,
        "load_sluice_config",
        lambda path: calls.append(("load_sluice_config", path)) or sluice_cfg,
    )
    monkeypatch.setattr(flows, "rotate_task", fake_rotate_task)

    result = await flows.rotate_task_flow(
        "example",
        config_dir="/configs",
        sluice_config="/sluice.toml",
    )

    assert result == {
        "task_name": "example",
        "status": "rotated",
        "carryover_count": 4,
        "archive_count": 5,
        "archive_path": "/archive/example.db.gz",
        "error_msg": None,
    }
    assert calls == [
        ("load_task", Path("/configs/tasks/example")),
        ("load_sluice_config", Path("/sluice.toml")),
        ("rotate_task", cfg, sluice_cfg),
    ]


@pytest.mark.asyncio
async def test_rotate_task_flow_serializes_lock_busy_result(
    monkeypatch: pytest.MonkeyPatch,
):
    flows = _import_flows(monkeypatch)
    cfg = SimpleNamespace(name="example")

    async def fake_rotate_task(task_cfg, *, config):
        return SimpleNamespace(
            task_name="example",
            status="lock_busy",
            carryover_count=0,
            archive_count=0,
            archive_path=None,
            error_msg="rotation lock already held",
        )

    monkeypatch.setattr(flows, "load_task", lambda path: cfg)
    monkeypatch.setattr(flows, "load_sluice_config", lambda path: object())
    monkeypatch.setattr(flows, "rotate_task", fake_rotate_task)

    result = await flows.rotate_task_flow("example")

    assert result == {
        "task_name": "example",
        "status": "lock_busy",
        "carryover_count": 0,
        "archive_count": 0,
        "archive_path": None,
        "error_msg": "rotation lock already held",
    }
