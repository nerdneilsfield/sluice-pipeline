from __future__ import annotations

import asyncio
import importlib
import sys
import types
from dataclasses import replace
from types import SimpleNamespace

import pytest

from sluice.core.errors import ConfigError
from sluice.dag.config import (
    DeploymentsDef,
    PipelineDef,
    SinkDef,
    SourceDef,
    StageDef,
    TaskConfig,
    WindowDef,
)
from sluice.dag.sluice_config import RotationConfig, RuntimeConfig, SluiceConfig


class FakeDeployment(SimpleNamespace):
    pass


class FakeFlow:
    def __init__(self, flow_name: str):
        self.flow_name = flow_name
        self.calls: list[dict] = []

    def to_deployment(self, **kwargs):
        self.calls.append(kwargs)
        return FakeDeployment(flow_name=self.flow_name, **kwargs)


class FakeTemporarySettings:
    def __init__(self, updates):
        self.updates = updates

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _install_prefect_stub(
    monkeypatch: pytest.MonkeyPatch,
    *,
    with_entrypoint: bool = True,
):
    calls: list[dict] = []

    async def fake_deploy(*deployments, **kwargs):
        calls.append({"deployments": deployments, "kwargs": kwargs})
        return None

    prefect_mod = types.ModuleType("prefect")
    prefect_mod.deploy = fake_deploy

    prefect_client_mod = types.ModuleType("prefect.client")
    prefect_client_schemas_mod = types.ModuleType("prefect.client.schemas")
    prefect_schedules_mod = types.ModuleType("prefect.client.schemas.schedules")

    class CronSchedule:
        def __init__(self, *, cron: str, timezone: str):
            self.cron = cron
            self.timezone = timezone

    prefect_schedules_mod.CronSchedule = CronSchedule

    prefect_settings_mod = types.ModuleType("prefect.settings")
    prefect_settings_mod.PREFECT_API_URL = "PREFECT_API_URL"
    prefect_settings_mod.PREFECT_API_AUTH_STRING = "PREFECT_API_AUTH_STRING"

    prefect_settings_mod.temporary_settings = FakeTemporarySettings

    monkeypatch.setitem(sys.modules, "prefect", prefect_mod)
    monkeypatch.setitem(sys.modules, "prefect.client", prefect_client_mod)
    monkeypatch.setitem(sys.modules, "prefect.client.schemas", prefect_client_schemas_mod)
    monkeypatch.setitem(
        sys.modules,
        "prefect.client.schemas.schedules",
        prefect_schedules_mod,
    )
    monkeypatch.setitem(sys.modules, "prefect.settings", prefect_settings_mod)

    if with_entrypoint:
        prefect_types_mod = types.ModuleType("prefect.types")
        prefect_entrypoint_mod = types.ModuleType("prefect.types.entrypoint")

        class EntrypointType:
            MODULE_PATH = "module_path"

        prefect_entrypoint_mod.EntrypointType = EntrypointType
        monkeypatch.setitem(sys.modules, "prefect.types", prefect_types_mod)
        monkeypatch.setitem(
            sys.modules,
            "prefect.types.entrypoint",
            prefect_entrypoint_mod,
        )
    else:
        monkeypatch.delitem(sys.modules, "prefect.types", raising=False)
        monkeypatch.delitem(sys.modules, "prefect.types.entrypoint", raising=False)

    return calls


@pytest.fixture
def deploy_module(monkeypatch: pytest.MonkeyPatch):
    deploy_calls = _install_prefect_stub(monkeypatch)
    flows_mod = types.ModuleType("sluice.dag.flows")
    flows_mod.run_pipeline_flow = FakeFlow("run")
    flows_mod.rotate_task_flow = FakeFlow("rotate")
    monkeypatch.setitem(sys.modules, "sluice.dag.flows", flows_mod)
    monkeypatch.delitem(sys.modules, "sluice.dag.deploy", raising=False)
    module = importlib.import_module("sluice.dag.deploy")
    module._prefect_deploy_calls = deploy_calls
    return module


def _task(**overrides) -> TaskConfig:
    pipeline = PipelineDef(
        name="daily",
        cron="0 8 * * *",
        stages=["fetch"],
        sources=["rss"],
        upstream_all=None,
        upstream_any=None,
        sinks=["out"],
        post_run_cleanup=None,
        input_mode="union",
    )
    base = dict(
        name="news",
        db_path="active.db",
        seen_db="seen.db",
        enabled=True,
        timezone=None,
        window=WindowDef(anchor="0 8 * * *", length="24h"),
        deployments=DeploymentsDef(pipelines="all"),
        sources=[SourceDef(name="rss", type="rss")],
        sinks=[SinkDef(name="out", type="file_md")],
        stages=[StageDef(name="fetch", type="fetch_rss", params={"sources": ["rss"]})],
        pipelines=[pipeline],
        task_dir=None,
        rotation_cron=None,
    )
    base.update(overrides)
    return TaskConfig(**base)


def _cfg(**runtime_overrides) -> SluiceConfig:
    return SluiceConfig(
        rotation=RotationConfig(default_cron="0 3 * * *"),
        runtime=RuntimeConfig(**{"work_pool": "worker-a", **runtime_overrides}),
    )


def test_build_pipeline_deployment_uses_task_timezone_then_config_fallback(deploy_module):
    task = _task(timezone="Asia/Shanghai")
    dep = deploy_module.build_pipeline_deployment(
        task, task.pipelines[0], _cfg(default_timezone="UTC"), "/configs"
    )

    assert dep.name == "news-daily"
    assert dep.schedule.cron == "0 8 * * *"
    assert dep.schedule.timezone == "Asia/Shanghai"
    assert dep.concurrency_limit == 1
    assert dep.parameters == {
        "task": "news",
        "pipeline": "daily",
        "config_dir": "/configs",
    }
    assert dep.work_pool_name == "worker-a"
    assert dep.entrypoint_type == "module_path"

    fallback = deploy_module.build_pipeline_deployment(
        replace(task, timezone=None), task.pipelines[0], _cfg(default_timezone="UTC"), "/c"
    )
    assert fallback.schedule.timezone == "UTC"


def test_build_rotation_deployment_includes_sluice_config_when_given(deploy_module):
    task = _task()
    dep = deploy_module.build_rotation_deployment(
        task, "15 4 * * *", _cfg(default_timezone="Europe/Berlin"), "/configs", "/sluice.toml"
    )

    assert dep.name == "news-rotate"
    assert dep.schedule.cron == "15 4 * * *"
    assert dep.schedule.timezone == "Europe/Berlin"
    assert dep.concurrency_limit == 1
    assert dep.parameters == {
        "task": "news",
        "config_dir": "/configs",
        "sluice_config": "/sluice.toml",
    }
    assert dep.work_pool_name == "worker-a"


def test_build_rotation_deployment_omits_none_sluice_config(deploy_module):
    dep = deploy_module.build_rotation_deployment(
        _task(), "15 4 * * *", _cfg(), "/configs", None
    )

    assert dep.parameters == {"task": "news", "config_dir": "/configs"}


def test_push_all_deployments_batches_pipeline_and_rotation(deploy_module):
    result = asyncio.run(
        deploy_module.push_all_deployments([_task()], _cfg(), "/configs", "/sluice.toml")
    )

    assert result == (1, 1)
    assert len(deploy_module._prefect_deploy_calls) == 1
    call = deploy_module._prefect_deploy_calls[0]
    assert [dep.name for dep in call["deployments"]] == ["news-daily", "news-rotate"]
    assert call["kwargs"] == {
        "work_pool_name": "worker-a",
        "build": False,
        "push": False,
    }


def test_push_all_deployments_skips_disabled_task_without_deploy_call(deploy_module):
    result = asyncio.run(
        deploy_module.push_all_deployments([_task(enabled=False)], _cfg(), "/configs", None)
    )

    assert result == (0, 0)
    assert deploy_module._prefect_deploy_calls == []


@pytest.mark.parametrize("cron", ["", None])
def test_push_all_deployments_skips_pipeline_without_cron(deploy_module, cron):
    task = _task(pipelines=[replace(_task().pipelines[0], cron=cron)])
    result = asyncio.run(
        deploy_module.push_all_deployments(
            [task],
            SluiceConfig(rotation=RotationConfig(default_cron=None), runtime=_cfg().runtime),
            "/configs",
            None,
        )
    )

    assert result == (0, 0)
    assert deploy_module._prefect_deploy_calls == []


def test_push_all_deployments_skips_rotation_without_effective_cron(deploy_module):
    cfg = SluiceConfig(rotation=RotationConfig(default_cron=None), runtime=_cfg().runtime)

    result = asyncio.run(
        deploy_module.push_all_deployments([_task()], cfg, "/configs", None)
    )

    assert result == (1, 0)
    call = deploy_module._prefect_deploy_calls[0]
    assert [dep.name for dep in call["deployments"]] == ["news-daily"]


def test_push_all_deployments_respects_pipeline_allowlist(deploy_module):
    task = _task(deployments=DeploymentsDef(pipelines=[]), rotation_cron=None)
    cfg = SluiceConfig(rotation=RotationConfig(default_cron=None), runtime=_cfg().runtime)

    result = asyncio.run(deploy_module.push_all_deployments([task], cfg, "/configs", None))

    assert result == (0, 0)
    assert deploy_module._prefect_deploy_calls == []


def test_push_all_deployments_includes_image_only_when_runtime_has_truthy_image(deploy_module):
    runtime_with_image = SimpleNamespace(
        prefect_api_url="",
        prefect_api_auth_string="",
        work_pool="worker-a",
        default_timezone="UTC",
        image="registry/sluice:latest",
    )
    cfg = SluiceConfig(rotation=RotationConfig(default_cron=None), runtime=runtime_with_image)

    result = asyncio.run(deploy_module.push_all_deployments([_task()], cfg, "/configs", None))

    assert result == (1, 0)
    assert deploy_module._prefect_deploy_calls[0]["kwargs"]["image"] == "registry/sluice:latest"


def test_invalid_task_name_raises_config_error_without_sanitizing(deploy_module):
    task = _task(name="bad/task")

    with pytest.raises(ConfigError, match="bad/task"):
        deploy_module.build_pipeline_deployment(task, task.pipelines[0], _cfg(), "/configs")


def test_blank_work_pool_raises_config_error(deploy_module):
    cfg = _cfg(work_pool=" \t")
    task = _task()

    with pytest.raises(ConfigError, match="runtime.work_pool"):
        deploy_module.build_pipeline_deployment(task, task.pipelines[0], cfg, "/configs")

    with pytest.raises(ConfigError, match="runtime.work_pool"):
        asyncio.run(deploy_module.push_all_deployments([task], cfg, "/configs", None))


def test_prefect_api_settings_rejects_empty_env_auth(deploy_module, monkeypatch):
    monkeypatch.setenv("SLUICE_EMPTY_TOKEN", "")

    with pytest.raises(ConfigError, match="prefect_api_auth_string"):
        deploy_module._prefect_api_settings(
            SimpleNamespace(prefect_api_url="", prefect_api_auth_string="env:SLUICE_EMPTY_TOKEN")
        )


def test_prefect_api_settings_no_updates_returns_nullcontext(deploy_module):
    cm = deploy_module._prefect_api_settings(
        RuntimeConfig(prefect_api_url="", prefect_api_auth_string="")
    )

    with cm as value:
        assert value is None


def test_prefect_api_settings_sets_url_and_resolved_auth(deploy_module, monkeypatch):
    monkeypatch.setenv("SLUICE_TOKEN", "secret-token")

    cm = deploy_module._prefect_api_settings(
        RuntimeConfig(
            prefect_api_url="http://localhost:4200/api",
            prefect_api_auth_string="env:SLUICE_TOKEN",
        )
    )

    assert cm.updates == {
        "PREFECT_API_URL": "http://localhost:4200/api",
        "PREFECT_API_AUTH_STRING": "secret-token",
    }
    with cm as active:
        assert active.updates == cm.updates
