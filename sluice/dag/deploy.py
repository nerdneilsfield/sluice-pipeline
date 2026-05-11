from __future__ import annotations

from contextlib import nullcontext
from typing import Any

import prefect
from prefect.client.schemas.schedules import CronSchedule
from prefect.settings import (
    PREFECT_API_AUTH_STRING,
    PREFECT_API_URL,
    temporary_settings,
)

from sluice.core.errors import ConfigError
from sluice.dag.config import PipelineDef, TaskConfig
from sluice.dag.flows import rotate_task_flow, run_pipeline_flow
from sluice.dag.sluice_config import SluiceConfig
from sluice.dag.validator import ValidationError, validate_task
from sluice.loader import resolve_env

try:
    from prefect.types.entrypoint import EntrypointType
except ImportError:  # pragma: no cover - exercised only with older Prefect installs.
    EntrypointType = None  # type: ignore[assignment]


def _effective_cron(task: TaskConfig, sluice_cfg: SluiceConfig) -> str | None:
    return task.rotation_cron or sluice_cfg.rotation.default_cron


def _effective_timezone(task: TaskConfig, sluice_cfg: SluiceConfig) -> str:
    return task.timezone or sluice_cfg.runtime.default_timezone or "UTC"


def _work_pool_name(sluice_cfg: SluiceConfig) -> str:
    work_pool = sluice_cfg.runtime.work_pool.strip()
    if not work_pool:
        raise ConfigError("runtime.work_pool must not be empty")
    return work_pool


def _validate_for_deployment(task: TaskConfig) -> None:
    try:
        validate_task(task)
    except ValidationError as exc:
        raise ConfigError(f"invalid task {getattr(task, 'name', '<unknown>')!r}: {exc}") from exc


def _entrypoint_kwargs() -> dict[str, Any]:
    if EntrypointType is None:
        return {}
    return {"entrypoint_type": EntrypointType.MODULE_PATH}


def build_pipeline_deployment(
    task: TaskConfig,
    pipeline: PipelineDef,
    sluice_cfg: SluiceConfig,
    config_dir: str,
):
    _validate_for_deployment(task)
    work_pool = _work_pool_name(sluice_cfg)
    tz = _effective_timezone(task, sluice_cfg)
    return run_pipeline_flow.to_deployment(
        name=f"{task.name}-{pipeline.name}",
        schedule=CronSchedule(cron=pipeline.cron, timezone=tz),
        concurrency_limit=1,
        parameters={
            "task": task.name,
            "pipeline": pipeline.name,
            "config_dir": config_dir,
        },
        work_pool_name=work_pool,
        **_entrypoint_kwargs(),
    )


def build_rotation_deployment(
    task: TaskConfig,
    effective_cron: str,
    sluice_cfg: SluiceConfig,
    config_dir: str,
    sluice_config_path: str | None,
):
    _validate_for_deployment(task)
    work_pool = _work_pool_name(sluice_cfg)
    tz = _effective_timezone(task, sluice_cfg)
    parameters = {"task": task.name, "config_dir": config_dir}
    if sluice_config_path is not None:
        parameters["sluice_config"] = sluice_config_path
    return rotate_task_flow.to_deployment(
        name=f"{task.name}-rotate",
        schedule=CronSchedule(cron=effective_cron, timezone=tz),
        concurrency_limit=1,
        parameters=parameters,
        work_pool_name=work_pool,
        **_entrypoint_kwargs(),
    )


def _pipeline_allowed(task: TaskConfig, pipeline: PipelineDef) -> bool:
    allowed = task.deployments.pipelines
    return allowed == "all" or pipeline.name in allowed


async def push_all_deployments(
    tasks: list[TaskConfig],
    sluice_cfg: SluiceConfig,
    config_dir: str,
    sluice_config_path: str | None,
) -> tuple[int, int]:
    work_pool = _work_pool_name(sluice_cfg)
    deployments = []
    pipeline_count = 0
    rotation_count = 0

    for task in tasks:
        if not task.enabled:
            continue

        for pipeline in task.pipelines:
            if pipeline.cron and _pipeline_allowed(task, pipeline):
                deployments.append(
                    build_pipeline_deployment(task, pipeline, sluice_cfg, config_dir)
                )
                pipeline_count += 1

        effective_cron = _effective_cron(task, sluice_cfg)
        if effective_cron is not None:
            deployments.append(
                build_rotation_deployment(
                    task,
                    effective_cron,
                    sluice_cfg,
                    config_dir,
                    sluice_config_path,
                )
            )
            rotation_count += 1

    if not deployments:
        return (0, 0)

    deploy_kwargs: dict[str, Any] = {
        "work_pool_name": work_pool,
        "build": False,
        "push": False,
    }
    image = getattr(sluice_cfg.runtime, "image", None)
    if image:
        deploy_kwargs["image"] = image

    await prefect.deploy(*deployments, **deploy_kwargs)
    return (pipeline_count, rotation_count)


def _prefect_api_settings(runtime):
    updates = {}
    if runtime.prefect_api_url:
        updates[PREFECT_API_URL] = runtime.prefect_api_url

    auth_setting = getattr(runtime, "prefect_api_auth_string", None)
    if auth_setting:
        auth_string = resolve_env(auth_setting)
        if not auth_string.strip():
            raise ConfigError("runtime.prefect_api_auth_string must not be empty")
        updates[PREFECT_API_AUTH_STRING] = auth_string

    if not updates:
        return nullcontext()

    return temporary_settings(updates)
