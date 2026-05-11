from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from sluice.dag.config import (
    DeploymentsDef,
    PipelineDef,
    SinkDef,
    SourceDef,
    StageDef,
    TaskConfig,
    WindowDef,
)

_REQUIRED_FILES = (
    "task.toml",
    "sources.toml",
    "sinks.toml",
    "stages.toml",
    "pipelines.toml",
)


def _read_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as f:
        return tomllib.load(f)


def _split_named(entry: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    known = {"name", "type"}
    head = {k: v for k, v in entry.items() if k in known}
    params = {k: v for k, v in entry.items() if k not in known}
    return head, params


def load_task(task_dir: Path) -> TaskConfig:
    if not task_dir.exists():
        raise FileNotFoundError(f"task directory not found: {task_dir}")
    if not task_dir.is_dir():
        raise NotADirectoryError(f"task path is not a directory: {task_dir}")

    for filename in _REQUIRED_FILES:
        path = task_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"missing required file: {path}")

    task_doc = _read_toml(task_dir / "task.toml")["task"]
    sources_doc = _read_toml(task_dir / "sources.toml").get("sources", [])
    sinks_doc = _read_toml(task_dir / "sinks.toml").get("sinks", [])
    stages_doc = _read_toml(task_dir / "stages.toml").get("stages", [])
    pipelines_doc = _read_toml(task_dir / "pipelines.toml").get("pipelines", [])

    sources = []
    for entry in sources_doc:
        head, params = _split_named(entry)
        sources.append(SourceDef(name=head["name"], type=head["type"], params=params))

    sinks = []
    for entry in sinks_doc:
        head, params = _split_named(entry)
        sinks.append(SinkDef(name=head["name"], type=head["type"], params=params))

    stages = []
    for entry in stages_doc:
        head, params = _split_named(entry)
        stages.append(StageDef(name=head["name"], type=head["type"], params=params))

    pipelines = [
        PipelineDef(
            name=entry["name"],
            stages=entry["stages"],
            sources=entry.get("sources"),
            upstream_all=entry.get("upstream_all"),
            upstream_any=entry.get("upstream_any"),
            sinks=entry.get("sinks"),
            post_run_cleanup=entry.get("post_run_cleanup"),
            cron=entry.get("cron"),
            input_mode=entry.get("input_mode", "union"),
            max_attempts=int(entry.get("max_attempts", 3)),
            concurrency_strategy=entry.get("concurrency_strategy"),
        )
        for entry in pipelines_doc
    ]

    return TaskConfig(
        name=task_doc["name"],
        db_path=task_doc["db_path"],
        seen_db=task_doc["seen_db"],
        enabled=task_doc.get("enabled", True),
        timezone=task_doc.get("timezone"),
        window=WindowDef(
            anchor=task_doc["window"]["anchor"],
            length=task_doc["window"]["length"],
        ),
        deployments=DeploymentsDef(
            pipelines=task_doc["deployments"].get("pipelines", "all")
        ),
        sources=sources,
        sinks=sinks,
        stages=stages,
        pipelines=pipelines,
        task_dir=str(task_dir),
        rotation_cron=task_doc.get("rotation_cron"),
    )


def load_all_tasks(config_dir: Path) -> list[TaskConfig]:
    """Glob config_dir/tasks/*/task.toml and load each."""
    task_dirs = sorted(config_dir.glob("tasks/*/"))
    return [load_task(d) for d in task_dirs if (d / "task.toml").exists()]
