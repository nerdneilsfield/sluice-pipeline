from __future__ import annotations

import logging
import tomllib
from pathlib import Path
from typing import Any

from sluice.core.errors import ConfigError
from sluice.dag.config import (
    CONCURRENCY_STRATEGIES,
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

_INPUT_MODES = frozenset({None, "union", "intersection"})
logger = logging.getLogger(__name__)


def _read_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"failed to parse {path}: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise ConfigError(f"failed to read {path}: invalid encoding") from exc
    except OSError as exc:
        raise ConfigError(f"failed to read {path}: {exc}") from exc


def _type_name(value: Any) -> str:
    if isinstance(value, dict):
        return "table"
    if isinstance(value, list):
        return "list"
    return type(value).__name__


def _missing(errors: list[str], filename: str, path: str) -> None:
    errors.append(f"{filename}: missing '{path}'")


def _require_table(
    errors: list[str],
    doc: dict[str, Any],
    key: str,
    ctx: str,
    filename: str,
) -> dict[str, Any] | None:
    if key not in doc:
        _missing(errors, filename, f"{ctx}.{key}" if ctx else key)
        return None
    value = doc[key]
    if not isinstance(value, dict):
        path = f"{ctx}.{key}" if ctx else key
        errors.append(f"{filename}: '{path}' expected table, got {_type_name(value)}")
        return None
    return value


def _require_list(
    errors: list[str],
    doc: dict[str, Any],
    key: str,
    filename: str,
) -> list[Any]:
    if key not in doc:
        return []
    value = doc[key]
    if not isinstance(value, list):
        errors.append(f"{filename}: '{key}' expected list, got {_type_name(value)}")
        return []
    return value


def _require_path(
    errors: list[str],
    doc: dict[str, Any],
    keys: tuple[str, ...],
    filename: str,
) -> Any | None:
    current: Any = doc
    prefix: list[str] = []
    for key in keys:
        path = ".".join((*prefix, key))
        if not isinstance(current, dict):
            errors.append(
                f"{filename}: '{'.'.join(prefix)}' expected table, "
                f"got {_type_name(current)}"
            )
            return None
        if key not in current:
            _missing(errors, filename, path)
            return None
        current = current[key]
        prefix.append(key)
    return current


def _validate_named_entries(
    errors: list[str],
    doc: dict[str, Any],
    key: str,
    required: tuple[str, ...],
    filename: str,
) -> None:
    entries = _require_list(errors, doc, key, filename)
    for index, entry in enumerate(entries):
        ctx = f"{key}[{index}]"
        if not isinstance(entry, dict):
            errors.append(f"{filename}: {ctx}: expected table, got {_type_name(entry)}")
            continue
        for field in required:
            if field not in entry:
                errors.append(f"{filename}: {ctx}: missing '{field}'")


def _validate_task_doc(errors: list[str], doc: dict[str, Any]) -> None:
    filename = "task.toml"
    task = _require_table(errors, doc, "task", "", filename)
    if task is None:
        return

    for field in ("name", "db_path", "seen_db"):
        if field not in task:
            _missing(errors, filename, f"task.{field}")

    window = _require_table(errors, task, "window", "task", filename)
    if window is not None:
        for field in ("anchor", "length"):
            if field not in window:
                _missing(errors, filename, f"task.window.{field}")

    deployments = _require_table(errors, task, "deployments", "task", filename)
    if deployments is not None and "pipelines" in deployments:
        pipelines = deployments["pipelines"]
        if pipelines != "all" and not isinstance(pipelines, list):
            errors.append(
                "task.toml: 'task.deployments.pipelines' must be 'all' or list, "
                f"got {_type_name(pipelines)}"
            )


def _validate_pipeline_values(errors: list[str], entry: dict[str, Any], index: int) -> None:
    ctx = f"pipelines[{index}]"
    if "stages" in entry and not isinstance(entry["stages"], list):
        errors.append(
            f"pipelines.toml: {ctx}.stages expected list, "
            f"got {_type_name(entry['stages'])}"
        )

    strategy = entry.get("concurrency_strategy")
    if strategy is not None and strategy not in CONCURRENCY_STRATEGIES:
        allowed = "/".join(sorted(CONCURRENCY_STRATEGIES))
        errors.append(
            f"pipelines.toml: {ctx}: invalid concurrency_strategy {strategy!r} "
            f"(must be {allowed})"
        )

    input_mode = entry.get("input_mode", "union")
    if input_mode not in _INPUT_MODES:
        errors.append(
            f"pipelines.toml: {ctx}: invalid input_mode {input_mode!r} "
            "(must be union/intersection)"
        )

    if "max_attempts" in entry:
        try:
            int(entry["max_attempts"])
        except (OverflowError, TypeError, ValueError):
            errors.append(f"pipelines.toml: {ctx}: max_attempts must be an integer")


def _validate_pipelines_doc(errors: list[str], doc: dict[str, Any]) -> None:
    filename = "pipelines.toml"
    entries = _require_list(errors, doc, "pipelines", filename)
    for index, entry in enumerate(entries):
        ctx = f"pipelines[{index}]"
        if not isinstance(entry, dict):
            errors.append(f"{filename}: {ctx}: expected table, got {_type_name(entry)}")
            continue
        for field in ("name", "stages"):
            if field not in entry:
                errors.append(f"{filename}: {ctx}: missing '{field}'")
        _validate_pipeline_values(errors, entry, index)


def _validate_structure(docs: dict[str, dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    _validate_task_doc(errors, docs["task.toml"])
    _validate_named_entries(
        errors, docs["sources.toml"], "sources", ("name", "type"), "sources.toml"
    )
    _validate_named_entries(errors, docs["sinks.toml"], "sinks", ("name", "type"), "sinks.toml")
    _validate_named_entries(errors, docs["stages.toml"], "stages", ("name", "type"), "stages.toml")
    _validate_pipelines_doc(errors, docs["pipelines.toml"])
    return errors


def _split_named(entry: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    known = {"name", "type"}
    head = {k: v for k, v in entry.items() if k in known}
    params = {k: v for k, v in entry.items() if k not in known}
    return head, params


def _pipeline_concurrency_strategy(entry: dict[str, Any]) -> str | None:
    strategy = entry.get("concurrency_strategy")
    if strategy is not None and strategy not in CONCURRENCY_STRATEGIES:
        raise ConfigError(
            "pipeline.concurrency_strategy must be one of queue/enqueue/cancel_new, "
            f"got {strategy!r}"
        )
    return strategy


def load_task(task_dir: Path) -> TaskConfig:
    if not task_dir.exists():
        raise FileNotFoundError(f"task directory not found: {task_dir}")
    if not task_dir.is_dir():
        raise NotADirectoryError(f"task path is not a directory: {task_dir}")

    for filename in _REQUIRED_FILES:
        path = task_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"missing required file: {path}")

    docs = {filename: _read_toml(task_dir / filename) for filename in _REQUIRED_FILES}
    errors = _validate_structure(docs)
    if errors:
        raise ConfigError(
            f"invalid config in {task_dir}:\n  - " + "\n  - ".join(errors)
        )

    try:
        task_doc = docs["task.toml"]["task"]
        sources_doc = docs["sources.toml"].get("sources", [])
        sinks_doc = docs["sinks.toml"].get("sinks", [])
        stages_doc = docs["stages.toml"].get("stages", [])
        pipelines_doc = docs["pipelines.toml"].get("pipelines", [])

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
                concurrency_strategy=_pipeline_concurrency_strategy(entry),
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
    except (AttributeError, KeyError, OverflowError, TypeError, ValueError) as exc:
        raise ConfigError(f"invalid config in {task_dir}: {exc}") from exc


def load_all_tasks(config_dir: Path) -> list[TaskConfig]:
    """Glob config_dir/tasks/*/task.toml and load each."""
    task_dirs = sorted(config_dir.glob("tasks/*/"))
    tasks = []
    for task_dir in task_dirs:
        if not (task_dir / "task.toml").exists():
            logger.warning("skipping %s: no task.toml", task_dir)
            continue
        tasks.append(load_task(task_dir))
    return tasks
