"""Loading and validation of the v4 YAML definition.

The objects produced here are the authoring boundary for the rest of Sluice.
They are deliberately immutable: a resolved runtime may hand a thawed copy of
one ``with`` mapping to user code without allowing that code to alter the
definition used by a later execution or cache lookup.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml

from sluice.canonical import canonical_json
from sluice.contracts import (
    BatchPolicy,
    Definition,
    DeliveryDef,
    ExecutionPolicy,
    FrozenJSON,
    JSONValue,
    NodeDef,
    PipelineDef,
    SourceDef,
    StageDef,
    TaskDef,
)
from sluice.errors import DefinitionError, GraphError


# This map is intentionally a literal.  It is a validation aid only; it is
# not a plugin registry and does not import or discover implementation modules.
BUILTIN_TASK_MODES: dict[str, str] = {
    "sluice.tasks.dedup": "batch",
    "sluice.tasks.filter": "batch",
    "sluice.tasks.sort": "batch",
    "sluice.tasks.limit": "batch",
    "sluice.tasks.fetch_webpage": "each",
    "sluice.tasks.extract_content": "each",
    "sluice.tasks.to_markdown": "each",
    "sluice.tasks.llm_extract": "each",
    "sluice.tasks.llm_summary": "batch",
    "sluice.tasks.render_email": "each",
    "sluice.tasks.render_telegram": "each",
}


_TOP_LEVEL_KEYS = {"version", "database", "sources", "stages", "pipelines"}
_SOURCE_KEYS = {"use", "schedule", "with", "timeout", "retries"}
_STAGE_KEYS = {"input", "tasks", "output", "output_schema", "cache_version"}
_TASK_KEYS = {"use", "with"}
_PIPELINE_KEYS = {"nodes"}
_NODE_KEYS = {
    "stage",
    "from",
    "schedule",
    "batch",
    "deliver",
    "timeout",
    "retries",
    "concurrency",
}
_BATCH_KEYS = {"count", "max_wait", "on_schedule", "include_updates"}
_DELIVERY_KEYS = {"use", "with"}
_FILE_CONFIG_KEYS = ("prompt", "schema", "output_schema")


def _reject_duplicate_yaml_keys(text: str) -> None:
    """Detect duplicate mapping keys before ``safe_load`` overwrites one."""

    try:
        root = yaml.compose(text, Loader=yaml.SafeLoader)
    except yaml.YAMLError as error:
        raise DefinitionError("invalid YAML") from error
    if root is None:
        return

    def walk(node: yaml.nodes.Node, active: set[int]) -> None:
        marker = id(node)
        if marker in active:
            raise DefinitionError("recursive YAML alias")
        active.add(marker)
        try:
            if isinstance(node, yaml.nodes.MappingNode):
                seen: set[tuple[object, object]] = set()
                for key, value in node.value:
                    key_marker = (key.tag, getattr(key, "value", repr(key)))
                    if key_marker in seen:
                        raise DefinitionError(
                            f"duplicate YAML key: {getattr(key, 'value', key_marker[1])!r}"
                        )
                    seen.add(key_marker)
                    walk(key, active)
                    walk(value, active)
            elif isinstance(node, yaml.nodes.SequenceNode):
                for item in node.value:
                    walk(item, active)
        finally:
            active.remove(marker)

    walk(root, set())


def _mapping(value: object, label: str) -> dict[str, object]:
    """Return a plain mapping with string keys or raise ``DefinitionError``."""

    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise DefinitionError(f"{label} must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise DefinitionError(f"{label} keys must be strings")
    return dict(value)


def _check_keys(spec: Mapping[str, object], allowed: set[str], label: str) -> None:
    if any(not isinstance(key, str) for key in spec):
        raise DefinitionError(f"keys in {label} must be strings")
    unknown = set(spec) - allowed
    if unknown:
        raise DefinitionError(f"unknown keys in {label}: {sorted(unknown)}")


def _required_string(spec: Mapping[str, object], key: str, label: str) -> str:
    value = spec.get(key)
    if not isinstance(value, str) or not value:
        raise DefinitionError(f"{label}.{key} must be a non-empty string")
    return value


def _integer(value: object, label: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DefinitionError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise DefinitionError(f"{label} must be at least {minimum}")
    return value


def freeze_json(value: object, _active: set[int] | None = None) -> FrozenJSON:
    """Recursively freeze JSON data into mappings and tuples.

    PyYAML's safe loader can still produce scalar objects (for example a
    timestamp) that are not JSON values.  Rejecting them at this boundary
    keeps canonical snapshots and user-callable inputs deterministic.
    """

    active = set() if _active is None else _active
    if isinstance(value, Mapping):
        marker = id(value)
        if marker in active:
            raise DefinitionError("recursive JSON value")
        active.add(marker)
        try:
            frozen: dict[str, FrozenJSON] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise DefinitionError("JSON object keys must be strings")
                frozen[key] = freeze_json(item, active)
            return MappingProxyType(frozen)
        finally:
            active.remove(marker)
    if isinstance(value, (list, tuple)):
        marker = id(value)
        if marker in active:
            raise DefinitionError("recursive JSON value")
        active.add(marker)
        try:
            return tuple(freeze_json(item, active) for item in value)
        finally:
            active.remove(marker)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise DefinitionError("JSON numbers must be finite")
        return value
    raise DefinitionError(f"value is not JSON-compatible: {value!r}")


def thaw_json(value: object) -> object:
    """Return a fresh mutable JSON copy of an immutable definition value."""

    if isinstance(value, Mapping):
        return {key: thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value


def parse_interval(text: str) -> timedelta:
    """Parse a positive whole-minute/hour interval (``m`` or ``h``)."""

    if not isinstance(text, str):
        raise DefinitionError("duration must be a positive integer followed by m or h")
    match = re.fullmatch(r"([1-9][0-9]*)(m|h)", text)
    if not match:
        raise DefinitionError("duration must be a positive integer followed by m or h")
    amount, unit = int(match.group(1)), match.group(2)
    try:
        return timedelta(minutes=amount) if unit == "m" else timedelta(hours=amount)
    except OverflowError as error:
        raise DefinitionError("duration is too large") from error


def parse_timeout(text: str) -> timedelta:
    """Parse a positive whole-second/minute/hour timeout."""

    if not isinstance(text, str):
        raise DefinitionError("timeout must be a positive integer followed by s, m, or h")
    match = re.fullmatch(r"([1-9][0-9]*)(s|m|h)", text)
    if not match:
        raise DefinitionError("timeout must be a positive integer followed by s, m, or h")
    amount, unit = int(match.group(1)), match.group(2)
    try:
        return {
            "s": timedelta(seconds=amount),
            "m": timedelta(minutes=amount),
            "h": timedelta(hours=amount),
        }[unit]
    except OverflowError as error:
        raise DefinitionError("timeout is too large") from error


def _optional_interval(value: object, label: str) -> timedelta | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise DefinitionError(f"{label} must be an interval string")
    try:
        return parse_interval(value)
    except DefinitionError as error:
        raise DefinitionError(f"invalid {label}: {value!r}") from error


def _optional_timeout(value: object, label: str) -> timedelta | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise DefinitionError(f"{label} must be a timeout string")
    try:
        return parse_timeout(value)
    except DefinitionError as error:
        raise DefinitionError(f"invalid {label}: {value!r}") from error


def _decode_policy(spec: Mapping[str, object], label: str) -> ExecutionPolicy:
    timeout = _optional_timeout(spec.get("timeout"), f"{label}.timeout")
    retries = _integer(spec.get("retries", 0), f"{label}.retries", minimum=0)
    concurrency = _integer(
        spec.get("concurrency", 1), f"{label}.concurrency", minimum=1
    )
    return ExecutionPolicy(timeout=timeout, retries=retries, concurrency=concurrency)


def _decode_source_policy(spec: Mapping[str, object], label: str) -> ExecutionPolicy:
    if "concurrency" in spec:
        raise DefinitionError(f"{label} cannot set concurrency")
    timeout = _optional_timeout(spec.get("timeout"), f"{label}.timeout")
    retries = _integer(spec.get("retries", 0), f"{label}.retries", minimum=0)
    return ExecutionPolicy(timeout=timeout, retries=retries, concurrency=1)


def _decode_sources(raw: Mapping[str, object]) -> dict[str, SourceDef]:
    decoded: dict[str, SourceDef] = {}
    for name, value in raw.items():
        if not name:
            raise DefinitionError("source names must be non-empty")
        spec = _mapping(value, f"sources.{name}")
        _check_keys(spec, _SOURCE_KEYS | {"concurrency"}, f"sources.{name}")
        use = _required_string(spec, "use", f"sources.{name}")
        decoded[name] = SourceDef(
            name=name,
            use=use,
            schedule=_optional_interval(spec.get("schedule"), f"sources.{name}.schedule"),
            with_config=freeze_json(_mapping(spec.get("with"), f"sources.{name}.with")),
            policy=_decode_source_policy(spec, f"sources.{name}"),
        )
    return decoded


def _normalize_task_file_paths(config: dict[str, object], base_dir: Path) -> dict[str, object]:
    normalized = dict(config)
    for key in _FILE_CONFIG_KEYS:
        value = normalized.get(key)
        if isinstance(value, str):
            normalized[key] = str((base_dir / value).resolve())
    return normalized


def _decode_task(spec: Mapping[str, object], label: str, base_dir: Path) -> TaskDef:
    _check_keys(spec, _TASK_KEYS, label)
    use = _required_string(spec, "use", label)
    config = _mapping(spec.get("with"), f"{label}.with")
    config = _normalize_task_file_paths(config, base_dir)
    return TaskDef(use=use, with_config=freeze_json(config))


def _decode_stages(raw: Mapping[str, object], base_dir: Path) -> dict[str, StageDef]:
    decoded: dict[str, StageDef] = {}
    for name, value in raw.items():
        if not name:
            raise DefinitionError("stage names must be non-empty")
        spec = _mapping(value, f"stages.{name}")
        _check_keys(spec, _STAGE_KEYS, f"stages.{name}")
        input_mode = spec.get("input")
        if input_mode not in {"each", "batch"}:
            raise DefinitionError(f"stages.{name}.input must be each or batch")
        raw_tasks = spec.get("tasks")
        if not isinstance(raw_tasks, (list, tuple)) or not raw_tasks:
            raise DefinitionError(f"stages.{name}.tasks must be a non-empty list")
        tasks = tuple(
            _decode_task(_mapping(task, f"stages.{name}.tasks[{index}]"), f"stages.{name}.tasks[{index}]", base_dir)
            for index, task in enumerate(raw_tasks)
        )
        for task in tasks:
            expected_mode = BUILTIN_TASK_MODES.get(task.use)
            if expected_mode is not None and expected_mode != input_mode:
                raise DefinitionError(f"{task.use} requires {expected_mode} input")
            invalid = task.with_config.get("invalid")
            if invalid is not None and not (
                invalid == "drop"
                and input_mode == "each"
                and task.use == "sluice.tasks.llm_extract"
            ):
                raise DefinitionError(
                    "invalid: drop is only valid for each llm_extract"
                )

        output_value = spec.get("output", spec.get("output_schema"))
        if output_value is None:
            output_schema: Mapping[str, FrozenJSON] | None = None
        else:
            output_schema = freeze_json(_mapping(output_value, f"stages.{name}.output"))  # type: ignore[assignment]
        cache_version = _integer(
            spec.get("cache_version", 1), f"stages.{name}.cache_version", minimum=1
        )
        decoded[name] = StageDef(
            name=name,
            input_mode=input_mode,
            tasks=tasks,
            output_schema=output_schema,
            cache_version=cache_version,
            canonical_config_json=canonical_stage_config(
                name, input_mode, tasks, output_schema, cache_version, base_dir
            ),
        )
    return decoded


def _decode_batch(value: object, label: str) -> BatchPolicy | None:
    if value is None:
        return None
    spec = _mapping(value, label)
    _check_keys(spec, _BATCH_KEYS, label)
    count = None
    if "count" in spec:
        count = _integer(spec["count"], f"{label}.count", minimum=1)
    max_wait = _optional_interval(spec.get("max_wait"), f"{label}.max_wait")
    on_schedule = spec.get("on_schedule", False)
    include_updates = spec.get("include_updates", False)
    if not isinstance(on_schedule, bool):
        raise DefinitionError(f"{label}.on_schedule must be a boolean")
    if not isinstance(include_updates, bool):
        raise DefinitionError(f"{label}.include_updates must be a boolean")
    return BatchPolicy(
        count=count,
        max_wait=max_wait,
        on_schedule=on_schedule,
        include_updates=include_updates,
    )


def _decode_delivery(value: object, label: str) -> DeliveryDef:
    spec = _mapping(value, label)
    _check_keys(spec, _DELIVERY_KEYS, label)
    return DeliveryDef(
        use=_required_string(spec, "use", label),
        with_config=freeze_json(_mapping(spec.get("with"), f"{label}.with")),
    )


def _decode_node(name: str, spec: Mapping[str, object], label: str) -> NodeDef:
    _check_keys(spec, _NODE_KEYS, label)
    raw_refs = spec.get("from", ())
    if isinstance(raw_refs, str):
        from_refs = (raw_refs,)
    elif isinstance(raw_refs, (list, tuple)):
        if any(not isinstance(ref, str) or not ref for ref in raw_refs):
            raise GraphError(f"{label}.from must contain non-empty strings")
        from_refs = tuple(raw_refs)
    else:
        raise GraphError(f"{label}.from must be a string or list of strings")
    if len(set(from_refs)) != len(from_refs):
        raise GraphError(f"{label}.from contains duplicate references")
    stage_value = spec.get("stage")
    if stage_value is not None and (not isinstance(stage_value, str) or not stage_value):
        raise DefinitionError(f"{label}.stage must be a non-empty string")
    deliver = (
        _decode_delivery(spec["deliver"], f"{label}.deliver")
        if "deliver" in spec
        else None
    )
    return NodeDef(
        name=name,
        stage=stage_value,
        from_refs=from_refs,
        schedule=_optional_interval(spec.get("schedule"), f"{label}.schedule"),
        batch=_decode_batch(spec.get("batch"), f"{label}.batch"),
        policy=_decode_policy(spec, label),
        deliver=deliver,
    )


def _decode_pipelines(raw: Mapping[str, object]) -> dict[str, PipelineDef]:
    decoded: dict[str, PipelineDef] = {}
    for name, value in raw.items():
        if not name:
            raise DefinitionError("pipeline names must be non-empty")
        spec = _mapping(value, f"pipelines.{name}")
        _check_keys(spec, _PIPELINE_KEYS, f"pipelines.{name}")
        raw_nodes = _mapping(spec.get("nodes"), f"pipelines.{name}.nodes")
        nodes = {
            node_name: _decode_node(
                node_name,
                _mapping(node, f"pipelines.{name}.nodes.{node_name}"),
                f"pipelines.{name}.nodes.{node_name}",
            )
            for node_name, node in raw_nodes.items()
        }
        decoded[name] = PipelineDef(name=name, nodes=MappingProxyType(nodes))
    return decoded


def _decode_definition(raw: Mapping[str, object], base_dir: Path) -> Definition:
    _check_keys(raw, _TOP_LEVEL_KEYS, "definition")
    version = raw.get("version")
    if type(version) is not int or version != 1:
        raise DefinitionError("version: 1 is required")
    database = raw.get("database", ".sluice/sluice.db")
    if not isinstance(database, str) or not database:
        raise DefinitionError("database must be a non-empty string")
    return Definition(
        version=1,
        database=database,
        sources=MappingProxyType(_decode_sources(_mapping(raw.get("sources"), "sources"))),
        stages=MappingProxyType(
            _decode_stages(_mapping(raw.get("stages"), "stages"), base_dir)
        ),
        pipelines=MappingProxyType(
            _decode_pipelines(_mapping(raw.get("pipelines"), "pipelines"))
        ),
    )


def load_definition(path: str | Path) -> Definition:
    """Safely load, freeze, and validate one YAML definition."""

    definition_path = Path(path)
    try:
        text = definition_path.read_text(encoding="utf-8")
        _reject_duplicate_yaml_keys(text)
        raw = yaml.safe_load(text)
    except OSError as error:
        raise DefinitionError(f"cannot read definition {definition_path}") from error
    except yaml.YAMLError as error:
        raise DefinitionError(f"invalid YAML in {definition_path}") from error
    if not isinstance(raw, Mapping):
        raise DefinitionError("definition must be a mapping")
    definition = _decode_definition(raw, definition_path.parent.resolve())
    validate_definition(definition)
    return definition


def _validate_policy(policy: ExecutionPolicy, label: str) -> None:
    if policy.timeout is not None and (
        not isinstance(policy.timeout, timedelta) or policy.timeout <= timedelta(0)
    ):
        raise DefinitionError(f"{label}.timeout must be positive")
    _integer(policy.retries, f"{label}.retries", minimum=0)
    _integer(policy.concurrency, f"{label}.concurrency", minimum=1)


def _validate_batch_policy(
    batch: BatchPolicy | None,
    stage: StageDef,
    node: NodeDef,
    label: str,
) -> None:
    if batch is None:
        if stage.input_mode == "batch":
            raise GraphError(
                f"{label} uses batch Stage {stage.name} without a batch trigger"
            )
        return
    if stage.input_mode == "each":
        raise GraphError(f"{label} cannot declare batch for an each Stage")
    if batch.count is not None:
        _integer(batch.count, f"{label}.batch.count", minimum=1)
    if batch.max_wait is not None and (
        not isinstance(batch.max_wait, timedelta)
        or batch.max_wait <= timedelta(0)
    ):
        raise DefinitionError(f"{label}.batch.max_wait must be positive")
    if not isinstance(batch.on_schedule, bool) or not isinstance(
        batch.include_updates, bool
    ):
        raise DefinitionError(f"{label}.batch flags must be booleans")
    if batch.count is None and batch.max_wait is None and not batch.on_schedule:
        raise GraphError(f"{label}.batch needs count, max_wait, or on_schedule")
    if batch.on_schedule and node.schedule is None:
        raise GraphError(f"{label}.batch.on_schedule requires a node schedule")


def _validate_pipeline(pipeline: PipelineDef, definition: Definition) -> None:
    nodes = pipeline.nodes
    if not isinstance(nodes, Mapping):
        raise GraphError(f"pipelines.{pipeline.name}.nodes must be a mapping")
    deliveries = 0
    for name, node in nodes.items():
        if name != node.name:
            raise GraphError(f"node key/name mismatch in pipeline {pipeline.name}")
        if node.deliver is not None:
            deliveries += 1
            if node.stage is not None:
                raise GraphError(f"delivery node {pipeline.name}.{name} cannot have a stage")
            if node.batch is not None:
                raise GraphError(f"delivery node {pipeline.name}.{name} cannot batch")
        elif node.stage is None:
            raise GraphError(f"node {pipeline.name}.{name} needs a stage or delivery")
        else:
            try:
                stage = definition.stages[node.stage]
            except KeyError as error:
                raise GraphError(
                    f"unknown Stage {node.stage!r} in pipeline {pipeline.name}.{name}"
                ) from error
            _validate_batch_policy(node.batch, stage, node, f"pipelines.{pipeline.name}.{name}")
        _validate_policy(node.policy, f"pipelines.{pipeline.name}.{name}")
        if node.schedule is not None and (
            not isinstance(node.schedule, timedelta) or node.schedule <= timedelta(0)
        ):
            raise DefinitionError(f"pipelines.{pipeline.name}.{name}.schedule must be positive")
        if node.deliver is not None and not node.from_refs:
            raise GraphError(f"delivery node {pipeline.name}.{name} needs an input")
        if node.stage is not None and not node.from_refs:
            raise GraphError(f"stage node {pipeline.name}.{name} needs an input")

    if deliveries == 0:
        raise GraphError(f"pipeline {pipeline.name} must have a Delivery endpoint")

    # Validate edge targets before traversing them.  Edges are directional from
    # a node to its upstream inputs, so DFS naturally catches self/cross cycles.
    for name, node in nodes.items():
        for ref in node.from_refs:
            if ref.startswith("source."):
                source_name = ref[len("source.") :]
                if not source_name or source_name not in definition.sources:
                    raise GraphError(
                        f"unknown Source reference {ref!r} in {pipeline.name}.{name}"
                    )
            elif ref not in nodes:
                raise GraphError(
                    f"unknown node reference {ref!r} in {pipeline.name}.{name}"
                )

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise GraphError(f"cycle detected in pipeline {pipeline.name} at {name}")
        if name in visited:
            return
        visiting.add(name)
        for ref in nodes[name].from_refs:
            if ref in nodes:
                visit(ref)
        visiting.remove(name)
        visited.add(name)

    for name in nodes:
        visit(name)


def validate_definition(definition: Definition) -> None:
    """Validate policies, built-in modes, references, and Pipeline DAGs."""

    if not isinstance(definition, Definition):
        raise DefinitionError("expected a Definition")
    if type(definition.version) is not int or definition.version != 1:
        raise DefinitionError("version: 1 is required")
    if not isinstance(definition.database, str) or not definition.database:
        raise DefinitionError("database must be a non-empty string")
    if not isinstance(definition.sources, Mapping) or not isinstance(
        definition.stages, Mapping
    ) or not isinstance(definition.pipelines, Mapping):
        raise DefinitionError("definition collections must be mappings")

    for name, source in definition.sources.items():
        if name != source.name:
            raise DefinitionError(f"source key/name mismatch: {name}")
        if not isinstance(source.use, str) or not source.use:
            raise DefinitionError(f"sources.{name}.use must be a non-empty string")
        _validate_policy(source.policy, f"sources.{name}")
        if source.policy.concurrency != 1:
            raise DefinitionError(f"sources.{name} cannot set concurrency")
        if source.schedule is not None and (
            not isinstance(source.schedule, timedelta)
            or source.schedule <= timedelta(0)
        ):
            raise DefinitionError(f"sources.{name}.schedule must be positive")
        freeze_json(source.with_config)

    for name, stage in definition.stages.items():
        if name != stage.name:
            raise DefinitionError(f"stage key/name mismatch: {name}")
        if stage.input_mode not in {"each", "batch"}:
            raise DefinitionError(f"stages.{name}.input must be each or batch")
        if not stage.tasks:
            raise DefinitionError(f"stages.{name}.tasks must be non-empty")
        _integer(stage.cache_version, f"stages.{name}.cache_version", minimum=1)
        for task in stage.tasks:
            if not isinstance(task.use, str) or not task.use:
                raise DefinitionError(f"stages.{name} task use must be non-empty")
            expected_mode = BUILTIN_TASK_MODES.get(task.use)
            if expected_mode is not None and expected_mode != stage.input_mode:
                raise DefinitionError(f"{task.use} requires {expected_mode} input")
            invalid = task.with_config.get("invalid")
            if invalid is not None and not (
                invalid == "drop"
                and stage.input_mode == "each"
                and task.use == "sluice.tasks.llm_extract"
            ):
                raise DefinitionError(
                    "invalid: drop is only valid for each llm_extract"
                )
            freeze_json(task.with_config)
        if stage.output_schema is not None:
            freeze_json(stage.output_schema)

    for pipeline in definition.pipelines.values():
        _validate_pipeline(pipeline, definition)


def canonical_stage_config(
    name: str,
    input_mode: str,
    tasks: tuple[TaskDef, ...],
    output_schema: Mapping[str, JSONValue] | None,
    cache_version: int,
    base_dir: str | Path,
) -> str:
    """Serialize the complete result-affecting Stage snapshot deterministically."""

    if input_mode not in {"each", "batch"}:
        raise DefinitionError("Stage input must be each or batch")
    base_path = Path(base_dir).resolve()
    values: list[dict[str, object]] = []
    for task in tasks:
        config_value = thaw_json(task.with_config)
        if not isinstance(config_value, dict):
            raise DefinitionError("Task with configuration must be a mapping")
        config = dict(config_value)
        for key in _FILE_CONFIG_KEYS:
            value = config.get(key)
            if not isinstance(value, str):
                continue
            file_path = Path(value)
            if not file_path.is_absolute():
                file_path = (base_path / file_path).resolve()
            else:
                file_path = file_path.resolve()
            try:
                digest = hashlib.sha256(file_path.read_bytes()).hexdigest()
            except OSError as error:
                raise DefinitionError(f"cannot read Stage file {file_path}") from error
            config[key] = str(file_path)
            config[f"{key}_digest"] = digest
        values.append({"use": task.use, "with": config})
    try:
        return canonical_json(
            {
                "name": name,
                "input": input_mode,
                "cache_version": cache_version,
                "tasks": values,
                "output_schema": thaw_json(output_schema),
            }
        )
    except Exception as error:
        if isinstance(error, DefinitionError):
            raise
        raise DefinitionError("Stage configuration is not JSON-compatible") from error


def verify_stage_files(stage: StageDef) -> None:
    """Reject edits to prompt/schema files after a Stage was loaded."""

    try:
        snapshot = json.loads(stage.canonical_config_json)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise DefinitionError("invalid canonical Stage configuration") from error
    for task in snapshot.get("tasks", []):
        config = task.get("with", {})
        if not isinstance(config, Mapping):
            continue
        for key in _FILE_CONFIG_KEYS:
            value = config.get(key)
            digest_key = f"{key}_digest"
            if not isinstance(value, str) or not isinstance(config.get(digest_key), str):
                continue
            file_path = Path(value)
            try:
                current_digest = hashlib.sha256(file_path.read_bytes()).hexdigest()
            except OSError as error:
                raise DefinitionError(f"stage file changed or is missing: {file_path}") from error
            if current_digest != config[digest_key]:
                raise DefinitionError(f"stage file changed or is missing: {file_path}")
