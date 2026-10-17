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
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Literal
from urllib.parse import urlsplit

import yaml

from sluice.canonical import canonical_json, web_binding_projection
from sluice.contracts import (
    BatchPolicy,
    Definition,
    DeliveryDef,
    ExecutionPolicy,
    FrozenJSON,
    JSONValue,
    LLMConfig,
    NodeDef,
    PipelineDef,
    SourceDef,
    StageDef,
    TaskDef,
)
from sluice.errors import DefinitionError, ExecutionError, GraphError
from sluice.sinks.file import SINK_PATH as FILE_SINK
from sluice.sinks.file import resolve_file_root, resolve_inside
from sluice.web.config import WebRegistryConfig, decode_web_config, expand_web_binding
from sluice.web.contracts import (
    ExtractorBinding,
    ProviderBinding,
    QualityPolicy,
    WebBinding,
    WebOutput,
)

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
    "sluice.tasks.to_text": "each",
    "sluice.tasks.llm_extract": "each",
    "sluice.tasks.llm_summary": "batch",
    "sluice.tasks.render_email": "each",
    "sluice.tasks.render_telegram": "each",
}


_TOP_LEVEL_KEYS = {
    "version",
    "database",
    "file_root",
    "web",
    "llm",
    "sources",
    "stages",
    "pipelines",
}
_LLM_KEYS = {"base_url", "api_key", "timeout_seconds", "max_attempts"}
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
_WEB_BINDING_KEYS = {
    "version",
    "direct_provider",
    "fetch_chain",
    "extract_chain",
    "quality",
    "output",
}
_WEB_PROVIDER_KEYS = {
    "name",
    "type",
    "options",
    "base_url",
    "timeout",
    "connect_timeout",
    "transport_retries",
    "max_body_bytes",
    "concurrency",
    "cooldown",
    "capabilities",
    "service_headers",
    "target_headers",
    "cookies",
    "proxy",
    "target_policy",
    "timeout_seconds",
    "only_main_content",
    "wait_for_selector",
    "wait_for",
    "mobile",
    "actions",
}
_WEB_EXTRACTOR_KEYS = {"name", "type", "options"}
_WEB_QUALITY_KEYS = {
    "min_chars",
    "min_words",
    "max_link_ratio",
    "reject_challenges",
    "max_body_bytes",
}
_WEB_FETCH_CHAIN_KEYS = {"name", "providers"}
_WEB_EXTRACT_CHAIN_KEYS = {"name", "extractors"}
_SNAPSHOT_ENV_REFERENCE = re.compile(r"^env:[A-Z_][A-Z0-9_]*$")


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


def freeze_mapping(value: Mapping[str, object]) -> Mapping[str, FrozenJSON]:
    """冻结已验证的配置映射，并保留其映射形状。"""

    frozen = freeze_json(value)
    if not isinstance(frozen, Mapping):
        raise DefinitionError("configuration must be a mapping")
    return frozen


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
        raise DefinitionError(
            "timeout must be a positive integer followed by s, m, or h"
        )
    match = re.fullmatch(r"([1-9][0-9]*)(s|m|h)", text)
    if not match:
        raise DefinitionError(
            "timeout must be a positive integer followed by s, m, or h"
        )
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


def _decode_sources(
    raw: Mapping[str, object], registry: WebRegistryConfig
) -> dict[str, SourceDef]:
    decoded: dict[str, SourceDef] = {}
    for name, value in raw.items():
        if not name:
            raise DefinitionError("source names must be non-empty")
        spec = _mapping(value, f"sources.{name}")
        _check_keys(spec, _SOURCE_KEYS | {"concurrency"}, f"sources.{name}")
        use = _required_string(spec, "use", f"sources.{name}")
        with_config = _mapping(spec.get("with"), f"sources.{name}.with")
        try:
            web_binding = expand_web_binding(use, with_config, registry)
        except DefinitionError as error:
            raise DefinitionError(f"invalid sources.{name}: {error}") from error
        decoded[name] = SourceDef(
            name=name,
            use=use,
            schedule=_optional_interval(
                spec.get("schedule"), f"sources.{name}.schedule"
            ),
            with_config=freeze_mapping(with_config),
            policy=_decode_source_policy(spec, f"sources.{name}"),
            web_binding=web_binding,
        )
    return decoded


def _normalize_task_file_paths(
    config: dict[str, object], base_dir: Path
) -> dict[str, object]:
    normalized = dict(config)
    for key in _FILE_CONFIG_KEYS:
        value = normalized.get(key)
        if isinstance(value, str):
            normalized[key] = str((base_dir / value).resolve())
    return normalized


def _decode_task(
    spec: Mapping[str, object],
    label: str,
    base_dir: Path,
    registry: WebRegistryConfig,
) -> TaskDef:
    _check_keys(spec, _TASK_KEYS, label)
    use = _required_string(spec, "use", label)
    config = _mapping(spec.get("with"), f"{label}.with")
    config = _normalize_task_file_paths(config, base_dir)
    try:
        web_binding = expand_web_binding(use, config, registry)
    except DefinitionError as error:
        raise DefinitionError(f"invalid {label}: {error}") from error
    return TaskDef(use=use, with_config=freeze_mapping(config), web_binding=web_binding)


def _validate_artifact_task_chain(name: str, tasks: tuple[TaskDef, ...]) -> None:
    artifact_tasks = {"sluice.tasks.fetch_webpage", "sluice.tasks.extract_content"}
    projectors = {"sluice.tasks.to_markdown", "sluice.tasks.to_text"}
    if not any(task.use in artifact_tasks for task in tasks):
        return

    uses = tuple(task.use for task in tasks)
    fetch_count = uses.count("sluice.tasks.fetch_webpage")
    extract_count = uses.count("sluice.tasks.extract_content")
    projector_count = sum(use in projectors for use in uses)
    valid = (
        projector_count == 1
        and uses[-1] in projectors
        and (
            (fetch_count == 1 and extract_count == 0 and len(uses) == 2)
            or (
                fetch_count == 1
                and extract_count == 1
                and len(uses) == 3
                and uses[1] == "sluice.tasks.extract_content"
            )
            or (fetch_count == 0 and extract_count == 1 and len(uses) == 2)
        )
    )
    if not valid:
        raise DefinitionError(
            f"stages.{name} Artifact-producing web Tasks must use only fetch_webpage, "
            "an optional adjacent extract_content, and one terminal to_markdown or to_text"
        )


def _decode_stages(
    raw: Mapping[str, object], base_dir: Path, registry: WebRegistryConfig
) -> dict[str, StageDef]:
    decoded: dict[str, StageDef] = {}
    for name, value in raw.items():
        if not name:
            raise DefinitionError("stage names must be non-empty")
        spec = _mapping(value, f"stages.{name}")
        _check_keys(spec, _STAGE_KEYS, f"stages.{name}")
        raw_input_mode = spec.get("input")
        if raw_input_mode == "each":
            input_mode: Literal["each", "batch"] = "each"
        elif raw_input_mode == "batch":
            input_mode = "batch"
        else:
            raise DefinitionError(f"stages.{name}.input must be each or batch")
        raw_tasks = spec.get("tasks")
        if not isinstance(raw_tasks, (list, tuple)) or not raw_tasks:
            raise DefinitionError(f"stages.{name}.tasks must be a non-empty list")
        tasks = tuple(
            _decode_task(
                _mapping(task, f"stages.{name}.tasks[{index}]"),
                f"stages.{name}.tasks[{index}]",
                base_dir,
                registry,
            )
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

        _validate_artifact_task_chain(name, tasks)

        output_value = spec.get("output", spec.get("output_schema"))
        if output_value is None:
            output_schema: Mapping[str, FrozenJSON] | None = None
        else:
            output_schema = freeze_mapping(
                _mapping(output_value, f"stages.{name}.output")
            )
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
        with_config=freeze_mapping(_mapping(spec.get("with"), f"{label}.with")),
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
    if stage_value is not None and (
        not isinstance(stage_value, str) or not stage_value
    ):
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


def _decode_llm_config(value: object) -> LLMConfig | None:
    if value is None:
        return None
    spec = _mapping(value, "llm")
    _check_keys(spec, _LLM_KEYS, "llm")
    base_url = _required_string(spec, "base_url", "llm")
    try:
        parsed = urlsplit(base_url)
        _ = parsed.port
    except ValueError as error:
        raise DefinitionError("llm.base_url is not a valid URL") from error
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or "?" in base_url
        or "#" in base_url
    ):
        raise DefinitionError(
            "llm.base_url must be an http(s) URL with a host and no "
            "userinfo, query, or fragment"
        )
    api_key = spec.get("api_key")
    if api_key is not None and (
        not isinstance(api_key, str)
        or _SNAPSHOT_ENV_REFERENCE.fullmatch(api_key) is None
    ):
        raise DefinitionError("llm.api_key must be an exact env:NAME reference")
    timeout = spec.get("timeout_seconds", 60)
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise DefinitionError("llm.timeout_seconds must be a finite positive number")
    max_attempts = _integer(spec.get("max_attempts", 3), "llm.max_attempts", minimum=1)
    return LLMConfig(
        base_url=base_url,
        api_key=api_key,
        timeout_seconds=timeout,
        max_attempts=max_attempts,
    )


def _decode_definition(raw: Mapping[str, object], base_dir: Path) -> Definition:
    _check_keys(raw, _TOP_LEVEL_KEYS, "definition")
    version = raw.get("version")
    if type(version) is not int or version != 1:
        raise DefinitionError("version: 1 is required")
    database = raw.get("database", ".sluice/sluice.db")
    if not isinstance(database, str) or not database:
        raise DefinitionError("database must be a non-empty string")
    file_root = raw.get("file_root")
    if file_root is not None and (not isinstance(file_root, str) or not file_root):
        raise DefinitionError("file_root must be a non-empty string")
    registry = decode_web_config(raw.get("web"), base_dir)
    pipelines = _decode_pipelines(_mapping(raw.get("pipelines"), "pipelines"))
    root = resolve_file_root(file_root, base_dir)
    for pipeline in pipelines.values():
        for node in pipeline.nodes.values():
            if node.deliver is not None and node.deliver.use == FILE_SINK:
                resolve_inside(
                    root, node.deliver.with_config.get("directory"), DefinitionError
                )
    return Definition(
        version=1,
        database=database,
        sources=MappingProxyType(
            _decode_sources(_mapping(raw.get("sources"), "sources"), registry)
        ),
        stages=MappingProxyType(
            _decode_stages(_mapping(raw.get("stages"), "stages"), base_dir, registry)
        ),
        pipelines=MappingProxyType(pipelines),
        web=registry,
        llm=_decode_llm_config(raw.get("llm")),
        file_root=file_root,
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
        not isinstance(batch.max_wait, timedelta) or batch.max_wait <= timedelta(0)
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
                raise GraphError(
                    f"delivery node {pipeline.name}.{name} cannot have a stage"
                )
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
            _validate_batch_policy(
                node.batch, stage, node, f"pipelines.{pipeline.name}.{name}"
            )
        _validate_policy(node.policy, f"pipelines.{pipeline.name}.{name}")
        if node.schedule is not None and (
            not isinstance(node.schedule, timedelta) or node.schedule <= timedelta(0)
        ):
            raise DefinitionError(
                f"pipelines.{pipeline.name}.{name}.schedule must be positive"
            )
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
    if (
        not isinstance(definition.sources, Mapping)
        or not isinstance(definition.stages, Mapping)
        or not isinstance(definition.pipelines, Mapping)
    ):
        raise DefinitionError("definition collections must be mappings")
    if definition.web is not None and not isinstance(definition.web, WebRegistryConfig):
        raise DefinitionError("definition.web must be a WebRegistryConfig or null")

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
    input_mode: Literal["each", "batch"],
    tasks: tuple[TaskDef, ...],
    output_schema: Mapping[str, FrozenJSON] | None,
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
        values.append(
            {
                "use": task.use,
                "with": config,
                "web_binding": (
                    None
                    if task.web_binding is None
                    else web_binding_projection(task.web_binding)
                ),
            }
        )
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
    if not isinstance(snapshot, Mapping) or not isinstance(snapshot.get("tasks"), list):
        raise DefinitionError("invalid canonical Stage configuration")
    for task in snapshot["tasks"]:
        if not isinstance(task, Mapping):
            raise DefinitionError("invalid canonical Task configuration")
        config = task.get("with", {})
        if not isinstance(config, Mapping):
            raise DefinitionError("invalid canonical Task configuration")
        for key in _FILE_CONFIG_KEYS:
            value = config.get(key)
            digest_key = f"{key}_digest"
            recorded_digest = config.get(digest_key)
            if value is None and recorded_digest is None:
                continue
            if not isinstance(value, str) or not isinstance(recorded_digest, str):
                raise DefinitionError("invalid retained Stage file metadata")
            file_path = Path(value)
            try:
                current_digest = hashlib.sha256(file_path.read_bytes()).hexdigest()
            except OSError as error:
                raise DefinitionError(
                    f"stage file changed or is missing: {file_path}"
                ) from error
            if current_digest != recorded_digest:
                raise DefinitionError(f"stage file changed or is missing: {file_path}")


def _snapshot_projection_mapping(
    value: object,
    label: str,
    required_keys: set[str] | frozenset[str],
    *,
    optional_keys: set[str] | frozenset[str] = frozenset(),
) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ExecutionError(f"invalid retained {label}")
    keys = set(value)
    allowed = set(required_keys) | set(optional_keys)
    if keys != set(required_keys) and keys != allowed:
        raise ExecutionError(f"invalid retained {label}")
    return value


def _snapshot_mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ExecutionError(f"invalid retained {label}")
    if any(not isinstance(key, str) for key in value):
        raise ExecutionError(f"invalid retained {label}")
    return value


def _snapshot_frozen_mapping(value: object, label: str) -> Mapping[str, FrozenJSON]:
    mapping = _snapshot_mapping(value, label)
    return freeze_mapping(mapping)


def _snapshot_integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ExecutionError(f"invalid retained {label}")
    return value


def _snapshot_boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise ExecutionError(f"invalid retained {label}")
    return value


def _snapshot_optional_integer(value: object, label: str) -> int | None:
    if value is None:
        return None
    return _snapshot_integer(value, label, minimum=1)


def _snapshot_optional_ratio(value: object, label: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ExecutionError(f"invalid retained {label}")
    ratio = float(value)
    if not math.isfinite(ratio) or not 0 <= ratio <= 1:
        raise ExecutionError(f"invalid retained {label}")
    return ratio


def _snapshot_web_output(value: object, label: str) -> WebOutput:
    if value == "artifact":
        return "artifact"
    if value == "markdown":
        return "markdown"
    if value == "text":
        return "text"
    if value == "raw_html":
        return "raw_html"
    if value == "raw_source":
        return "raw_source"
    raise ExecutionError(f"invalid retained {label}")


def _snapshot_optional_string(value: object, label: str) -> str | None:
    if value is not None and not isinstance(value, str):
        raise ExecutionError(f"invalid retained {label}")
    return value


def _snapshot_base_url(value: object, label: str) -> str | None:
    value = _snapshot_optional_string(value, label)
    if value is None:
        return None
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError as error:
        raise ExecutionError(f"invalid retained {label}") from error
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ExecutionError(f"invalid retained {label}")
    return value


def _snapshot_proxy(value: object, label: str) -> str | None:
    value = _snapshot_optional_string(value, label)
    if value is None:
        return None
    if _SNAPSHOT_ENV_REFERENCE.fullmatch(value):
        return value
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError as error:
        raise ExecutionError(f"invalid retained {label}") from error
    if (
        parsed.scheme not in {"http", "https", "socks5", "socks5h"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ExecutionError(f"invalid retained {label}")
    return value


def _snapshot_seconds(
    value: object,
    label: str,
    *,
    allow_zero: bool = False,
    required: bool = False,
) -> int | None:
    if value is None:
        if required:
            raise ExecutionError(f"invalid retained {label}")
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ExecutionError(f"invalid retained {label}")
    try:
        finite = math.isfinite(value)
    except OverflowError:
        finite = False
    if not finite:
        raise ExecutionError(f"invalid retained {label}")
    if isinstance(value, float) and not value.is_integer():
        raise ExecutionError(f"invalid retained {label}")
    seconds = int(value)
    if seconds < 0 or (seconds == 0 and not allow_zero):
        raise ExecutionError(f"invalid retained {label}")
    return seconds


def _snapshot_duration(
    value: object,
    label: str,
    *,
    allow_zero: bool = False,
    required: bool = False,
) -> timedelta | None:
    """Decode a canonical WebBinding duration expressed in whole seconds."""

    seconds = _snapshot_seconds(value, label, allow_zero=allow_zero, required=required)
    if seconds is None:
        return None
    try:
        return timedelta(seconds=seconds)
    except (OverflowError, ValueError) as error:
        raise ExecutionError(f"invalid retained {label}") from error


def _snapshot_sequence(value: object, label: str) -> tuple[object, ...]:
    if not isinstance(value, (list, tuple)):
        raise ExecutionError(f"invalid retained {label}")
    return tuple(value)


def _decode_snapshot_provider(value: object, label: str) -> ProviderBinding:
    value = _snapshot_projection_mapping(value, label, _WEB_PROVIDER_KEYS)
    name = value["name"]
    provider_type = value["type"]
    if (
        not isinstance(name, str)
        or not name
        or not isinstance(provider_type, str)
        or not provider_type
    ):
        raise ExecutionError(f"invalid retained {label}")
    capabilities = _snapshot_sequence(value["capabilities"], f"{label}.capabilities")
    if any(not isinstance(capability, str) for capability in capabilities):
        raise ExecutionError(f"invalid retained {label}.capabilities")
    capabilities = tuple(str(capability) for capability in capabilities)
    actions = _snapshot_sequence(value["actions"], f"{label}.actions")
    timeout = _snapshot_duration(value["timeout"], f"{label}.timeout", required=True)
    connect_timeout = _snapshot_duration(
        value["connect_timeout"], f"{label}.connect_timeout", required=True
    )
    cooldown = _snapshot_duration(
        value["cooldown"], f"{label}.cooldown", allow_zero=True, required=True
    )
    if cooldown is None:
        raise ExecutionError(f"invalid retained {label}.cooldown")
    timeout_seconds = _snapshot_seconds(
        value["timeout_seconds"], f"{label}.timeout_seconds", required=True
    )
    if timeout is None or connect_timeout is None or timeout_seconds is None:
        raise ExecutionError(f"invalid retained {label}.timeout")
    if connect_timeout > timeout:
        raise ExecutionError(f"invalid retained {label}.connect_timeout")
    if timeout_seconds != int(timeout.total_seconds()):
        raise ExecutionError(f"invalid retained {label}.timeout_seconds")
    base_url = _snapshot_base_url(value["base_url"], f"{label}.base_url")
    proxy = _snapshot_proxy(value["proxy"], f"{label}.proxy")
    wait_for_selector = _snapshot_optional_string(
        value["wait_for_selector"], f"{label}.wait_for_selector"
    )
    target_policy = value["target_policy"]
    if target_policy is not None:
        target_policy = _snapshot_mapping(target_policy, f"{label}.target_policy")
    try:
        return ProviderBinding(
            name=name,
            type=provider_type,
            options=_snapshot_frozen_mapping(value["options"], f"{label}.options"),
            base_url=base_url,
            timeout=timeout,
            connect_timeout=connect_timeout,
            transport_retries=_snapshot_integer(
                value["transport_retries"], f"{label}.transport_retries"
            ),
            max_body_bytes=_snapshot_integer(
                value["max_body_bytes"], f"{label}.max_body_bytes", minimum=1
            ),
            concurrency=_snapshot_integer(
                value["concurrency"], f"{label}.concurrency", minimum=1
            ),
            cooldown=cooldown,
            capabilities=frozenset(capabilities),
            service_headers=_snapshot_frozen_mapping(
                value["service_headers"], f"{label}.service_headers"
            ),
            target_headers=_snapshot_frozen_mapping(
                value["target_headers"], f"{label}.target_headers"
            ),
            cookies=_snapshot_frozen_mapping(value["cookies"], f"{label}.cookies"),
            proxy=proxy,
            target_policy=(
                None
                if target_policy is None
                else _snapshot_frozen_mapping(target_policy, f"{label}.target_policy")
            ),
            timeout_seconds=timeout_seconds,
            only_main_content=_snapshot_boolean(
                value["only_main_content"], f"{label}.only_main_content"
            ),
            wait_for_selector=wait_for_selector,
            wait_for=value["wait_for"],
            mobile=_snapshot_boolean(value["mobile"], f"{label}.mobile"),
            actions=actions,
        )
    except (DefinitionError, TypeError, ValueError) as error:
        raise ExecutionError(f"invalid retained {label}") from error


def _decode_snapshot_extractor(value: object, label: str) -> ExtractorBinding:
    value = _snapshot_projection_mapping(value, label, _WEB_EXTRACTOR_KEYS)
    name = value["name"]
    extractor_type = value["type"]
    if (
        not isinstance(name, str)
        or not name
        or not isinstance(extractor_type, str)
        or not extractor_type
    ):
        raise ExecutionError(f"invalid retained {label}")
    try:
        return ExtractorBinding(
            name=name,
            type=extractor_type,
            options=_snapshot_frozen_mapping(value["options"], f"{label}.options"),
        )
    except (DefinitionError, TypeError, ValueError) as error:
        raise ExecutionError(f"invalid retained {label}") from error


def _normalise_snapshot_binding_projection(
    value: Mapping[str, object],
) -> Mapping[str, object]:
    """Normalize whole-number timing floats before semantic comparison."""

    result = dict(value)

    def normalise_provider(provider: object) -> object:
        if not isinstance(provider, Mapping):
            return provider
        normalized = dict(provider)
        for field in ("timeout", "connect_timeout", "cooldown", "timeout_seconds"):
            timing = normalized.get(field)
            if isinstance(timing, float) and timing.is_integer():
                normalized[field] = int(timing)
        return normalized

    direct = result.get("direct_provider")
    if direct is not None:
        result["direct_provider"] = normalise_provider(direct)
    for field in ("fetch_chain",):
        chain = result.get(field)
        if isinstance(chain, Mapping):
            normalized_chain = dict(chain)
            providers = normalized_chain.get("providers")
            if isinstance(providers, (list, tuple)):
                normalized_chain["providers"] = [
                    normalise_provider(provider) for provider in providers
                ]
            result[field] = normalized_chain
    return result


def _decode_snapshot_web_binding(value: object) -> WebBinding:
    """Reconstruct a typed binding from its persisted semantic projection."""

    value = _snapshot_projection_mapping(
        value,
        "Task web binding",
        _WEB_BINDING_KEYS,
        optional_keys={"web_cache_version"},
    )
    direct_value = value["direct_provider"]
    direct_provider = (
        None
        if direct_value is None
        else _decode_snapshot_provider(direct_value, "Task web binding.direct_provider")
    )

    fetch_value = value["fetch_chain"]
    if fetch_value is None:
        fetch_chain = None
        fetch_chain_name = None
    else:
        fetch_value = _snapshot_projection_mapping(
            fetch_value, "Task web binding.fetch_chain", _WEB_FETCH_CHAIN_KEYS
        )
        fetch_chain_name = fetch_value["name"]
        if not isinstance(fetch_chain_name, str) or not fetch_chain_name:
            raise ExecutionError("invalid retained Task web binding.fetch_chain.name")
        fetch_chain = tuple(
            _decode_snapshot_provider(
                provider, f"Task web binding.fetch_chain.providers[{index}]"
            )
            for index, provider in enumerate(
                _snapshot_sequence(
                    fetch_value["providers"],
                    "Task web binding.fetch_chain.providers",
                )
            )
        )
        if len({provider.name for provider in fetch_chain}) != len(fetch_chain):
            raise ExecutionError(
                "invalid retained Task web binding.fetch_chain.providers"
            )

    extract_value = value["extract_chain"]
    if extract_value is None:
        extract_chain = None
        extract_chain_name = None
    else:
        extract_value = _snapshot_projection_mapping(
            extract_value, "Task web binding.extract_chain", _WEB_EXTRACT_CHAIN_KEYS
        )
        extract_chain_name = extract_value["name"]
        if not isinstance(extract_chain_name, str) or not extract_chain_name:
            raise ExecutionError("invalid retained Task web binding.extract_chain.name")
        extract_chain = tuple(
            _decode_snapshot_extractor(
                extractor, f"Task web binding.extract_chain.extractors[{index}]"
            )
            for index, extractor in enumerate(
                _snapshot_sequence(
                    extract_value["extractors"],
                    "Task web binding.extract_chain.extractors",
                )
            )
        )
        if len({extractor.name for extractor in extract_chain}) != len(extract_chain):
            raise ExecutionError(
                "invalid retained Task web binding.extract_chain.extractors"
            )

    quality_value = _snapshot_projection_mapping(
        value["quality"], "Task web binding.quality", _WEB_QUALITY_KEYS
    )
    if quality_value["reject_challenges"] is not True:
        raise ExecutionError(
            "invalid retained Task web binding.quality.reject_challenges"
        )
    cache_version = value.get("web_cache_version")
    if cache_version is not None and (
        isinstance(cache_version, bool)
        or not isinstance(cache_version, int)
        or cache_version <= 0
    ):
        raise ExecutionError("invalid retained Task web binding.web_cache_version")
    try:
        quality = QualityPolicy(
            min_chars=_snapshot_integer(
                quality_value["min_chars"], "Task web binding.quality.min_chars"
            ),
            min_words=_snapshot_optional_integer(
                quality_value["min_words"], "Task web binding.quality.min_words"
            ),
            max_link_ratio=_snapshot_optional_ratio(
                quality_value["max_link_ratio"],
                "Task web binding.quality.max_link_ratio",
            ),
            reject_challenges=_snapshot_boolean(
                quality_value["reject_challenges"],
                "Task web binding.quality.reject_challenges",
            ),
            max_body_bytes=_snapshot_integer(
                quality_value["max_body_bytes"],
                "Task web binding.quality.max_body_bytes",
                minimum=1,
            ),
        )
        binding = WebBinding(
            version=_snapshot_integer(
                value["version"], "Task web binding.version", minimum=1
            ),
            direct_provider=direct_provider,
            fetch_chain=fetch_chain,
            extract_chain=extract_chain,
            quality=quality,
            output=_snapshot_web_output(value["output"], "Task web binding.output"),
            fetch_chain_name=fetch_chain_name,
            extract_chain_name=extract_chain_name,
            web_cache_version=cache_version,
        )
        normalized_value = _normalise_snapshot_binding_projection(value)
        if canonical_json(binding.semantic_projection()) != canonical_json(
            normalized_value
        ):
            raise ExecutionError("invalid retained Task web binding")
        return binding
    except (TypeError, ValueError) as error:
        raise ExecutionError("invalid retained Task web binding") from error


def _validate_snapshot_task_binding(use: str, binding: WebBinding | None) -> None:
    if binding is None:
        return
    if use not in {
        "sluice.tasks.fetch_webpage",
        "sluice.tasks.extract_content",
        "sluice.tasks.to_markdown",
        "sluice.tasks.to_text",
    }:
        raise ExecutionError("retained web binding is not valid for this Task")
    if binding.web_cache_version is not None:
        raise ExecutionError("retained Stage Task cannot carry web_cache_version")
    if binding.direct_provider is not None:
        raise ExecutionError("retained Stage Task cannot carry a direct provider")
    if use == "sluice.tasks.fetch_webpage":
        if (
            binding.output != "artifact"
            or binding.fetch_chain is None
            or not binding.fetch_chain
            or binding.extract_chain is None
            or not binding.extract_chain
        ):
            raise ExecutionError("invalid retained fetch_webpage binding")
    elif use == "sluice.tasks.extract_content":
        if (
            binding.output != "artifact"
            or binding.fetch_chain is not None
            or binding.extract_chain is None
            or not binding.extract_chain
        ):
            raise ExecutionError("invalid retained extract_content binding")
    else:
        expected_output = "markdown" if use == "sluice.tasks.to_markdown" else "text"
        if (
            binding.output != expected_output
            or binding.fetch_chain is not None
            or binding.extract_chain is not None
        ):
            raise ExecutionError("invalid retained projector binding")


def decode_stage_snapshot(value: Mapping[str, JSONValue]) -> StageDef:
    """Decode and verify one immutable Stage configuration retained in SQLite.

    Execution snapshots carry file digests alongside the user-facing Task
    configuration.  The digest fields are retained in the canonical snapshot
    for verification but are stripped from the config passed to a Task.
    """

    if not isinstance(value, Mapping):
        raise ExecutionError("invalid retained Stage configuration")
    required = {"name", "input", "cache_version", "tasks", "output_schema"}
    if set(value) != required or value.get("input") not in {"each", "batch"}:
        raise ExecutionError("invalid retained Stage configuration")
    name = value.get("name")
    cache_version = value.get("cache_version")
    encoded_tasks = value.get("tasks")
    if not isinstance(name, str) or not name:
        raise ExecutionError("invalid retained Stage configuration")
    if (
        isinstance(cache_version, bool)
        or not isinstance(cache_version, int)
        or cache_version < 1
    ):
        raise ExecutionError("invalid retained Stage cache version")
    if not isinstance(encoded_tasks, list) or not encoded_tasks:
        raise ExecutionError("invalid retained Task configuration")

    try:
        canonical = canonical_json(value)
    except (TypeError, ValueError) as error:
        raise ExecutionError("invalid retained Stage configuration") from error

    tasks: list[TaskDef] = []
    for encoded in encoded_tasks:
        if not isinstance(encoded, Mapping) or set(encoded) not in (
            {"use", "with"},
            {"use", "with", "web_binding"},
        ):
            raise ExecutionError("invalid retained Task configuration")
        use = encoded.get("use")
        config = encoded.get("with")
        if not isinstance(use, str) or not use or not isinstance(config, Mapping):
            raise ExecutionError("invalid retained Task configuration")
        runtime_config = dict(config)
        for file_key in _FILE_CONFIG_KEYS:
            runtime_config.pop(f"{file_key}_digest", None)
        try:
            frozen_config = freeze_json(runtime_config)
        except DefinitionError as error:
            raise ExecutionError("invalid retained Task configuration") from error
        if "web_binding" not in encoded:
            web_binding = None
        elif encoded.get("web_binding") is None:
            if use in {
                "sluice.tasks.fetch_webpage",
                "sluice.tasks.extract_content",
                "sluice.tasks.to_markdown",
                "sluice.tasks.to_text",
            }:
                raise ExecutionError(
                    "retained web Task must include a non-null web_binding"
                )
            web_binding = None
        else:
            web_binding = _decode_snapshot_web_binding(encoded.get("web_binding"))
        _validate_snapshot_task_binding(use, web_binding)
        tasks.append(
            TaskDef(use=use, with_config=frozen_config, web_binding=web_binding)
        )

    output = value.get("output_schema")
    if output is None:
        output_schema = None
    else:
        try:
            frozen_output = freeze_json(output)
        except DefinitionError as error:
            raise ExecutionError("invalid retained Stage output schema") from error
        if not isinstance(frozen_output, Mapping):
            raise ExecutionError("invalid retained Stage output schema")
        output_schema = frozen_output
    stage = StageDef(
        name=name,
        input_mode=value["input"],  # type: ignore[arg-type]
        tasks=tuple(tasks),
        output_schema=output_schema,
        cache_version=cache_version,
        canonical_config_json=canonical,
    )
    try:
        verify_stage_files(stage)
    except (DefinitionError, KeyError, TypeError, ValueError) as error:
        raise ExecutionError("retained Stage file metadata is invalid") from error
    return stage
