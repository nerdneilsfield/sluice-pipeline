"""Process-start application assembly for a validated definition."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Callable

from sluice.config import decode_stage_snapshot, load_definition
from sluice.contracts import (
    Definition,
    JSONValue,
    SinkContract,
    SourceContract,
    TaskContract,
)
from sluice.db import init_schema, open_database
from sluice.errors import DefinitionError, ExecutionError
from sluice.resolution import (
    BoundaryAdapters,
    freeze_adapters,
    resolve_sink,
    resolve_retained_tasks,
    resolve_source,
    resolve_task,
)


@dataclass(frozen=True, slots=True)
class App:
    definition: Definition
    conn: sqlite3.Connection
    sources: MappingProxyType
    tasks: MappingProxyType
    sinks: MappingProxyType


def _definition_and_base(
    definition_path: str | Path,
) -> tuple[Definition, Path]:
    if isinstance(definition_path, Definition):
        raise DefinitionError("open_app requires a definition path")
    try:
        path = Path(definition_path)
    except TypeError as error:
        raise DefinitionError("open_app requires a definition path") from error
    return load_definition(path), path.parent.resolve()


def open_app(
    definition_path: str | Path,
    db_path: str | Path | None = None,
    *,
    llm_client=None,
    http_client=None,
    sink_clients: Mapping[str, Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]] | None = None,
) -> App:
    """Load and resolve a definition, then return an immutable startup App.

    Every configured dotted path is resolved before this function returns.  A
    failed resolution closes the SQLite connection so no half-started App
    retains its writer lock.
    """

    definition, base_dir = _definition_and_base(definition_path)
    adapters: BoundaryAdapters = freeze_adapters(
        http_client=http_client, llm_client=llm_client, sink_clients=sink_clients
    )

    source_cache: dict[str, SourceContract] = {}
    task_cache: dict[str, TaskContract] = {}
    sink_cache: dict[str, SinkContract] = {}
    resolved_sources: dict[str, SourceContract] = {}

    conn: sqlite3.Connection | None = None
    try:
        for name, source in definition.sources.items():
            if source.use not in source_cache:
                source_cache[source.use] = resolve_source(source.use, adapters)
            resolved_sources[name] = source_cache[source.use]

        for stage in definition.stages.values():
            for task in stage.tasks:
                if task.use not in task_cache:
                    task_cache[task.use] = resolve_task(
                        task.use, stage.input_mode, adapters
                    )
                elif task_cache[task.use].input_mode != stage.input_mode:
                    raise DefinitionError(
                        f"{task.use} declares {task_cache[task.use].input_mode}, "
                        f"expected {stage.input_mode}"
                    )

        for pipeline in definition.pipelines.values():
            for node in pipeline.nodes.values():
                if node.deliver is None:
                    continue
                use = node.deliver.use
                if use not in sink_cache:
                    sink_cache[use] = resolve_sink(use, adapters)

        database = Path(db_path) if db_path is not None else Path(definition.database)
        if not database.is_absolute():
            database = base_dir / database
        conn = open_database(database)
        init_schema(conn)

        # Retained Stage executions outlive the currently authored YAML.  Load
        # and verify every distinct snapshot before returning the immutable App
        # so no callable is imported during a later rebuild.
        retained_configs: set[str] = set()
        rows = conn.execute(
            "SELECT DISTINCT config_json FROM executions WHERE kind='stage'"
        ).fetchall()
        retained_configs.update(str(row[0]) for row in rows if row[0] is not None)
        rows = conn.execute(
            "SELECT DISTINCT config_json FROM materialization_waiters"
        ).fetchall()
        retained_configs.update(str(row[0]) for row in rows if row[0] is not None)
        for config_json in sorted(retained_configs):
            try:
                snapshot = json.loads(config_json)
                stage = decode_stage_snapshot(snapshot)
            except (ExecutionError, TypeError, ValueError, json.JSONDecodeError) as error:
                raise DefinitionError("invalid retained Stage snapshot") from error
            resolve_retained_tasks(stage, adapters, task_cache)
    except BaseException:
        if conn is not None:
            conn.close()
        raise

    assert conn is not None

    app = App(
        definition=definition,
        conn=conn,
        sources=MappingProxyType(resolved_sources),
        tasks=MappingProxyType(task_cache),
        sinks=MappingProxyType(sink_cache),
    )
    # Recovery is a process-start transition, not a periodic tick callback.
    # Import lazily to avoid the app/reconcile module cycle during assembly.
    from sluice.reconcile import recover_startup

    recover_startup(app, datetime.now(timezone.utc))
    return app
