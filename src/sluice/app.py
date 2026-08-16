"""Process-start application assembly for a validated definition."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Callable

from sluice.config import load_definition
from sluice.contracts import (
    Definition,
    JSONValue,
    SinkContract,
    SourceContract,
    TaskContract,
)
from sluice.db import init_schema, open_database
from sluice.errors import DefinitionError
from sluice.resolution import (
    BoundaryAdapters,
    freeze_adapters,
    resolve_sink,
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
        try:
            init_schema(conn)
        except BaseException:
            conn.close()
            raise
    except BaseException:
        # A connection is only created after all resolution above.  Keep this
        # guard for future resolver/database changes without leaking a lock.
        raise

    return App(
        definition=definition,
        conn=conn,
        sources=MappingProxyType(resolved_sources),
        tasks=MappingProxyType(task_cache),
        sinks=MappingProxyType(sink_cache),
    )
