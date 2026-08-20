"""Process-start application assembly for a validated definition."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Callable
import threading

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
from sluice.web.registry import WebRuntimeCatalog


class _AdmissionState:
    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.active = 0
        self.closing = False
        self.closed = False


@dataclass(frozen=True, slots=True)
class App:
    definition: Definition
    conn: sqlite3.Connection
    sources: MappingProxyType
    tasks: MappingProxyType
    sinks: MappingProxyType
    web_runtime_catalog: WebRuntimeCatalog | None = None
    web_content_client: object | None = None
    _admission: _AdmissionState = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_admission", _AdmissionState())

    @contextmanager
    def admit(self):
        """Admit one local drain until completion, or reject during close."""

        state = self._admission
        with state.condition:
            if state.closing or state.closed:
                raise ExecutionError("App is closed")
            state.active += 1
        try:
            yield self
        finally:
            with state.condition:
                state.active -= 1
                if state.active <= 0:
                    state.active = 0
                    state.condition.notify_all()

    def close(self) -> None:
        """Synchronously stop admission, drain workers, and close resources."""

        state = self._admission
        with state.condition:
            if state.closed:
                return
            if state.closing:
                while not state.closed:
                    state.condition.wait()
                return
            state.closing = True
            while state.active:
                state.condition.wait()
        try:
            catalog = self.web_runtime_catalog
            if catalog is not None:
                catalog.close()
        finally:
            try:
                self.conn.close()
            finally:
                with state.condition:
                    state.closed = True
                    state.condition.notify_all()


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
    web_content_client: object | None = None,
) -> App:
    """Load and resolve a definition, then return an immutable startup App.

    Every configured dotted path is resolved before this function returns.  A
    failed resolution closes the SQLite connection so no half-started App
    retains its writer lock.
    """

    definition, base_dir = _definition_and_base(definition_path)
    catalog: WebRuntimeCatalog | None = None
    try:
        catalog = WebRuntimeCatalog.from_definition(
            definition,
            http_client=http_client,
            web_content_client=web_content_client,
        )
        selected_web_client = (
            web_content_client
            if web_content_client is not None
            else catalog.web_content_client
        )
        adapters: BoundaryAdapters = freeze_adapters(
            http_client=http_client,
            llm_client=llm_client,
            sink_clients=sink_clients,
            web_content_client=selected_web_client,
        )
    except BaseException:
        if catalog is not None:
            catalog.close()
        raise

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
        # Keep the lifecycle state alongside each retained snapshot.  Queued,
        # running, and waiter snapshots are immediately executable and must
        # resolve their current secret references at startup.  Only terminal
        # history may defer a missing secret until an explicit rebuild.  If a
        # snapshot is present in both sets, the executable occurrence wins.
        retained_configs: dict[str, bool] = {}
        rows = conn.execute(
            "SELECT config_json,state FROM executions WHERE kind='stage'"
        ).fetchall()
        terminal_states = {"succeeded", "failed", "dropped", "cached"}
        for row in rows:
            config_json = row[0]
            if config_json is None:
                continue
            state = str(row[1])
            allow_missing = state in terminal_states
            previous = retained_configs.get(str(config_json))
            retained_configs[str(config_json)] = (
                allow_missing if previous is None else previous and allow_missing
            )
        rows = conn.execute(
            "SELECT config_json FROM materialization_waiters"
        ).fetchall()
        for row in rows:
            config_json = row[0]
            if config_json is not None:
                retained_configs[str(config_json)] = False
        for config_json, allow_missing in sorted(retained_configs.items()):
            try:
                snapshot = json.loads(config_json)
                stage = decode_stage_snapshot(snapshot)
            except (ExecutionError, TypeError, ValueError, json.JSONDecodeError) as error:
                raise DefinitionError("invalid retained Stage snapshot") from error
            # Validate every retained binding against the same fixed maps.  A
            # terminal snapshot may refer to a retired env secret, so defer
            # only that lookup until an explicit rebuild.
            for task in stage.tasks:
                web_binding = task.web_binding
                if web_binding is None:
                    continue
                for provider in web_binding.providers:
                    assert catalog is not None
                    if allow_missing:
                        catalog.validate_provider_binding(provider)
                    else:
                        catalog.register_provider(provider, allow_missing_secrets=False)
                for extractor in web_binding.extractors:
                    assert catalog is not None
                    if allow_missing:
                        catalog.validate_extractor_binding(extractor)
                    else:
                        catalog.extractor(extractor)
            resolve_retained_tasks(stage, adapters, task_cache)
    except BaseException:
        if conn is not None:
            conn.close()
        if catalog is not None:
            catalog.close()
        raise

    assert conn is not None

    app = App(
        definition=definition,
        conn=conn,
        sources=MappingProxyType(resolved_sources),
        tasks=MappingProxyType(task_cache),
        sinks=MappingProxyType(sink_cache),
        web_runtime_catalog=catalog,
        web_content_client=adapters.web_content_client,
    )
    # Recovery is a process-start transition, not a periodic tick callback.
    # Import lazily to avoid the app/reconcile module cycle during assembly.
    from sluice.reconcile import recover_startup

    try:
        recover_startup(app, datetime.now(timezone.utc))
    except BaseException:
        # Startup recovery is part of opening the process boundary.  A
        # failed recovery must not leak the writer connection or provider
        # transports that were already assembled above.
        app.close()
        raise
    return app
