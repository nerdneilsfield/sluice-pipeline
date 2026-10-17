"""The sole dotted-path and built-in binding boundary.

Resolution happens during process startup.  Runtime code receives small frozen
adapters and never imports a configured path on demand.
"""

from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from sluice.contracts import (
    HTTPClient,
    JSONValue,
    LLMClient,
    SinkContract,
    SourceContract,
    SourceResult,
    StageDef,
    TaskContract,
    TaskDef,
)
from sluice.errors import DefinitionError
from sluice.web.contracts import PrivateWebInvocation


@dataclass(frozen=True, slots=True)
class BoundaryAdapters:
    http_client: HTTPClient | None
    llm_client: LLMClient | None
    sink_clients: Mapping[
        str, Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]
    ]
    web_content_client: object | None = None


# Built-ins are an explicit, fixed map.  Runtime resolution never discovers
# modules speculatively or imports configured paths after startup.
BUILTIN_SOURCE_BINDERS: dict[str, str] = {
    "sluice.sources.rss": "sluice.sources.rss.bind",
    "sluice.sources.webpage": "sluice.sources.webpage.bind",
}
BUILTIN_TASK_BINDERS: dict[str, str] = {
    "sluice.tasks.dedup": "sluice.tasks.collection.dedup",
    "sluice.tasks.filter": "sluice.tasks.collection.filter",
    "sluice.tasks.sort": "sluice.tasks.collection.sort",
    "sluice.tasks.limit": "sluice.tasks.collection.limit",
    "sluice.tasks.fetch_webpage": "sluice.tasks.content.fetch_webpage",
    "sluice.tasks.extract_content": "sluice.tasks.content.extract_content",
    "sluice.tasks.to_markdown": "sluice.tasks.content.to_markdown",
    "sluice.tasks.to_text": "sluice.tasks.content.to_text",
    "sluice.tasks.render_email": "sluice.tasks.content.render_email",
    "sluice.tasks.render_telegram": "sluice.tasks.content.render_telegram",
    "sluice.tasks.llm_extract": "sluice.tasks.llm.bind_extract",
    "sluice.tasks.llm_summary": "sluice.tasks.llm.bind_summary",
}
BUILTIN_WEB_TASKS = frozenset(
    {
        "sluice.tasks.fetch_webpage",
        "sluice.tasks.extract_content",
        "sluice.tasks.to_markdown",
        "sluice.tasks.to_text",
    }
)
BUILTIN_LLM_TASKS = frozenset({"sluice.tasks.llm_extract", "sluice.tasks.llm_summary"})
BUILTIN_SINK_BINDERS: dict[str, str] = {
    "sluice.sinks.email": "sluice.sinks.email.bind",
    "sluice.sinks.telegram": "sluice.sinks.telegram.bind",
    "sluice.sinks.feishu": "sluice.sinks.feishu.bind",
}


def _fresh_json(value: object) -> object:
    """Thaw an immutable JSON value before invoking user code."""

    if isinstance(value, Mapping):
        return {key: _fresh_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_fresh_json(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class FunctionSource:
    function: Callable[[Mapping[str, JSONValue], JSONValue], SourceResult]

    def collect(
        self, config: Mapping[str, JSONValue], checkpoint: JSONValue
    ) -> SourceResult:
        copied = _fresh_json(config)
        return self.function(copied, checkpoint)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class FunctionTask:
    function: Callable[[Mapping[str, JSONValue], object], object]
    input_mode: Literal["each", "batch"]

    def run(self, config: Mapping[str, JSONValue], input: object) -> object:
        copied = _fresh_json(config)
        return self.function(copied, input)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class BuiltinWebSource:
    """Private invocation wrapper for a resolved built-in web Source."""

    source: object

    def collect(
        self, config: Mapping[str, JSONValue], checkpoint: JSONValue
    ) -> SourceResult:
        collector = getattr(self.source, "collect", None)
        if not callable(collector):
            raise DefinitionError("built-in web Source has no collect operation")
        return collector(config, checkpoint)

    def collect_web(
        self, invocation: PrivateWebInvocation, checkpoint: JSONValue
    ) -> SourceResult:
        collector = getattr(self.source, "collect_web", None)
        if not callable(collector):
            raise DefinitionError(
                "built-in web Source has no private collect operation"
            )
        return collector(invocation, checkpoint)


@dataclass(frozen=True, slots=True)
class BuiltinWebTask:
    """Private invocation wrapper for a resolved built-in web Task."""

    task: object
    input_mode: Literal["each", "batch"]

    def run(self, config: Mapping[str, JSONValue], input: object) -> object:
        runner = getattr(self.task, "run", None)
        if not callable(runner):
            raise DefinitionError("built-in web Task has no run operation")
        return runner(config, input)

    def run_web(self, invocation: PrivateWebInvocation, input: object) -> object:
        runner = getattr(self.task, "run_web", None)
        if not callable(runner):
            raise DefinitionError("built-in web Task has no private run operation")
        return runner(invocation, input)


@dataclass(frozen=True, slots=True)
class FunctionSink:
    function: Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]

    def send(
        self,
        config: Mapping[str, JSONValue],
        rendered_payload: JSONValue,
        delivery_key: str,
    ) -> str | None:
        copied = _fresh_json(config)
        return self.function(copied, rendered_payload, delivery_key)  # type: ignore[arg-type]


def private_web_invocation(task: TaskDef) -> PrivateWebInvocation | None:
    """Build the private typed payload reserved for built-in web Tasks.

    Custom callables keep the public ``Mapping`` contract, even if a caller
    constructs a custom ``TaskDef`` with a binding by hand.  Legacy retained
    web Tasks have no binding and therefore cannot produce an invocation.
    """

    if not isinstance(task, TaskDef):
        raise DefinitionError("private web invocation requires a TaskDef")
    if task.use not in BUILTIN_WEB_TASKS or task.web_binding is None:
        return None
    return PrivateWebInvocation(
        with_config=task.with_config,
        web_binding=task.web_binding,
    )


def import_value(path: str) -> object:
    """Import one explicitly configured dotted path."""

    if not isinstance(path, str) or not path or "." not in path:
        raise DefinitionError(f"cannot resolve {path}")
    module_name, attribute = path.rsplit(".", 1)
    if not module_name or not attribute:
        raise DefinitionError(f"cannot resolve {path}")
    try:
        value = getattr(importlib.import_module(module_name), attribute)
    except (ImportError, AttributeError, ValueError) as error:
        raise DefinitionError(f"cannot resolve {path}") from error
    return value


def _bound_builtin(binding: str, path: str) -> object:
    try:
        binder = import_value(binding)
    except DefinitionError as error:
        raise DefinitionError(f"cannot bind built-in {path}") from error
    if not callable(binder) and not callable(getattr(binder, "run", None)):
        raise DefinitionError(f"cannot bind built-in {path}")
    return binder


def bind_builtin_source(
    path: str, adapters: BoundaryAdapters | HTTPClient | None
) -> SourceContract:
    """Bind one built-in Source against the immutable startup boundary.

    The v4 Source modules still expose ``bind(http_client)`` for compatibility;
    accepting the full adapter bundle here lets newer wrappers consume the
    shared WebContentClient without changing public custom Source contracts.
    """
    boundary = (
        adapters
        if isinstance(adapters, BoundaryAdapters)
        else freeze_adapters(http_client=adapters)
    )
    try:
        binder = _bound_builtin(BUILTIN_SOURCE_BINDERS[path], path)
        try:
            signature = inspect.signature(binder)
            parameter = next(iter(signature.parameters.values()), None)
        except (TypeError, ValueError):
            parameter = None
        if path in BUILTIN_SOURCE_BINDERS:
            # Built-in Sources need the complete immutable startup boundary so
            # their web variants can retain the shared private client.  The
            # source adapters accept a plain HTTPClient as a compatibility
            # form for direct callers.
            value = binder(boundary)
        elif parameter is not None and parameter.name in {"adapters", "boundary"}:
            value = binder(boundary)
        else:
            value = binder(boundary.http_client)
    except (KeyError, TypeError, AttributeError) as error:
        raise DefinitionError(f"cannot bind built-in Source {path}") from error
    return value  # normalized by resolve_source below


def bind_builtin_task(path: str, adapters: BoundaryAdapters) -> TaskContract:
    try:
        binder = _bound_builtin(BUILTIN_TASK_BINDERS[path], path)
        # Collection/content built-ins are immutable Task objects.  The HTTP
        # fetch Task additionally exposes ``bind`` so the startup adapter is
        # captured once; ordinary local callable binders retain the historical
        # binder(adapters) shape.
        if callable(getattr(binder, "run", None)) and getattr(
            binder, "input_mode", None
        ) in {"each", "batch"}:
            bind = getattr(binder, "bind", None)
            value = bind(adapters) if callable(bind) else binder
        else:
            value = binder(adapters)
    except (KeyError, TypeError, AttributeError) as error:
        raise DefinitionError(f"cannot bind built-in Task {path}") from error
    return value  # normalized by resolve_task below


def bind_builtin_sink(
    path: str,
    sink_clients: Mapping[
        str, Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]
    ],
) -> SinkContract:
    try:
        binder = _bound_builtin(BUILTIN_SINK_BINDERS[path], path)
        client = sink_clients.get(path)
        if client is not None and not callable(client):
            raise DefinitionError(f"startup Sink client for {path} is not callable")
        value = binder(client)
    except (KeyError, TypeError, AttributeError) as error:
        raise DefinitionError(f"cannot bind built-in Sink {path}") from error
    return value  # normalized by resolve_sink below


def require_call_shape(
    path: str, function: Callable[..., object], *sample_args: object
) -> None:
    if not callable(function):
        raise DefinitionError(f"{path} is not callable")
    try:
        inspect.signature(function).bind(*sample_args)
    except (TypeError, ValueError) as error:
        raise DefinitionError(f"{path} has the wrong callable signature") from error


def resolve_source(path: str, adapters: BoundaryAdapters) -> SourceContract:
    value = (
        bind_builtin_source(path, adapters)
        if path in BUILTIN_SOURCE_BINDERS
        else import_value(path)
    )
    method = getattr(value, "collect", None)
    if callable(method):
        require_call_shape(path, method, {}, None)
        if path in BUILTIN_SOURCE_BINDERS:
            return BuiltinWebSource(value)  # type: ignore[return-value]
        return FunctionSource(method)
    if callable(value):
        require_call_shape(path, value, {}, None)
        return FunctionSource(value)
    raise DefinitionError(f"{path} is not a Source callable")


def resolve_task(
    path: str,
    expected_mode: Literal["each", "batch"],
    adapters: BoundaryAdapters,
) -> TaskContract:
    value = (
        bind_builtin_task(path, adapters)
        if path in BUILTIN_TASK_BINDERS
        else import_value(path)
    )
    declared_mode = getattr(value, "input_mode", None)
    if declared_mode not in {"each", "batch"}:
        raise DefinitionError(f"{path} must declare input_mode")
    method = getattr(value, "run", None)
    if callable(method):
        require_call_shape(path, method, {}, object())
        task: TaskContract = FunctionTask(method, declared_mode)
    elif callable(value):
        require_call_shape(path, value, {}, object())
        task = FunctionTask(value, declared_mode)
    else:
        raise DefinitionError(f"{path} is not a Task callable with input_mode")
    if task.input_mode != expected_mode:
        raise DefinitionError(
            f"{path} declares {task.input_mode}, expected {expected_mode}"
        )
    if path in BUILTIN_WEB_TASKS:
        return BuiltinWebTask(value, task.input_mode)
    return task


def resolve_retained_tasks(
    stage: StageDef,
    adapters: BoundaryAdapters,
    cache: dict[str, TaskContract] | None = None,
) -> dict[str, TaskContract]:
    """Resolve every Task path in one retained Stage snapshot.

    This shares the same startup adapter boundary and cache as current YAML
    stages.  It is called before an ``App`` is returned so execution never
    imports a retained callable in the middle of a run.
    """

    if not isinstance(stage, StageDef):
        raise DefinitionError("retained Stage must be a StageDef")
    resolved = {} if cache is None else cache
    for task in stage.tasks:
        existing = resolved.get(task.use)
        if existing is None:
            resolved[task.use] = resolve_task(task.use, stage.input_mode, adapters)
        elif existing.input_mode != stage.input_mode:
            raise DefinitionError(
                f"{task.use} declares {existing.input_mode}, expected {stage.input_mode}"
            )
    return resolved


def resolve_sink(path: str, adapters: BoundaryAdapters) -> SinkContract:
    value = (
        bind_builtin_sink(path, adapters.sink_clients)
        if path in BUILTIN_SINK_BINDERS
        else import_value(path)
    )
    method = getattr(value, "send", None)
    if callable(method):
        require_call_shape(path, method, {}, None, "key")
        return FunctionSink(method)
    if callable(value):
        require_call_shape(path, value, {}, None, "key")
        return FunctionSink(value)
    raise DefinitionError(f"{path} is not a Sink callable")


def freeze_adapters(
    *,
    http_client: HTTPClient | None = None,
    llm_client: LLMClient | None = None,
    sink_clients: Mapping[
        str, Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]
    ]
    | None = None,
    web_content_client: object | None = None,
) -> BoundaryAdapters:
    """Copy the startup boundary bundle into immutable mapping storage."""

    clients = {} if sink_clients is None else dict(sink_clients)
    return BoundaryAdapters(
        http_client=http_client,
        llm_client=llm_client,
        sink_clients=MappingProxyType(clients),
        web_content_client=web_content_client,
    )


def run_web(
    invocation: PrivateWebInvocation,
    input: object,
    web_content_client: object | None = None,
) -> object:
    """Invoke a private web wrapper without widening public Mapping contracts."""

    if not isinstance(invocation, PrivateWebInvocation):
        raise DefinitionError("web invocation must be a PrivateWebInvocation")
    client = web_content_client
    if client is None:
        raise DefinitionError("web content client is not configured")
    runner = getattr(client, "run_web", None)
    if callable(runner):
        return runner(invocation, input)
    runner = getattr(client, "run", None)
    if callable(runner):
        return runner(invocation, input)
    if callable(client):
        return client(invocation, input)
    raise DefinitionError("web content client has no run operation")


def collect_web(
    invocation: PrivateWebInvocation,
    checkpoint: JSONValue,
    web_content_client: object | None = None,
) -> object:
    """Invoke a private built-in web Source wrapper."""

    if not isinstance(invocation, PrivateWebInvocation):
        raise DefinitionError("web invocation must be a PrivateWebInvocation")
    if web_content_client is None:
        raise DefinitionError("web content client is not configured")
    runner = getattr(web_content_client, "collect_web", None)
    if callable(runner):
        return runner(invocation, checkpoint)
    runner = getattr(web_content_client, "collect", None)
    if callable(runner):
        return runner(invocation, checkpoint)
    raise DefinitionError("web content client has no collect operation")
