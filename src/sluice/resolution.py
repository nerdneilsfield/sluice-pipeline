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
    TaskContract,
)
from sluice.errors import DefinitionError


@dataclass(frozen=True, slots=True)
class BoundaryAdapters:
    http_client: HTTPClient | None
    llm_client: LLMClient | None
    sink_clients: Mapping[str, Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]]


# Built-ins are an explicit, fixed map.  Runtime resolution never discovers
# modules speculatively or imports configured paths after startup.
BUILTIN_SOURCE_BINDERS: dict[str, str] = {
    "sluice.sources.rss": "sluice.sources.rss.bind",
    "sluice.sources.webpage": "sluice.sources.webpage.bind",
}
BUILTIN_TASK_BINDERS: dict[str, str] = {}
BUILTIN_SINK_BINDERS: dict[str, str] = {}


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

    def collect(self, config: Mapping[str, JSONValue], checkpoint: JSONValue) -> SourceResult:
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
    if not callable(binder):
        raise DefinitionError(f"cannot bind built-in {path}")
    return binder


def bind_builtin_source(path: str, http_client: HTTPClient | None) -> SourceContract:
    try:
        binder = _bound_builtin(BUILTIN_SOURCE_BINDERS[path], path)
        value = binder(http_client)
    except (KeyError, TypeError, AttributeError) as error:
        raise DefinitionError(f"cannot bind built-in Source {path}") from error
    return value  # normalized by resolve_source below


def bind_builtin_task(path: str, adapters: BoundaryAdapters) -> TaskContract:
    try:
        binder = _bound_builtin(BUILTIN_TASK_BINDERS[path], path)
        value = binder(adapters)
    except (KeyError, TypeError, AttributeError) as error:
        raise DefinitionError(f"cannot bind built-in Task {path}") from error
    return value  # normalized by resolve_task below


def bind_builtin_sink(
    path: str,
    sink_clients: Mapping[str, Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]],
) -> SinkContract:
    try:
        binder = _bound_builtin(BUILTIN_SINK_BINDERS[path], path)
        client = sink_clients[path]
        value = binder(client)
    except (KeyError, TypeError, AttributeError) as error:
        raise DefinitionError(f"cannot bind built-in Sink {path}") from error
    return value  # normalized by resolve_sink below


def require_call_shape(path: str, function: Callable[..., object], *sample_args: object) -> None:
    if not callable(function):
        raise DefinitionError(f"{path} is not callable")
    try:
        inspect.signature(function).bind(*sample_args)
    except (TypeError, ValueError) as error:
        raise DefinitionError(f"{path} has the wrong callable signature") from error


def resolve_source(path: str, adapters: BoundaryAdapters) -> SourceContract:
    value = (
        bind_builtin_source(path, adapters.http_client)
        if path in BUILTIN_SOURCE_BINDERS
        else import_value(path)
    )
    method = getattr(value, "collect", None)
    if callable(method):
        require_call_shape(path, method, {}, None)
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
        raise DefinitionError(f"{path} declares {task.input_mode}, expected {expected_mode}")
    return task


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
    sink_clients: Mapping[str, Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]] | None = None,
) -> BoundaryAdapters:
    """Copy the startup boundary bundle into immutable mapping storage."""

    clients = {} if sink_clients is None else dict(sink_clients)
    return BoundaryAdapters(
        http_client=http_client,
        llm_client=llm_client,
        sink_clients=MappingProxyType(clients),
    )
