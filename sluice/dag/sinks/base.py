from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from importlib import import_module
from typing import Any


@dataclass(frozen=True)
class SinkChunk:
    ordinal: int
    payload: Any
    message_kind: str | None = None
    items_in_chunk: tuple[str, ...] = ()


@dataclass(frozen=True)
class EmissionResult:
    sink_id: str
    ordinal: int
    success: bool
    error_class: str | None = None
    error_msg: str | None = None
    external_id: str | None = None
    recipient: str | None = None


class Sink(ABC):
    type_name: str = ""

    @abstractmethod
    async def chunks(self, rendered: Any, params: dict) -> list[SinkChunk]: ...

    @abstractmethod
    async def emit(self, chunk: SinkChunk, params: dict) -> EmissionResult: ...


_REGISTRY: dict[str, type[Sink]] = {}
_BUILTIN_MODULES = {
    "file_md": "sluice.dag.sinks.file_md",
    "email": "sluice.dag.sinks.email_sink",
    "feishu": "sluice.dag.sinks.feishu_sink",
    "notion": "sluice.dag.sinks.notion_sink",
    "telegram": "sluice.dag.sinks.telegram_sink",
}


def bind_sink(type_name: str, cls: type[Sink]) -> None:
    if type_name in _REGISTRY:
        raise ValueError(f"sink already bound for type {type_name!r}")
    _REGISTRY[type_name] = cls


def get_sink_class(type_name: str) -> type[Sink]:
    if type_name not in _REGISTRY and type_name in _BUILTIN_MODULES:
        import_module(_BUILTIN_MODULES[type_name])
    try:
        return _REGISTRY[type_name]
    except KeyError as exc:
        raise KeyError(f"no sink bound for type {type_name!r}") from exc


def reset_sink_registry_for_tests() -> None:
    _REGISTRY.clear()
