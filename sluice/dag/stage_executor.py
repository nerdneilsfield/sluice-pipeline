from __future__ import annotations

from abc import ABC, abstractmethod
from importlib import import_module

from sluice.dag.stage_context import StageContext, StageResult


class StageExecutor(ABC):
    type_name: str = ""
    reorders_items: bool = False

    @abstractmethod
    async def execute(self, ctx: StageContext) -> StageResult: ...


_REGISTRY: dict[str, type[StageExecutor]] = {}


def bind_executor(type_name: str, cls: type[StageExecutor]) -> None:
    if type_name in _REGISTRY:
        raise ValueError(f"executor already bound for type {type_name!r}")
    _REGISTRY[type_name] = cls


def get_executor_class(type_name: str) -> type[StageExecutor]:
    if type_name not in _REGISTRY:
        import_module("sluice.dag.stages.executors").bind_all()
    try:
        return _REGISTRY[type_name]
    except KeyError as exc:
        raise KeyError(f"no executor bound for stage type {type_name!r}") from exc


def is_bound(type_name: str) -> bool:
    return type_name in _REGISTRY


def reset_registry_for_tests() -> None:
    _REGISTRY.clear()
