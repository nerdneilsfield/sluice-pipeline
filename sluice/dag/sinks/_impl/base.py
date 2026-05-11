from abc import ABC, abstractmethod

from sluice.context import PipelineContext

_REGISTRY: dict[str, type["Sink"]] = {}


def register_sink(type_name: str):
    def _decorator(cls: type["Sink"]) -> type["Sink"]:
        _REGISTRY[type_name] = cls
        return cls

    return _decorator


class Sink(ABC):
    type: str = "abstract"

    def __init__(self, *, id: str, mode: str = "upsert", emit_on_empty: bool = False):
        self.id = id
        self.mode = mode
        self.emit_on_empty = emit_on_empty

    @abstractmethod
    async def create(self, ctx: PipelineContext) -> str: ...

    async def update(self, external_id: str, ctx: PipelineContext) -> str:
        return await self.create(ctx)

    def _input_payload(self, ctx: PipelineContext):
        path = getattr(self, "input", None)
        if not path:
            return None
        cur = ctx
        for part in path.split("."):
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                cur = getattr(cur, part, None)
            if cur is None:
                return None
        return cur

    def _has_input_payload(self, ctx: PipelineContext) -> bool:
        value = self._input_payload(ctx)
        if value is None:
            return False
        if isinstance(value, str):
            return value != ""
        try:
            return len(value) > 0
        except TypeError:
            return True
