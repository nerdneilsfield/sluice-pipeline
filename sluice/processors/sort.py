from __future__ import annotations

from dataclasses import dataclass

from sluice.context import PipelineContext
from sluice.core.item import Item

_MISSING = object()


@dataclass
class SortProcessor:
    name: str
    sort_by: str
    sort_order: str = "desc"
    sort_missing: str = "last"

    def _sort_key(self, item: Item):
        val = item.get(self.sort_by, _MISSING)
        if val is _MISSING or val is None:
            if self.sort_missing == "first":
                return (0, 0)
            return (2, 0)
        try:
            num = float(val)
        except (TypeError, ValueError):
            num = 0.0
        direction = -1 if self.sort_order == "desc" else 1
        return (1, num * direction)

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        items = list(ctx.items)
        if self.sort_missing == "drop":
            items = [it for it in items if it.get(self.sort_by, _MISSING) not in (_MISSING, None)]
        ctx.items = sorted(items, key=self._sort_key)
        return ctx
