from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from sluice.context import PipelineContext
from sluice.core.errors import ConfigError
from sluice.core.item import Item

_MISSING = object()


@dataclass
class LimitProcessor:
    name: str
    top_n: int
    sort_by: str
    sort_order: str = "desc"
    sort_missing: str = "last"
    group_by: str | None = None
    per_group_max: int | None = None

    def __post_init__(self):
        if self.group_by is not None and self.per_group_max is None:
            raise ConfigError("per_group_max is required when group_by is set")

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

    def _sorted(self, items: list[Item]) -> list[Item]:
        return sorted(items, key=self._sort_key)

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        items = list(ctx.items)
        if self.sort_missing == "drop":
            items = [it for it in items if it.get(self.sort_by, _MISSING) not in (_MISSING, None)]
        if self.group_by is not None:
            groups: dict[str, list[Item]] = defaultdict(list)
            for it in items:
                key = it.get(self.group_by, "") or ""
                groups[str(key)].append(it)
            limited: list[Item] = []
            for grp_items in groups.values():
                limited.extend(self._sorted(grp_items)[: self.per_group_max])
            items = limited
        items = self._sorted(items)[: self.top_n]
        ctx.items = items
        return ctx
