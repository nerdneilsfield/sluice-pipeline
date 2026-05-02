from datetime import datetime
from typing import AsyncIterator

from sluice.config import SourceFilterConfig
from sluice.core.item import Item
from sluice.processors.filter import _eval


class _SourceFilterView:
    def __init__(self, item: Item):
        self.item = item

    def get(self, path: str, default=None):
        if path == "content":
            content = "\n".join(
                part for part in (self.item.raw_summary, self.item.fulltext) if part
            )
            return content or default
        return self.item.get(path, default)


class SourceFilter:
    def __init__(self, source, filter: SourceFilterConfig):
        self.source = source
        self.filter = filter

    def __getattr__(self, name: str):
        return getattr(self.source, name)

    def _matches(self, item: Item) -> bool:
        view = _SourceFilterView(item)
        results = [_eval(rule, view) for rule in self.filter.rules]
        if self.filter.mode == "keep_if_all":
            return all(results)
        if self.filter.mode == "keep_if_any":
            return any(results)
        if self.filter.mode == "drop_if_all":
            return not all(results)
        if self.filter.mode == "drop_if_any":
            return not any(results)
        raise ValueError(self.filter.mode)

    async def fetch(
        self,
        window_start: datetime,
        window_end: datetime,
    ) -> AsyncIterator[Item]:
        async for item in self.source.fetch(window_start, window_end):
            if self._matches(item):
                yield item


def wrap_source_filter(source, filter: SourceFilterConfig | None):
    if filter is None:
        return source
    return SourceFilter(source, filter)
