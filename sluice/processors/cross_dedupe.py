import difflib
from dataclasses import replace

from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.logging_setup import get_logger

log = get_logger(__name__)


class CrossDedupeProcessor:
    name = "cross_dedupe"

    def __init__(
        self,
        *,
        name: str,
        title_similarity_threshold: float = 0.85,
        source_priority: list[str] | None = None,
        merge_tags: bool = True,
    ):
        self.name = name
        self.title_similarity_threshold = title_similarity_threshold
        self.source_priority = list(source_priority or [])
        self.merge_tags = merge_tags

    def _priority_rank(self, item: Item) -> int:
        try:
            return self.source_priority.index(item.source_id)
        except ValueError:
            return len(self.source_priority)

    def _pick_winner(self, group: list[Item]) -> tuple[Item, list[Item]]:
        best_index = min(range(len(group)), key=lambda index: self._priority_rank(group[index]))
        return group[best_index], group[:best_index] + group[best_index + 1 :]

    def _with_merged_tags(self, kept: Item, dropped: list[Item]) -> Item:
        if not self.merge_tags:
            return kept

        tags = list(kept.tags)
        seen = set(tags)
        for item in dropped:
            for tag in item.tags:
                if tag not in seen:
                    tags.append(tag)
                    seen.add(tag)
        return replace(kept, tags=tags)

    def _url_round(self, items: list[Item]) -> list[Item]:
        groups: dict[str, list[Item]] = {}
        for item in items:
            if item.url:
                groups.setdefault(item.url, []).append(item)

        dropped_ids: set[int] = set()
        replacements: dict[int, Item] = {}
        for url, group in groups.items():
            if len(group) == 1:
                continue

            kept, dropped = self._pick_winner(group)
            replacements[id(kept)] = self._with_merged_tags(kept, dropped)
            dropped_ids.update(id(item) for item in dropped)
            for item in dropped:
                log.bind(
                    stage=self.name,
                    method="url",
                    url=url,
                    ratio=1.0,
                    kept_source=kept.source_id,
                    dropped_source=item.source_id,
                ).debug("cross_dedupe.merged")

        return [replacements.get(id(item), item) for item in items if id(item) not in dropped_ids]

    def _title_ratio(self, anchor: Item, other: Item) -> float:
        return difflib.SequenceMatcher(None, anchor.title.lower(), other.title.lower()).ratio()

    def _title_round(self, items: list[Item]) -> list[Item]:
        dropped_ids: set[int] = set()
        replacements: dict[int, Item] = {}
        processed_ids: set[int] = set()

        for index, anchor in enumerate(items):
            anchor_id = id(anchor)
            if anchor_id in processed_ids or not anchor.title:
                continue

            group = [anchor]
            for other in items[index + 1 :]:
                other_id = id(other)
                if other_id in processed_ids or not other.title:
                    continue
                if self._title_ratio(anchor, other) >= self.title_similarity_threshold:
                    group.append(other)

            processed_ids.update(id(item) for item in group)
            if len(group) == 1:
                continue

            kept, dropped = self._pick_winner(group)
            replacements[id(kept)] = self._with_merged_tags(kept, dropped)
            dropped_ids.update(id(item) for item in dropped)
            for item in dropped:
                log.bind(
                    stage=self.name,
                    method="title",
                    url=item.url,
                    ratio=self._title_ratio(kept, item),
                    kept_source=kept.source_id,
                    dropped_source=item.source_id,
                ).debug("cross_dedupe.merged")

        return [replacements.get(id(item), item) for item in items if id(item) not in dropped_ids]

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        ctx.items = self._title_round(self._url_round(list(ctx.items)))
        return ctx
