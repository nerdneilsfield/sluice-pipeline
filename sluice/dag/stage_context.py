from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sluice.dag.item import Item


@dataclass(frozen=True)
class ItemDrop:
    item_id: str
    reason: str


@dataclass(frozen=True)
class ItemError:
    item_id: str
    error_class: str
    error_msg: str


@dataclass
class StageContext:
    task_name: str
    pipeline_name: str
    stage_name: str
    stage_type: str
    run_key: str
    cycle_anchor: str
    items: list[Item]
    params: dict[str, Any]

    def live_items(self) -> list[Item]:
        return [item for item in self.items if not item.is_dropped()]


@dataclass
class StageResult:
    drops: list[ItemDrop] = field(default_factory=list)
    errors: list[ItemError] = field(default_factory=list)
    new_items: list[Item] = field(default_factory=list)
    rendered_output: str | None = None

    def dropped_ids(self) -> set[str]:
        return {drop.item_id for drop in self.drops}

    def errored_ids(self) -> set[str]:
        return {error.item_id for error in self.errors}
