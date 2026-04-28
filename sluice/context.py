from dataclasses import dataclass, field
from typing import Any
from sluice.core.item import Item


@dataclass
class RunStats:
    items_in: int = 0
    items_out: int = 0
    llm_calls: int = 0
    est_cost_usd: float = 0.0


@dataclass
class PipelineContext:
    pipeline_id: str
    run_key: str
    run_date: str
    items: list[Item]
    context: dict[str, Any]
    stats: RunStats = field(default_factory=RunStats)
    items_resolved_from_failures: list[Item] = field(default_factory=list)
