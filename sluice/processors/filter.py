import re
from datetime import datetime, timezone
from sluice.context import PipelineContext
from sluice.core.item import Item
from sluice.config import FilterRule
from sluice.window import parse_duration

def _coerce_dt(v):
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    return None

def _eval(rule: FilterRule, item: Item) -> bool:
    v = item.get(rule.field)
    op, target = rule.op, rule.value
    if op == "exists":     return v is not None
    if op == "not_exists": return v is None
    if v is None:          return False
    if op == "gt":  return v >  target
    if op == "gte": return v >= target
    if op == "lt":  return v <  target
    if op == "lte": return v <= target
    if op == "eq":  return v == target
    if op == "matches":      return bool(re.search(target, str(v)))
    if op == "not_matches":  return not re.search(target, str(v))
    if op == "contains":     return target in v
    if op == "not_contains": return target not in v
    if op == "in":           return v in target
    if op == "not_in":       return v not in target
    if op == "min_length":   return len(v) >= target
    if op == "max_length":   return len(v) <= target
    if op in ("newer_than", "older_than"):
        dt = _coerce_dt(v);
        if dt is None: return False
        cutoff = datetime.now(timezone.utc) - parse_duration(target)
        return dt > cutoff if op == "newer_than" else dt < cutoff
    raise ValueError(f"unknown op {op}")

class FilterProcessor:
    name = "filter"

    def __init__(self, *, name: str, mode: str, rules: list[FilterRule]):
        self.name = name
        self.mode = mode
        self.rules = rules

    def _matches(self, item: Item) -> bool:
        results = [_eval(r, item) for r in self.rules]
        if self.mode == "keep_if_all":  return all(results)
        if self.mode == "keep_if_any":  return any(results)
        if self.mode == "drop_if_all":  return not all(results)
        if self.mode == "drop_if_any":  return not any(results)
        raise ValueError(self.mode)

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        ctx.items = [it for it in ctx.items if self._matches(it)]
        return ctx
