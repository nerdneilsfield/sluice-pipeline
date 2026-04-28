from dataclasses import fields as dc_fields

from sluice.config import FieldOp
from sluice.context import PipelineContext
from sluice.core.item import Item

_DC_NAMES = {f.name for f in dc_fields(Item)}


def _truncate(it: Item, field: str, n: int):
    if "." in field:
        head, _, tail = field.partition(".")
        bucket = getattr(it, head, None)
        if isinstance(bucket, dict) and tail in bucket and isinstance(bucket[tail], str):
            bucket[tail] = bucket[tail][:n]
        return
    if field in _DC_NAMES:
        v = getattr(it, field)
        if isinstance(v, str):
            setattr(it, field, v[:n])


def _drop(it: Item, field: str):
    if "." in field:
        head, _, tail = field.partition(".")
        bucket = getattr(it, head, None)
        if isinstance(bucket, dict):
            bucket.pop(tail, None)
        return
    if field in _DC_NAMES:
        setattr(it, field, None)


class FieldFilterProcessor:
    name = "field_filter"

    def __init__(self, *, name: str, ops: list[FieldOp]):
        self.name = name
        self.ops = ops

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        for it in ctx.items:
            for op in self.ops:
                if op.op == "truncate":
                    _truncate(it, op.field, op.n or 0)
                elif op.op == "drop":
                    _drop(it, op.field)
        return ctx
