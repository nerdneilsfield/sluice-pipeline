from dataclasses import fields as dc_fields

import regex
from pydantic import TypeAdapter

from sluice.config import FieldOp
from sluice.context import PipelineContext
from sluice.core.errors import ConfigError
from sluice.core.item import Item

_DC_NAMES = {f.name for f in dc_fields(Item)}

_FIELD_OP_ADAPTER = TypeAdapter(FieldOp)


def _validate_op(op):
    if isinstance(op, dict):
        try:
            return _FIELD_OP_ADAPTER.validate_python(op)
        except Exception:
            raise ConfigError(f"unknown field_filter op: {op!r}")
    return op


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


def _lower(it: Item, field: str):
    if "." in field:
        head, _, tail = field.partition(".")
        bucket = getattr(it, head, None)
        if isinstance(bucket, dict) and tail in bucket and isinstance(bucket[tail], str):
            bucket[tail] = bucket[tail].lower()
        return
    if field in _DC_NAMES:
        v = getattr(it, field)
        if isinstance(v, str):
            setattr(it, field, v.lower())


def _strip(it: Item, field: str, chars: str | None):
    if "." in field:
        head, _, tail = field.partition(".")
        bucket = getattr(it, head, None)
        if isinstance(bucket, dict) and tail in bucket and isinstance(bucket[tail], str):
            bucket[tail] = bucket[tail].strip(chars)
        return
    if field in _DC_NAMES:
        v = getattr(it, field)
        if isinstance(v, str):
            setattr(it, field, v.strip(chars))


def _regex_replace(it: Item, field: str, pattern: str, replacement: str, count: int):
    compiled = regex.compile(pattern)
    if "." in field:
        head, _, tail = field.partition(".")
        bucket = getattr(it, head, None)
        if isinstance(bucket, dict) and tail in bucket and isinstance(bucket[tail], str):
            bucket[tail] = compiled.sub(replacement, bucket[tail], count=count or 0, timeout=2.0)
        return
    if field in _DC_NAMES:
        v = getattr(it, field)
        if isinstance(v, str):
            setattr(it, field, compiled.sub(replacement, v, count=count or 0, timeout=2.0))


class FieldFilterProcessor:
    name = "field_filter"

    def __init__(self, *, name: str, ops: list[FieldOp | dict]):
        self.name = name
        self.ops = [_validate_op(op) for op in ops]

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        for it in ctx.items:
            for op in self.ops:
                if op.op == "truncate":
                    _truncate(it, op.field, op.n or 0)
                elif op.op == "drop":
                    _drop(it, op.field)
                elif op.op == "lower":
                    _lower(it, op.field)
                elif op.op == "strip":
                    _strip(it, op.field, op.chars)
                elif op.op == "regex_replace":
                    _regex_replace(it, op.field, op.pattern, op.replacement, op.count)
        return ctx
