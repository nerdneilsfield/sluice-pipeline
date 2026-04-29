import pytest

from sluice.core.errors import ConfigError
from sluice.core.item import Item
from sluice.processors.field_filter import FieldFilterProcessor
from tests.conftest import make_ctx


def _make_item(**kw):
    base = dict(
        source_id="s",
        pipeline_id="p",
        guid="g",
        url="u",
        title="T",
        published_at=None,
        raw_summary=None,
    )
    base.update(kw)
    return Item(**base)


@pytest.mark.asyncio
async def test_lower_op():
    p = FieldFilterProcessor(name="t", ops=[{"op": "lower", "field": "title"}])
    ctx = make_ctx(items=[_make_item(title="HELLO")])
    out = await p.process(ctx)
    assert out.items[0].title == "hello"


@pytest.mark.asyncio
async def test_strip_op_default_whitespace():
    p = FieldFilterProcessor(name="t", ops=[{"op": "strip", "field": "title"}])
    ctx = make_ctx(items=[_make_item(title="  abc \n")])
    out = await p.process(ctx)
    assert out.items[0].title == "abc"


@pytest.mark.asyncio
async def test_strip_op_custom_chars():
    p = FieldFilterProcessor(name="t", ops=[{"op": "strip", "field": "title", "chars": "*-"}])
    ctx = make_ctx(items=[_make_item(title="**abc--")])
    out = await p.process(ctx)
    assert out.items[0].title == "abc"


@pytest.mark.asyncio
async def test_regex_replace_op():
    p = FieldFilterProcessor(
        name="t",
        ops=[
            {
                "op": "regex_replace",
                "field": "title",
                "pattern": r"\[ad\].*$",
                "replacement": "",
            }
        ],
    )
    ctx = make_ctx(items=[_make_item(title="story [ad] sponsor")])
    out = await p.process(ctx)
    assert out.items[0].title == "story "


@pytest.mark.asyncio
async def test_op_none_field_is_noop():
    p = FieldFilterProcessor(name="t", ops=[{"op": "lower", "field": "summary"}])
    ctx = make_ctx(items=[_make_item()])
    out = await p.process(ctx)
    assert out.items[0].summary is None


def test_unknown_op_raises_at_construction():
    with pytest.raises(ConfigError):
        FieldFilterProcessor(name="t", ops=[{"op": "frobnicate", "field": "title"}])
