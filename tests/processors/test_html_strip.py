import pytest

from sluice.config import HtmlStripConfig
from sluice.core.errors import ConfigError
from sluice.processors.html_strip import HtmlStripProcessor
from tests.conftest import make_ctx, make_item


def _proc(*fields: str) -> HtmlStripProcessor:
    return HtmlStripProcessor(name="html", fields=list(fields))


@pytest.mark.asyncio
async def test_strips_html_from_top_level_field_with_block_semantics():
    item = make_item(
        fulltext=(
            "<h1>Hello&nbsp;world</h1>"
            "<p>Alpha <strong> beta</strong><br>Gamma</p>"
            "<script>alert('drop me')</script>"
            "<style>.drop { display: none; }</style>"
            "<template><p>hidden</p></template>"
            "<div>Tail &amp; end</div>"
        )
    )

    ctx = await _proc("fulltext").process(make_ctx(items=[item]))

    assert ctx.items[0].fulltext == "Hello world\n\nAlpha beta\nGamma\n\nTail & end"


@pytest.mark.asyncio
async def test_block_tags_preserve_blank_line_around_inline_text():
    item = make_item(raw_summary="Intro<p>Para</p>Tail")

    ctx = await _proc("raw_summary").process(make_ctx(items=[item]))

    assert ctx.items[0].raw_summary == "Intro\n\nPara\n\nTail"


@pytest.mark.asyncio
async def test_strips_html_from_extras_field():
    item = make_item(extras={"body": "<section><p>One</p><p>Two</p></section>"})

    ctx = await _proc("extras.body").process(make_ctx(items=[item]))

    assert ctx.items[0].extras["body"] == "One\n\nTwo"


@pytest.mark.asyncio
async def test_missing_and_non_string_fields_skip_silently():
    item = make_item(
        title="<p>Keep <em>text</em></p>",
        raw_summary=None,
        extras={"count": 3},
    )

    ctx = await _proc("title", "raw_summary", "extras.missing", "extras.count").process(
        make_ctx(items=[item])
    )

    assert ctx.items[0].title == "Keep text"
    assert ctx.items[0].raw_summary is None
    assert "missing" not in ctx.items[0].extras
    assert ctx.items[0].extras["count"] == 3


@pytest.mark.asyncio
async def test_entities_and_nbsp_collapse_to_single_spaces():
    item = make_item(raw_summary="<p>A&nbsp;&nbsp;B &#169; &lt;tag&gt;</p>")

    ctx = await _proc("raw_summary").process(make_ctx(items=[item]))

    assert ctx.items[0].raw_summary == "A B \xa9 <tag>"


@pytest.mark.asyncio
async def test_self_closing_drop_content_tag_does_not_leak_following_text():
    item = make_item(raw_summary="<template/>visible")

    ctx = await _proc("raw_summary").process(make_ctx(items=[item]))

    assert ctx.items[0].raw_summary == ""


def test_html_strip_config_accepts_top_level_and_extras_paths():
    cfg = HtmlStripConfig(type="html_strip", name="html", fields=["fulltext", "extras.body"])

    assert cfg.fields == ["fulltext", "extras.body"]


def test_html_strip_config_rejects_empty_extras_key():
    with pytest.raises(ConfigError):
        HtmlStripConfig(type="html_strip", name="html", fields=["extras."])


def test_html_strip_config_rejects_empty_fields():
    with pytest.raises(ConfigError):
        HtmlStripConfig(type="html_strip", name="html", fields=[])


def test_html_strip_config_rejects_deep_paths():
    with pytest.raises(ConfigError):
        HtmlStripConfig(type="html_strip", name="html", fields=["a.b.c"])

    with pytest.raises(ConfigError):
        HtmlStripConfig(type="html_strip", name="html", fields=["extras.body.html"])
