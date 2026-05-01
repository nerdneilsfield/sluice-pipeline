import re
from dataclasses import fields as dc_fields
from html.parser import HTMLParser
from typing import Any

from sluice.context import PipelineContext
from sluice.core.item import Item

_ITEM_FIELD_NAMES = {field.name for field in dc_fields(Item)}
_DROP_CONTENT_TAGS = {"script", "style", "template"}
_BLOCK_TAGS = {
    "article",
    "blockquote",
    "div",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "li",
    "p",
    "section",
    "td",
    "th",
}


class _PlainTextHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._chunks: list[str] = []
        self._drop_depth = 0

    def handle_starttag(self, tag: str, _attrs: Any) -> None:  # ty: ignore[invalid-method-override]
        normalized = tag.lower()
        if normalized in _DROP_CONTENT_TAGS:
            self._drop_depth += 1
            return
        if self._drop_depth:
            return
        if normalized == "br" or normalized in _BLOCK_TAGS:
            self._append_break("\n" if normalized == "br" else "\n\n")

    def handle_startendtag(self, tag: str, _attrs: Any) -> None:  # ty: ignore[invalid-method-override]
        normalized = tag.lower()
        if self._drop_depth:
            return
        if normalized == "br" or normalized in _BLOCK_TAGS:
            self._append_break("\n" if normalized == "br" else "\n\n")

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.lower()
        if normalized in _DROP_CONTENT_TAGS and self._drop_depth:
            self._drop_depth -= 1
            return
        if self._drop_depth:
            return
        if normalized in _BLOCK_TAGS:
            self._append_break("\n\n")

    def handle_data(self, data: str) -> None:
        if not self._drop_depth:
            self._chunks.append(data)

    def get_text(self) -> str:
        return "".join(self._chunks)

    def _append_break(self, value: str) -> None:
        self._chunks.append(value)


def _strip_html(value: str) -> str:
    parser = _PlainTextHTMLParser()
    parser.feed(value)
    parser.close()
    lines = [re.sub(r"[^\S\n]+", " ", line).strip() for line in parser.get_text().split("\n")]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def _set_stripped_field(item: Item, field: str):
    if field.startswith("extras."):
        key = field[len("extras.") :]
        value = item.extras.get(key)
        if isinstance(value, str):
            item.extras[key] = _strip_html(value)
        return
    if field in _ITEM_FIELD_NAMES:
        value = getattr(item, field)
        if isinstance(value, str):
            setattr(item, field, _strip_html(value))


class HtmlStripProcessor:
    name = "html_strip"

    def __init__(self, *, name: str, fields: list[str]):
        self.name = name
        self.fields = fields

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        for item in ctx.items:
            for field in self.fields:
                _set_stripped_field(item, field)
        return ctx
