from typing import Any

from markdown_it import MarkdownIt
from markdown_it.token import Token

Cleaner: Any
try:
    import lxml_html_clean
except ImportError:
    Cleaner = None
else:
    Cleaner = lxml_html_clean.Cleaner

_md = MarkdownIt("commonmark")


def render_to_html(tokens: list[Token]) -> str:
    raw = _md.renderer.render(tokens, _md.options, env={})
    if not raw:
        return raw
    if Cleaner is None:
        return raw

    cleaner = Cleaner(
        scripts=True,
        javascript=True,
        embedded=True,
        frames=True,
        meta=True,
        style=False,
        remove_unknown_tags=False,
    )
    return cleaner.clean_html(raw)
