from markdown_it import MarkdownIt
from markdown_it.token import Token

_md = MarkdownIt("commonmark")


def render_to_html(tokens: list[Token]) -> str:
    raw = _md.renderer.render(tokens, _md.options, env={})
    try:
        from lxml_html_clean import Cleaner

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
    except Exception:
        return raw
