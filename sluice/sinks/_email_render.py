from markdown_it import MarkdownIt
from markdown_it.token import Token

_md = MarkdownIt("commonmark")


def render_to_html(tokens: list[Token]) -> str:
    return _md.renderer.render(tokens, _md.options, env={})
