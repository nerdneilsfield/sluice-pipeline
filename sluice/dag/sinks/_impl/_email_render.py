from typing import Any

from bs4 import BeautifulSoup
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
_DANGEROUS_TAGS = {"script", "iframe", "frame", "frameset", "object", "embed", "meta"}
_URI_ATTRS = {"href", "src", "xlink:href", "formaction"}
_SAFE_SCHEMES = {"http", "https", "mailto", "cid"}


def _has_unsafe_scheme(value: str) -> bool:
    stripped = value.strip().lower()
    if not stripped or stripped.startswith(("#", "/", "./", "../")):
        return False
    if ":" not in stripped:
        return False
    scheme = stripped.split(":", 1)[0]
    return scheme not in _SAFE_SCHEMES


def _fallback_clean_html(raw: str) -> str:
    soup = BeautifulSoup(raw, "html.parser")
    for tag in soup.find_all(_DANGEROUS_TAGS):
        tag.decompose()
    for tag in soup.find_all(True):
        for attr, value in list(tag.attrs.items()):
            attr_lower = attr.lower()
            values = value if isinstance(value, list) else [value]
            value_text = " ".join(str(v) for v in values)
            if attr_lower.startswith("on") or attr_lower == "srcdoc":
                del tag.attrs[attr]
            elif attr_lower in _URI_ATTRS and _has_unsafe_scheme(value_text):
                del tag.attrs[attr]
            elif attr_lower == "style" and any(
                bad in value_text.lower() for bad in ("javascript:", "expression(")
            ):
                del tag.attrs[attr]
    return str(soup)


def render_to_html(tokens: list[Token]) -> str:
    raw = _md.renderer.render(tokens, _md.options, env={})
    if not raw:
        return raw
    if Cleaner is None:
        return _fallback_clean_html(raw)

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
