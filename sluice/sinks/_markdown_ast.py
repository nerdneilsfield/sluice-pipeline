from typing import Callable

from markdown_it import MarkdownIt
from markdown_it.token import Token

_md = MarkdownIt("commonmark")


def parse_markdown(text: str) -> list[Token]:
    return _md.parse(text)


_BLOCK_OPEN_SUFFIX = "_open"
_BLOCK_CLOSE_SUFFIX = "_close"


def _block_groups(tokens: list[Token]) -> list[list[Token]]:
    groups: list[list[Token]] = []
    cur: list[Token] = []
    depth = 0
    for tok in tokens:
        cur.append(tok)
        if tok.type.endswith(_BLOCK_OPEN_SUFFIX):
            depth += 1
        elif tok.type.endswith(_BLOCK_CLOSE_SUFFIX):
            depth -= 1
            if depth == 0:
                groups.append(cur)
                cur = []
        elif tok.type in {"fence", "code_block", "hr", "html_block"} and depth == 0:
            groups.append(cur)
            cur = []
    if cur:
        groups.append(cur)
    return groups


def split_tokens(
    tokens: list[Token],
    max_size: int,
    estimate_size: Callable[[list[Token]], int],
) -> list[list[Token]]:
    """Split tokens into chunks that each fit within *max_size*.

    Splits at block boundaries (paragraphs, headings, code blocks, etc.).
    If a single block is already larger than *max_size*, it is emitted as
    its own chunk — callers are expected to truncate the rendered output
    as a safety net (e.g. Telegram's ``_safe_truncate``).
    """
    groups = _block_groups(tokens)
    chunks: list[list[Token]] = []
    cur: list[Token] = []
    for g in groups:
        candidate = cur + g
        if cur and estimate_size(candidate) > max_size:
            chunks.append(cur)
            cur = list(g)
        else:
            cur = candidate
        if estimate_size(cur) > max_size:
            chunks.append(cur)
            cur = []
    if cur:
        chunks.append(cur)
    return chunks
