"""Built-in Task bindings.

Only deterministic collection/content Tasks are registered in this slice;
LLM and renderer bindings belong to their later owning slices.
"""

from . import collection, content
from .collection import dedup, filter, limit, sort
from .content import extract_content, fetch_webpage, to_markdown, to_text

__all__ = [
    "collection",
    "content",
    "dedup",
    "extract_content",
    "fetch_webpage",
    "filter",
    "limit",
    "sort",
    "to_markdown",
    "to_text",
]
