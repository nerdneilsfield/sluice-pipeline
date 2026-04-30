"""
Convert Markdown to a Feishu post-message structure.

Output format:
    {"zh_cn": {"title": str, "content": list[list[dict]]}}

PostNode dicts:
    {"tag": "text", "text": "...", "style": ["bold"]}   (style omitted when empty)
    {"tag": "a",    "text": "...", "href": "..."}
    {"tag": "code_block", "language": "go", "text": "..."} (language omitted when empty)
    {"tag": "md",   "text": "..."}                       (for lists and blockquotes)
"""

from __future__ import annotations

from markdown_it import MarkdownIt
from markdown_it.token import Token

_md = MarkdownIt("commonmark").enable("strikethrough")

_NORMALIZE_LANGS = {"", "text", "txt", "plaintext", "plain_text", "plain"}


def _normalize_language(lang: str) -> str:
    return "" if lang.strip().lower() in _NORMALIZE_LANGS else lang.strip()


def _same_styles(a: list[str] | None, b: list[str] | None) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return a == b


def _append_merged(out: list[dict], item: dict) -> list[dict]:
    """Merge adjacent same-style text nodes (mirrors Go appendMergedPostNode)."""
    if item["tag"] == "text" and not item.get("text", "").strip():
        return out
    if not out:
        return [item]
    last = out[-1]
    if (
        last["tag"] == "text"
        and item["tag"] == "text"
        and _same_styles(last.get("style"), item.get("style"))
        and not last.get("href")
        and not item.get("href")
    ):
        merged = {**last, "text": last["text"] + item["text"]}
        return out[:-1] + [merged]
    return out + [item]


def _make_text_node(text: str, style: str = "") -> dict:
    node: dict = {"tag": "text", "text": text}
    if style:
        node["style"] = [style]
    return node


# ---------------------------------------------------------------------------
# Inline renderer — returns a list of post-nodes from inline token children
# ---------------------------------------------------------------------------

def _render_inlines(children: list[Token]) -> list[dict]:
    out: list[dict] = []
    idx = 0
    while idx < len(children):
        tok = children[idx]
        if tok.type == "text":
            node = _make_text_node(tok.content)
            out = _append_merged(out, node)
        elif tok.type in ("softbreak", "hardbreak"):
            node = _make_text_node("\n")
            out = _append_merged(out, node)
        elif tok.type in ("strong_open", "em_open", "s_open"):
            # Collect tokens until matching close
            style_map = {
                "strong_open": ("bold", "strong_close"),
                "em_open": ("italic", "em_close"),
                "s_open": ("lineThrough", "s_close"),
            }
            style, close_type = style_map[tok.type]
            idx += 1
            inner: list[Token] = []
            depth = 1
            while idx < len(children) and depth > 0:
                if children[idx].type == tok.type:
                    depth += 1
                elif children[idx].type == close_type:
                    depth -= 1
                    if depth == 0:
                        break
                if depth > 0:
                    inner.append(children[idx])
                idx += 1
            # Render inner as plain text with style (no nested styles — match Go)
            text = _render_plain_text_from_tokens(inner)
            if text:
                node = _make_text_node(text, style)
                out = _append_merged(out, node)
        elif tok.type == "link_open":
            href = (tok.attrs or {}).get("href", "")
            idx += 1
            label_tokens: list[Token] = []
            while idx < len(children) and children[idx].type != "link_close":
                label_tokens.append(children[idx])
                idx += 1
            label = _render_plain_text_from_tokens(label_tokens)
            out = out + [{"tag": "a", "text": label, "href": href}]
        elif tok.type == "code_inline":
            node = _make_text_node(f"`{tok.content}`")
            out = _append_merged(out, node)
        idx += 1
    return out


def _render_plain_text_from_tokens(tokens: list[Token]) -> str:
    parts: list[str] = []
    for tok in tokens:
        if tok.type == "text":
            parts.append(tok.content)
        elif tok.type in ("softbreak", "hardbreak"):
            parts.append(" ")
        elif tok.type == "code_inline":
            parts.append(tok.content)
        elif tok.children:
            parts.append(_render_plain_text_from_tokens(tok.children))
    return "".join(parts)


# ---------------------------------------------------------------------------
# Block-level list/blockquote markdown renderer (for "md" nodes)
# ---------------------------------------------------------------------------

def _prefix_each_line(text: str, prefix: str) -> str:
    if not text:
        return ""
    lines = text.split("\n")
    return "\n".join(prefix + line for line in lines)


def _indent_block(text: str, prefix: str) -> str:
    """Apply prefix to first line; indent subsequent lines by same width."""
    if not text:
        return prefix
    lines = text.split("\n")
    indent = " " * len(prefix)
    result = [prefix + lines[0]]
    for line in lines[1:]:
        result.append(indent + line)
    return "\n".join(result)


# ---------------------------------------------------------------------------
# Block-level token group processor
# ---------------------------------------------------------------------------

class _Converter:
    def __init__(self, all_tokens: list[Token]):
        self._tokens = all_tokens
        self.title: str = ""
        self._title_seen: bool = False
        self.content: list[list[dict]] = []

    def convert(self) -> None:
        groups = _group_blocks(self._tokens)
        for group in groups:
            rows = self._convert_group(group)
            if rows:
                self.content.extend(rows)

    def _convert_group(self, group: list[Token]) -> list[list[dict]]:
        if not group:
            return []
        first = group[0]

        # Heading H1 → title (first occurrence only)
        if first.type == "heading_open" and first.markup == "#":
            if not self._title_seen:
                inline_tok = next((t for t in group if t.type == "inline"), None)
                if inline_tok and inline_tok.children:
                    self.title = _render_plain_text_from_tokens(inline_tok.children)
                self._title_seen = True
            # Subsequent H1s are ignored (match Go: return unsupportedNodeError)
            return []

        # Paragraph
        if first.type == "paragraph_open":
            inline_tok = next((t for t in group if t.type == "inline"), None)
            if not inline_tok or not inline_tok.children:
                return []
            nodes = _render_inlines(inline_tok.children)
            return [nodes] if nodes else []

        # Fenced code block
        if first.type == "fence":
            lang = _normalize_language(first.info)
            text = first.content.rstrip("\r\n")
            node: dict = {"tag": "code_block", "text": text}
            if lang:
                node["language"] = lang
            return [[node]]

        # Indented code block
        if first.type == "code_block":
            text = first.content.rstrip("\r\n")
            return [[{"tag": "code_block", "text": text}]]

        # Blockquote
        if first.type == "blockquote_open":
            inner = group[1:-1]  # strip blockquote_open / blockquote_close
            text = _render_blocks_as_markdown(inner)
            if not text:
                return []
            return [[{"tag": "md", "text": _prefix_each_line(text, "> ")}]]

        # Bullet list
        if first.type == "bullet_list_open":
            text = _render_list_as_markdown(group, ordered=False)
            if not text:
                return []
            return [[{"tag": "md", "text": text}]]

        # Ordered list
        if first.type == "ordered_list_open":
            text = _render_list_as_markdown(group, ordered=True)
            if not text:
                return []
            return [[{"tag": "md", "text": text}]]

        return []


# ---------------------------------------------------------------------------
# Token grouping — split flat token list into logical block groups
# ---------------------------------------------------------------------------

def _group_blocks(tokens: list[Token]) -> list[list[Token]]:
    """Group flat token stream into per-block groups."""
    groups: list[list[Token]] = []
    cur: list[Token] = []
    depth = 0

    for tok in tokens:
        cur.append(tok)
        if tok.type.endswith("_open"):
            depth += 1
        elif tok.type.endswith("_close"):
            depth -= 1
            if depth == 0:
                groups.append(cur)
                cur = []
        elif tok.type in ("fence", "code_block", "hr", "html_block") and depth == 0:
            groups.append(cur)
            cur = []

    if cur:
        groups.append(cur)

    return groups


# ---------------------------------------------------------------------------
# Markdown re-serialisation for lists and blockquotes (used in "md" nodes)
# ---------------------------------------------------------------------------

def _render_blocks_as_markdown(tokens: list[Token]) -> str:
    """Re-serialise a sequence of block tokens as Markdown text."""
    parts: list[str] = []
    groups = _group_blocks(tokens)
    for group in groups:
        if not group:
            continue
        first = group[0]
        if first.type == "paragraph_open":
            inline_tok = next((t for t in group if t.type == "inline"), None)
            if inline_tok and inline_tok.children:
                parts.append(_render_inline_markdown_proper(inline_tok.children))
        elif first.type == "fence":
            lang = first.info.strip()
            code = first.content.rstrip("\r\n")
            if lang:
                parts.append(f"```{lang}\n{code}\n```")
            else:
                parts.append(f"```\n{code}\n```")
        elif first.type == "code_block":
            code = first.content.rstrip("\r\n")
            parts.append(f"```\n{code}\n```")
        elif first.type in ("bullet_list_open", "ordered_list_open"):
            parts.append(_render_list_as_markdown(group, ordered=first.type == "ordered_list_open"))
        elif first.type == "blockquote_open":
            inner = group[1:-1]
            text = _render_blocks_as_markdown(inner)
            if text:
                parts.append(_prefix_each_line(text, "> "))
    return "\n\n".join(parts)


def _render_inline_markdown_proper(children: list[Token]) -> str:
    """Re-serialise inline tokens to Markdown, correctly handling links."""
    parts: list[str] = []
    idx = 0
    while idx < len(children):
        tok = children[idx]
        if tok.type == "text":
            parts.append(tok.content)
        elif tok.type == "softbreak":
            parts.append("\n")
        elif tok.type == "hardbreak":
            parts.append("  \n")
        elif tok.type == "strong_open":
            parts.append("**")
        elif tok.type == "strong_close":
            parts.append("**")
        elif tok.type == "em_open":
            parts.append("*")
        elif tok.type == "em_close":
            parts.append("*")
        elif tok.type == "s_open":
            parts.append("~~")
        elif tok.type == "s_close":
            parts.append("~~")
        elif tok.type == "code_inline":
            parts.append(f"`{tok.content}`")
        elif tok.type == "link_open":
            href = (tok.attrs or {}).get("href", "")
            idx += 1
            label_parts: list[str] = []
            while idx < len(children) and children[idx].type != "link_close":
                if children[idx].type == "text":
                    label_parts.append(children[idx].content)
                idx += 1
            label = "".join(label_parts)
            parts.append(f"[{label}]({href})")
        idx += 1
    return "".join(parts)


def _render_list_as_markdown(list_group: list[Token], ordered: bool) -> str:
    """Convert a list token group into its Markdown representation."""
    # Find list items — each is list_item_open ... list_item_close
    items: list[list[Token]] = []
    cur_item: list[Token] = []
    depth = 0
    for tok in list_group:
        if tok.type in ("bullet_list_open", "ordered_list_open"):
            if depth == 0:
                depth += 1
                continue
        if tok.type in ("bullet_list_close", "ordered_list_close"):
            if depth == 1:
                depth -= 1
                continue
        if tok.type == "list_item_open" and depth == 1:
            cur_item = []
        elif tok.type == "list_item_close" and depth == 1:
            items.append(cur_item)
            cur_item = []
        else:
            cur_item.append(tok)

    lines: list[str] = []
    for i, item_tokens in enumerate(items):
        body = _render_blocks_as_markdown(item_tokens)
        if ordered:
            prefix = f"{i + 1}. "
        else:
            prefix = "- "
        lines.append(_indent_block(body, prefix))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def convert_to_feishu_post(markdown_text: str) -> dict:
    """Convert Markdown to a Feishu post message structure.

    Returns:
        {"zh_cn": {"title": str, "content": list[list[dict]]}}
    """
    tokens = _md.parse(markdown_text)
    conv = _Converter(tokens)
    conv.convert()
    return {
        "zh_cn": {
            "title": conv.title,
            "content": conv.content,
        }
    }
