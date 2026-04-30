from markdown_it.token import Token

_ESCAPE_CHARS = "_*[]()~`>#+-=|{}.!"


def _escape(text: str) -> str:
    out = []
    for ch in text:
        if ch in _ESCAPE_CHARS:
            out.append("\\")
        out.append(ch)
    return "".join(out)


_LINK_MARK = "\x00LINK\x00"


def render_to_markdown_v2(tokens: list[Token]) -> str:
    import re

    out: list[str] = []
    in_pre = False
    for tok in tokens:
        if tok.type == "text":
            out.append(_escape(tok.content) if not in_pre else tok.content)
        elif tok.type in ("softbreak", "hardbreak"):
            out.append("\n")
        elif tok.type == "paragraph_open":
            pass
        elif tok.type == "paragraph_close":
            out.append("\n\n")
        elif tok.type == "heading_open":
            out.append("*")
        elif tok.type == "heading_close":
            out.append("*\n\n")
        elif tok.type == "strong_open":
            out.append("*")
        elif tok.type == "strong_close":
            out.append("*")
        elif tok.type == "em_open":
            out.append("_")
        elif tok.type == "em_close":
            out.append("_")
        elif tok.type == "code_inline":
            out.append("`" + tok.content.replace("`", "\\`") + "`")
        elif tok.type in ("fence", "code_block"):
            in_pre = True
            out.append("```\n" + tok.content + "```\n\n")
            in_pre = False
        elif tok.type == "link_open":
            href = next((a[1] for a in (tok.attrs or {}).items() if a[0] == "href"), "")
            out.append("[")
            out.append(_LINK_MARK + href + "\x01")
        elif tok.type == "link_close":
            out.append("\x02")
        elif tok.type == "inline":
            out.append(render_to_markdown_v2(tok.children or []))
        elif tok.type in ("bullet_list_open", "ordered_list_open"):
            pass
        elif tok.type in ("bullet_list_close", "ordered_list_close"):
            out.append("\n")
        elif tok.type == "list_item_open":
            out.append("• ")
        elif tok.type == "list_item_close":
            out.append("\n")
    raw = "".join(out)
    raw = re.sub(
        r"\[" + re.escape(_LINK_MARK) + r"(.*?)\x01(.*?)\x02",
        lambda m: f"[{m.group(2)}]({m.group(1)})",
        raw,
    )
    return raw.rstrip() + "\n"
