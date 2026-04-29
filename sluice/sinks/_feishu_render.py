from markdown_it.token import Token


def render_to_post_array(tokens: list[Token]) -> list[list[dict]]:
    lines: list[list[dict]] = []
    cur: list[dict] = []
    bold = False
    italic = False
    for tok in tokens:
        if tok.type == "text":
            seg: dict = {"tag": "text", "text": tok.content}
            if bold:
                seg["un_escape"] = False
                seg["bold"] = True
            if italic:
                seg["italic"] = True
            cur.append(seg)
        elif tok.type == "strong_open":
            bold = True
        elif tok.type == "strong_close":
            bold = False
        elif tok.type == "em_open":
            italic = True
        elif tok.type == "em_close":
            italic = False
        elif tok.type in ("softbreak", "hardbreak"):
            cur.append({"tag": "text", "text": "\n"})
        elif tok.type in ("paragraph_close", "heading_close"):
            if cur:
                lines.append(cur)
            cur = []
        elif tok.type == "link_open":
            href = ""
            if tok.attrs:
                for k, v in tok.attrs.items() if isinstance(tok.attrs, dict) else tok.attrs:
                    if k == "href":
                        href = v
            cur.append({"tag": "a", "text": "", "href": href})
        elif tok.type == "code_inline":
            cur.append({"tag": "text", "text": "`" + tok.content + "`"})
        elif tok.type in ("fence", "code_block"):
            lines.append([{"tag": "text", "text": "```\n" + tok.content + "```"}])
        elif tok.type == "list_item_close":
            if cur:
                cur.insert(0, {"tag": "text", "text": "• "})
                lines.append(cur)
                cur = []
        elif tok.type == "image":
            url = ""
            if tok.attrs:
                for k, v in tok.attrs.items() if isinstance(tok.attrs, dict) else tok.attrs:
                    if k == "src":
                        url = v
            cur.append({"tag": "a", "text": "🖼 image", "href": url})
        elif tok.type == "inline":
            sub = render_to_post_array(tok.children or [])
            for line in sub:
                cur.extend(line)
    if cur:
        lines.append(cur)
    return lines
