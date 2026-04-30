from bs4 import BeautifulSoup

from sluice.core.errors import SluiceError


class EnricherParseError(SluiceError):
    pass


def parse_hn_thread(html: str, top_n: int = 20) -> str:
    """Parse hckrnws.com thread page."""
    soup = BeautifulSoup(html, "html.parser")
    comments = soup.select(".comment")
    if not comments:
        raise EnricherParseError("hckrnws: no .comment elements found")
    out_lines: list[str] = []
    for c in comments[:top_n]:
        author_el = c.select_one(".author")
        body_el = c.select_one(".body") or c
        author = author_el.get_text(strip=True) if author_el else "?"
        body = body_el.get_text(" ", strip=True)
        out_lines.append(f"**{author}**: {body}")
    return "\n\n".join(out_lines)


def parse_hn_official(html: str, top_n: int = 20) -> str:
    """Parse official news.ycombinator.com item page."""
    soup = BeautifulSoup(html, "html.parser")
    # Top-level comments are tr.athing.comtr; indent=0 means top-level
    rows = soup.select("tr.athing.comtr")
    if not rows:
        raise EnricherParseError("hn official: no comment rows found")
    out_lines: list[str] = []
    for row in rows[:top_n]:
        # Skip deeply nested comments (indent > 0 via width of indent spacer)
        indent_el = row.select_one("td.ind img")
        if indent_el:
            width = int(indent_el.get("width", "0"))
            if width > 0:
                continue
        author_el = row.select_one("a.hnuser")
        text_el = row.select_one("span.commtext")
        if not text_el:
            continue
        author = author_el.get_text(strip=True) if author_el else "?"
        body = text_el.get_text(" ", strip=True)
        if body:
            out_lines.append(f"**{author}**: {body}")
        if len(out_lines) >= top_n:
            break
    if not out_lines:
        raise EnricherParseError("hn official: no parseable comments found")
    return "\n\n".join(out_lines)
