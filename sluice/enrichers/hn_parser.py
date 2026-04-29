from bs4 import BeautifulSoup


class EnricherParseError(RuntimeError):
    pass


def parse_hn_thread(html: str, top_n: int = 20) -> str:
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
