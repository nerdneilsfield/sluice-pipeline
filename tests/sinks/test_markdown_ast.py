from sluice.sinks._markdown_ast import parse_markdown, split_tokens


def test_parse_returns_tokens():
    toks = parse_markdown("# h1\n\nbody\n")
    kinds = [t.type for t in toks]
    assert "heading_open" in kinds
    assert "paragraph_open" in kinds


def test_split_tokens_short_no_split():
    toks = parse_markdown("a\n\nb\n\nc\n")
    chunks = split_tokens(toks, max_size=1000, estimate_size=lambda ts: 1)
    assert len(chunks) == 1


def test_split_tokens_breaks_on_block_boundary():
    toks = parse_markdown("# h1\n\np1\n\n# h2\n\np2\n")
    chunks = split_tokens(toks, max_size=300, estimate_size=lambda ts: 100 * len(ts) // 2)
    assert len(chunks) >= 2
    for c in chunks:
        kinds = [t.type for t in c]
        assert any(k.endswith("_open") for k in kinds)


def test_split_never_inside_code_block():
    md = "```python\n" + "x = 1\n" * 100 + "```\n"
    toks = parse_markdown(md)
    chunks = split_tokens(toks, max_size=50, estimate_size=lambda ts: len(ts))
    fences_per = [sum(1 for t in c if t.type == "fence") for c in chunks]
    assert sum(fences_per) == 1


def test_split_oversized_single_block_emitted_as_own_chunk():
    md = "x" * 5000
    toks = parse_markdown(md)

    def estimate(ts):
        return sum(len(t.content) for t in ts)

    chunks = split_tokens(toks, max_size=100, estimate_size=estimate)
    assert len(chunks) == 1
    assert len(chunks[0]) > 0
    assert estimate(chunks[0]) > 100
