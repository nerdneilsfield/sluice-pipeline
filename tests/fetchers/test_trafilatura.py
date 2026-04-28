import pytest, httpx, respx
from sluice.fetchers.trafilatura_fetcher import TrafilaturaFetcher

HTML = "<html><head><title>T</title></head><body>" \
       "<article><h1>Hello</h1><p>This is the body content of an article. " \
       "It has enough text to be a real extraction target. " * 5 + \
       "</p></article></body></html>"

@pytest.mark.asyncio
async def test_extract_html():
    f = TrafilaturaFetcher(timeout=10)
    with respx.mock() as r:
        r.get("https://x/a").mock(return_value=httpx.Response(200, text=HTML))
        md = await f.extract("https://x/a")
    assert "Hello" in md
    assert "body content" in md
