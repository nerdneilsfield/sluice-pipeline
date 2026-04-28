import pytest, httpx, respx
from sluice.fetchers.jina_reader import JinaReaderFetcher

@pytest.mark.asyncio
async def test_jina_prefix_url():
    f = JinaReaderFetcher(base_url="https://r.jina.ai", timeout=30)
    with respx.mock() as r:
        r.get("https://r.jina.ai/https://x/a").mock(
            return_value=httpx.Response(200, text="# md content"))
        md = await f.extract("https://x/a")
    assert "md content" in md
