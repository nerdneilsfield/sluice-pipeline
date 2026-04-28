import asyncio
import httpx, trafilatura
from sluice.fetchers.base import register_fetcher

@register_fetcher("trafilatura")
class TrafilaturaFetcher:
    name = "trafilatura"

    def __init__(self, *, timeout: float = 10.0):
        self.timeout = timeout

    async def extract(self, url: str) -> str:
        async with httpx.AsyncClient(timeout=self.timeout,
                                      follow_redirects=True) as c:
            r = await c.get(url)
            r.raise_for_status()
            html = r.text
        md = await asyncio.to_thread(
            trafilatura.extract, html,
            output_format="markdown",
            include_comments=False,
            include_tables=True,
        )
        return md or ""
