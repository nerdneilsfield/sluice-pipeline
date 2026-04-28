import asyncio

import httpx
import trafilatura

from sluice.fetchers._ssrf import guard, guard_response, guarded_redirect_url
from sluice.fetchers.base import register_fetcher


@register_fetcher("trafilatura")
class TrafilaturaFetcher:
    name = "trafilatura"

    def __init__(self, *, timeout: float = 10.0):
        self.timeout = timeout

    async def extract(self, url: str) -> str:
        guard(url)
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=False) as c:
            r = await c.get(url)
            guard_response(r)
            # Manually follow redirects with SSRF guard at each hop
            redirect_count = 0
            while r.is_redirect:
                redirect_count += 1
                if redirect_count > 10:
                    raise httpx.TooManyRedirects(f"Too many redirects for {url}")
                location = r.headers.get("location")
                if not location:
                    break
                next_url = guarded_redirect_url(str(r.url), location)
                r = await c.get(next_url)
                guard_response(r)
            r.raise_for_status()
            html = r.text
        md = await asyncio.to_thread(
            trafilatura.extract,
            html,
            output_format="markdown",
            include_comments=False,
            include_tables=True,
        )
        return md or ""
