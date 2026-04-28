import httpx
from fake_useragent import UserAgent

from sluice.fetchers._ssrf import guard
from sluice.fetchers.base import register_fetcher

_ua = UserAgent()


@register_fetcher("firecrawl")
class FirecrawlFetcher:
    name = "firecrawl"

    def __init__(self, *, base_url: str, api_key: str | None = None, timeout: float = 60.0):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    async def extract(self, url: str) -> str:
        guard(url)
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.post(
                f"{self.base_url}/v1/scrape",
                headers=headers,
                json={
                    "url": url,
                    "formats": ["markdown"],
                    "headers": {"User-Agent": _ua.chrome},
                },
            )
            r.raise_for_status()
            data = r.json()
        return data.get("data", {}).get("markdown", "") or ""
