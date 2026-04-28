import httpx
from sluice.fetchers.base import register_fetcher

@register_fetcher("jina_reader")
class JinaReaderFetcher:
    name = "jina_reader"

    def __init__(self, *, base_url: str = "https://r.jina.ai",
                 api_key: str | None = None, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    async def extract(self, url: str) -> str:
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.get(f"{self.base_url}/{url}", headers=headers)
            r.raise_for_status()
            return r.text
