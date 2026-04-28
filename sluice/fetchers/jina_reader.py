import httpx
from sluice.fetchers.base import register_fetcher
from sluice.fetchers._ssrf import guard


@register_fetcher("jina_reader")
class JinaReaderFetcher:
    name = "jina_reader"

    def __init__(self, *, base_url: str = "https://r.jina.ai",
                 api_key: str | None = None, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    async def extract(self, url: str) -> str:
        guard(url)
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        async with httpx.AsyncClient(
            timeout=self.timeout, follow_redirects=False
        ) as c:
            r = await c.get(f"{self.base_url}/{url}", headers=headers)
            # Manually follow redirects with SSRF guard at each hop
            redirect_count = 0
            while r.is_redirect:
                redirect_count += 1
                if redirect_count > 10:
                    raise httpx.TooManyRedirects(
                        f"Too many redirects for {url}"
                    )
                location = r.headers.get("location")
                if not location:
                    break
                guard(location)
                r = await c.get(location)
            r.raise_for_status()
            return r.text
