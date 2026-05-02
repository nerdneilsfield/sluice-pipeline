import httpx

from sluice.core.errors import ConfigError
from sluice.fetchers._ssrf import guard
from sluice.fetchers._utils import has_auth_header
from sluice.fetchers.base import register_fetcher

_VALID_VERSIONS = ("v1", "v2")


def _build_endpoint(base_url: str, api_version: str) -> str:
    """Return the scrape endpoint URL, enforcing version consistency."""
    if api_version not in _VALID_VERSIONS:
        raise ConfigError(
            f"firecrawl api_version={api_version!r} is invalid; "
            f"must be one of {list(_VALID_VERSIONS)}"
        )
    stripped = base_url.rstrip("/")
    for version in _VALID_VERSIONS:
        if stripped.endswith(f"/{version}"):
            if version != api_version:
                raise ConfigError(
                    f"firecrawl base_url ends with /{version} "
                    f"but api_version={api_version!r} conflicts"
                )
            return f"{stripped}/scrape"
    return f"{stripped}/{api_version}/scrape"


@register_fetcher("firecrawl")
class FirecrawlFetcher:
    name = "firecrawl"

    def __init__(
        self,
        *,
        base_url: str,
        api_version: str = "v2",
        api_key: str | None = None,
        api_headers: dict | None = None,
        timeout: float = 60.0,
        wait_for_ms: int | None = None,
        wait_for_selector: str | None = None,
        wait_for: int | None = None,
        waitFor: int | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self.api_key = api_key
        self.api_headers = dict(api_headers) if api_headers else {}
        self.timeout = timeout
        self.wait_for_ms = wait_for_ms if wait_for_ms is not None else wait_for
        if self.wait_for_ms is None:
            self.wait_for_ms = waitFor
        self.wait_for_selector = wait_for_selector
        self._endpoint = _build_endpoint(self.base_url, self.api_version)

    def _build_headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        headers.update(self.api_headers)
        if self.api_key and not has_auth_header(headers):
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _build_payload(self, url: str, *, formats: list[str]) -> dict:
        payload = {
            "url": url,
            "formats": formats,
        }
        wait = {}
        if self.wait_for_ms is not None:
            wait["milliseconds"] = self.wait_for_ms
        if self.wait_for_selector:
            wait["selector"] = self.wait_for_selector
        if wait:
            payload["wait"] = wait
        return payload

    async def _scrape(self, payload: dict) -> dict:
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.post(
                self._endpoint,
                headers=self._build_headers(),
                json=payload,
            )
            r.raise_for_status()
            return r.json()

    async def extract(self, url: str) -> str:
        guard(url)
        data = await self._scrape(self._build_payload(url, formats=["markdown"]))
        return data.get("data", {}).get("markdown", "") or ""

    async def extract_raw(self, url: str) -> str:
        guard(url)
        data = await self._scrape(self._build_payload(url, formats=["html"]))
        body = data.get("data", {})
        return body.get("html", "") or body.get("rawHtml", "") or body.get("raw_html", "") or ""
