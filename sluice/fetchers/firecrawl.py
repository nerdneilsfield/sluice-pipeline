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
    ):
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self.api_key = api_key
        self.api_headers = dict(api_headers) if api_headers else {}
        self.timeout = timeout
        self._endpoint = _build_endpoint(self.base_url, self.api_version)

    def _build_headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        headers.update(self.api_headers)
        if self.api_key and not has_auth_header(headers):
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def extract(self, url: str) -> str:
        guard(url)
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.post(
                self._endpoint,
                headers=self._build_headers(),
                json={
                    "url": url,
                    "formats": ["markdown"],
                },
            )
            r.raise_for_status()
            data = r.json()
        return data.get("data", {}).get("markdown", "") or ""
