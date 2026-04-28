from sluice.core.errors import AllFetchersFailed
from sluice.state.cache import UrlCacheStore


class FetcherChain:
    def __init__(
        self,
        fetchers: list,
        *,
        min_chars: int,
        on_all_failed: str,
        cache: UrlCacheStore | None,
        ttl_seconds: int,
    ):
        self.fetchers = fetchers
        self.min_chars = min_chars
        self.on_all_failed = on_all_failed
        self.cache = cache
        self.ttl_seconds = ttl_seconds

    async def fetch(self, url: str) -> str | None:
        if self.cache:
            hit = await self.cache.get(url)
            if hit:
                return hit.markdown
        attempts: list[str] = []
        details: list[str] = []
        for f in self.fetchers:
            attempts.append(f.name)
            try:
                md = await f.extract(url)
            except Exception as e:
                details.append(f"{f.name}: {type(e).__name__}: {e}")
                continue
            if md and len(md) >= self.min_chars:
                if self.cache:
                    await self.cache.set(url, f.name, md, self.ttl_seconds)
                return md
            length = len(md) if md else 0
            details.append(f"{f.name}: too_short({length}<{self.min_chars})")
        if self.on_all_failed == "continue_empty":
            return None
        raise AllFetchersFailed(url, attempts, details)
