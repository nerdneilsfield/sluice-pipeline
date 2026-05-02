from sluice.core.errors import AllFetchersFailed
from sluice.logging_setup import get_logger
from sluice.state.cache import UrlCacheStore

log = get_logger(__name__)


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
                log.bind(url=url, chars=len(hit.markdown)).debug("fetcher.cache_hit")
                return hit.markdown
        attempts: list[str] = []
        details: list[str] = []
        for f in self.fetchers:
            attempts.append(f.name)
            log.bind(url=url, fetcher=f.name).debug("fetcher.attempt_started")
            try:
                md = await f.extract(url)
            except Exception as e:
                details.append(f"{f.name}: {type(e).__name__}: {e}")
                log.bind(
                    url=url,
                    fetcher=f.name,
                    error_class=type(e).__name__,
                    error=str(e),
                ).debug("fetcher.attempt_failed")
                continue
            if md and len(md) >= self.min_chars:
                if self.cache:
                    await self.cache.set(url, f.name, md, self.ttl_seconds)
                log.bind(url=url, fetcher=f.name, chars=len(md)).debug("fetcher.attempt_succeeded")
                return md
            length = len(md) if md else 0
            details.append(f"{f.name}: too_short({length}<{self.min_chars})")
            log.bind(
                url=url,
                fetcher=f.name,
                chars=length,
                min_chars=self.min_chars,
            ).debug("fetcher.attempt_too_short")
        if self.on_all_failed == "continue_empty":
            log.bind(url=url, attempts=attempts, details=details).info("fetcher.chain_empty")
            return None
        log.bind(url=url, attempts=attempts, details=details).info("fetcher.chain_failed")
        raise AllFetchersFailed(url, attempts, details)


class RawFetcherChain:
    def __init__(self, fetchers: list):
        self.fetchers = fetchers

    async def fetch(self, url: str) -> str:
        attempts: list[str] = []
        details: list[str] = []
        for fetcher in self.fetchers:
            attempts.append(fetcher.name)
            log.bind(url=url, fetcher=fetcher.name).debug("raw_fetcher.attempt_started")
            try:
                text = await fetcher.extract_raw(url)
            except Exception as exc:
                details.append(f"{fetcher.name}: {type(exc).__name__}: {exc}")
                log.bind(
                    url=url,
                    fetcher=fetcher.name,
                    error_class=type(exc).__name__,
                    error=str(exc),
                ).debug("raw_fetcher.attempt_failed")
                continue
            if text:
                log.bind(url=url, fetcher=fetcher.name, chars=len(text)).debug(
                    "raw_fetcher.attempt_succeeded"
                )
                return text
            details.append(f"{fetcher.name}: empty")
            log.bind(url=url, fetcher=fetcher.name).debug("raw_fetcher.attempt_empty")
        log.bind(url=url, attempts=attempts, details=details).info("raw_fetcher.chain_failed")
        raise AllFetchersFailed(url, attempts, details)
