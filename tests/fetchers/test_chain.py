import pytest

from sluice.core.errors import AllFetchersFailed
from sluice.fetchers.chain import FetcherChain
from sluice.state.cache import UrlCacheStore
from sluice.state.db import open_db


class FakeFetcher:
    def __init__(self, name, result=None, raise_exc=None):
        self.name = name
        self._result = result
        self._exc = raise_exc
        self.calls = 0

    async def extract(self, url):
        self.calls += 1
        if self._exc:
            raise self._exc
        return self._result


@pytest.mark.asyncio
async def test_first_succeeds(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        cache = UrlCacheStore(db)
        a = FakeFetcher("a", result="hello world this is long enough text " * 30)
        b = FakeFetcher("b", result="never used")
        chain = FetcherChain(
            [a, b], min_chars=100, on_all_failed="skip", cache=cache, ttl_seconds=3600
        )
        out = await chain.fetch("https://x/y")
    assert "hello" in out
    assert b.calls == 0


@pytest.mark.asyncio
async def test_falls_through_on_short(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        cache = UrlCacheStore(db)
        a = FakeFetcher("a", result="too short")
        b = FakeFetcher("b", result="x" * 1000)
        chain = FetcherChain(
            [a, b], min_chars=500, on_all_failed="skip", cache=cache, ttl_seconds=3600
        )
        out = await chain.fetch("https://x/y")
    assert len(out) >= 1000
    assert b.calls == 1


@pytest.mark.asyncio
async def test_falls_through_on_exception(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        cache = UrlCacheStore(db)
        a = FakeFetcher("a", raise_exc=RuntimeError("nope"))
        b = FakeFetcher("b", result="x" * 1000)
        chain = FetcherChain(
            [a, b], min_chars=500, on_all_failed="skip", cache=cache, ttl_seconds=3600
        )
        out = await chain.fetch("https://x/y")
    assert len(out) >= 1000


@pytest.mark.asyncio
async def test_all_failed_skip(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        cache = UrlCacheStore(db)
        a = FakeFetcher("a", result="x")
        b = FakeFetcher("b", result="x")
        chain = FetcherChain(
            [a, b], min_chars=500, on_all_failed="skip", cache=cache, ttl_seconds=3600
        )
        with pytest.raises(AllFetchersFailed):
            await chain.fetch("https://x/y")


@pytest.mark.asyncio
async def test_all_failed_continue_empty(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        cache = UrlCacheStore(db)
        a = FakeFetcher("a", result="x")
        chain = FetcherChain(
            [a], min_chars=500, on_all_failed="continue_empty", cache=cache, ttl_seconds=3600
        )
        out = await chain.fetch("https://x/y")
    assert out == ""


@pytest.mark.asyncio
async def test_cache_hit_skips_fetchers(tmp_path):
    async with open_db(tmp_path / "d.db") as db:
        cache = UrlCacheStore(db)
        await cache.set("https://x/y", "trafilatura", "cached md " * 100, ttl_seconds=3600)
        a = FakeFetcher("a", result="x")
        chain = FetcherChain(
            [a], min_chars=100, on_all_failed="skip", cache=cache, ttl_seconds=3600
        )
        out = await chain.fetch("https://x/y")
    assert "cached md" in out
    assert a.calls == 0


@pytest.mark.asyncio
async def test_cache_disabled(tmp_path):
    a = FakeFetcher("a", result="x" * 1000)
    chain = FetcherChain([a], min_chars=100, on_all_failed="skip", cache=None, ttl_seconds=0)
    out = await chain.fetch("https://x/y")
    assert len(out) == 1000 and a.calls == 1
