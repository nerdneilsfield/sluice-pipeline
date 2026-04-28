import pytest
from sluice.core.errors import (
    SluiceError,
    ConfigError,
    FetchError,
    AllFetchersFailed,
    LLMError,
    RateLimitError,
    QuotaExhausted,
    AllProvidersExhausted,
    StageError,
    SinkError,
    BudgetExceeded,
)


def test_hierarchy():
    assert issubclass(ConfigError, SluiceError)
    assert issubclass(AllFetchersFailed, FetchError)
    assert issubclass(RateLimitError, LLMError)
    assert issubclass(QuotaExhausted, LLMError)
    assert issubclass(AllProvidersExhausted, LLMError)
    assert issubclass(BudgetExceeded, SluiceError)


def test_carries_context():
    e = AllFetchersFailed("https://x/y", attempts=["traf", "firecrawl"])
    assert e.url == "https://x/y"
    assert e.attempts == ["traf", "firecrawl"]
