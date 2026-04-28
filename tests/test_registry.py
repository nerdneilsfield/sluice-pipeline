import pytest
from sluice.registry import (
    register_source,
    register_fetcher,
    register_processor,
    register_sink,
    get_source,
    get_fetcher,
    get_processor,
    get_sink,
    all_processors,
)
from sluice.core.errors import ConfigError


def test_register_and_lookup():
    @register_processor("widget")
    class W:
        pass

    assert get_processor("widget") is W


def test_unknown_raises():
    with pytest.raises(ConfigError, match="unknown processor"):
        get_processor("nonexistent")


def test_duplicate_raises():
    @register_fetcher("dup_one")
    class A:
        pass

    with pytest.raises(ConfigError, match="already registered"):

        @register_fetcher("dup_one")
        class B:
            pass


def test_separate_namespaces():
    @register_source("xyz")
    class S:
        pass

    @register_processor("xyz")
    class P:
        pass

    assert get_source("xyz") is S
    assert get_processor("xyz") is P
