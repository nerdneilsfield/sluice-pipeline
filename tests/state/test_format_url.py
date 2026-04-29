import pytest

from sluice.core.errors import ConfigError
from sluice.state.attachments import format_url


def test_format_url_empty_prefix_returns_relative():
    assert format_url("2026/04/ab/abc.jpg", "") == "2026/04/ab/abc.jpg"


def test_format_url_https_concatenates():
    assert format_url("a.jpg", "https://cdn/x") == "https://cdn/x/a.jpg"


def test_format_url_strips_trailing_slash():
    assert format_url("a.jpg", "https://cdn/x/") == "https://cdn/x/a.jpg"


def test_format_url_file_scheme():
    assert format_url("a.jpg", "file:///data") == "file:///data/a.jpg"


def test_format_url_invalid_scheme_raises():
    with pytest.raises(ConfigError):
        format_url("a.jpg", "ftp://x")
