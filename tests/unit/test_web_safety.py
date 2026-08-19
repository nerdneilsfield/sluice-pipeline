"""Red tests for the web transport safety primitives.

These tests intentionally exercise the public, provider-independent boundary;
the implementation is added only after this collection is observed failing.
"""

from dataclasses import dataclass

import pytest

from sluice.errors import WebSafetyError
from sluice.web.contracts import FetchRequest, ProviderBinding, QualityPolicy
from sluice.web.http import DirectResponse, bounded_manual_get
from sluice.web.url import (
    DEFAULT_TARGET_POLICY,
    guard_resolved_addresses,
    normalize_target_url,
    redact_url,
    validate_target_headers,
)


@pytest.mark.parametrize(
    "url",
    ["file:///etc/passwd", "http://127.0.0.1/", "http://user:pass@example.test/"],
)
def test_unsafe_target_urls_are_rejected(url):
    with pytest.raises(WebSafetyError):
        normalize_target_url(url)


def test_url_normalization_is_idna_canonical_and_diagnostics_are_redacted():
    target = normalize_target_url("HTTPS://Bücher.Example.:443/a%20b?q=one&token=secret#frag")
    assert target.host == "xn--bcher-kva.example"
    assert target.port == 443
    assert str(target).startswith("https://xn--bcher-kva.example/a%20b?")
    diagnostic = redact_url("https://user:password@example.test/a?x=1&token=secret#frag")
    assert "password" not in diagnostic and "secret" not in diagnostic
    assert "x=" in diagnostic and "token=" in diagnostic


def test_mixed_dns_and_private_redirect_are_rejected(monkeypatch):
    monkeypatch.setattr(
        "sluice.web.url.resolve_host",
        lambda host: ["93.184.216.34", "10.0.0.1"],
    )
    with pytest.raises(WebSafetyError, match="ssrf_blocked"):
        guard_resolved_addresses("example.test", DEFAULT_TARGET_POLICY)


def test_protected_target_headers_are_rejected():
    with pytest.raises(WebSafetyError, match="forbidden_header"):
        validate_target_headers({"Host": "internal.test"})
    with pytest.raises(WebSafetyError, match="forbidden_header"):
        validate_target_headers({"X-Forwarded-For": "127.0.0.1"})


@dataclass
class _FakeTransport:
    responses: list[DirectResponse]
    calls: int = 0

    def get(self, *args, **kwargs):
        response = self.responses[min(self.calls, len(self.responses) - 1)]
        self.calls += 1
        return response


def _request(url="https://example.test/article"):
    return FetchRequest(
        url=url,
        mode="article",
        acceptable=frozenset({"text"}),
        quality=QualityPolicy(min_chars=1),
    )


def test_manual_get_pins_peer_and_bounds_redirects(monkeypatch):
    transport = _FakeTransport(
        [
            DirectResponse(
                status_code=302,
                headers={"location": "/next"},
                body=b"",
                peer="93.184.216.34",
                url="https://example.test/article",
            ),
            DirectResponse(
                status_code=200,
                headers={"content-type": "text/plain"},
                body=b"ok",
                peer="93.184.216.34",
                url="https://example.test/next",
            ),
        ]
    )
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    binding = ProviderBinding("direct", "httpx")
    result = bounded_manual_get(_request(), binding, transport=transport)
    assert result.peer == "93.184.216.34"
    assert result.final_url == "https://example.test/next"
    assert result.body == b"ok"


def test_manual_get_rejects_peer_rebinding(monkeypatch):
    transport = _FakeTransport(
        [
            DirectResponse(
                status_code=200,
                headers={"content-type": "text/plain"},
                body=b"ok",
                peer="10.0.0.1",
                url="https://example.test/article",
            )
        ]
    )
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    binding = ProviderBinding("direct", "httpx")
    with pytest.raises(WebSafetyError, match="peer_mismatch"):
        bounded_manual_get(_request(), binding, transport=transport)


def test_manual_get_rejects_oversized_decompressed_body(monkeypatch):
    transport = _FakeTransport(
        [
            DirectResponse(
                status_code=200,
                headers={"content-type": "text/plain"},
                body=b"0123456789",
                peer="93.184.216.34",
                url="https://example.test/article",
            )
        ]
    )
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    binding = ProviderBinding("direct", "httpx", max_body_bytes=5)
    with pytest.raises(WebSafetyError, match="body_too_large"):
        bounded_manual_get(_request(), binding, transport=transport)


def test_manual_get_limits_redirects(monkeypatch):
    transport = _FakeTransport(
        [
            DirectResponse(
                status_code=302,
                headers={"location": "/next"},
                body=b"",
                peer="93.184.216.34",
                url="https://example.test/article",
            )
        ]
    )
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    binding = ProviderBinding("direct", "httpx")
    with pytest.raises(WebSafetyError, match="too_many_redirects"):
        bounded_manual_get(_request(), binding, transport=transport)
