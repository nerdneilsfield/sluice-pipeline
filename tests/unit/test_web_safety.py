"""Red tests for the web transport safety primitives.

These tests intentionally exercise the public, provider-independent boundary;
the implementation is added only after this collection is observed failing.
"""

from dataclasses import dataclass

import pytest

from sluice.errors import WebSafetyError
from sluice.web.contracts import FetchRequest, ProviderBinding, QualityPolicy
from sluice.web.http import (
    DirectResponse,
    bounded_manual_get,
    bounded_service_get,
    send_pinned_httpx_request,
)
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


@pytest.mark.parametrize(
    "host",
    [
        "0x7f000001",
        "0x7f.0.0.1",
        "127.0x0.0.1",
        "0177.0.0.1",
    ],
)
def test_noncanonical_mixed_base_ipv4_aliases_are_rejected_before_dns(host):
    with pytest.raises(WebSafetyError, match="bad_url"):
        normalize_target_url(f"http://{host}/metadata")


def test_url_normalization_is_idna_canonical_and_diagnostics_are_redacted():
    target = normalize_target_url(
        "HTTPS://Bücher.Example.:443/a%20b?q=one&token=secret#frag"
    )
    assert target.host == "xn--bcher-kva.example"
    assert target.port == 443
    assert str(target).startswith("https://xn--bcher-kva.example/a%20b?")
    diagnostic = redact_url(
        "https://user:password@example.test/a?x=1&token=secret#frag"
    )
    assert "password" not in diagnostic and "secret" not in diagnostic
    assert "x=" in diagnostic and "token=" in diagnostic


def test_mixed_dns_and_private_redirect_are_rejected(monkeypatch):
    monkeypatch.setattr(
        "sluice.web.url.resolve_host",
        lambda host: ["93.184.216.34", "10.0.0.1"],
    )
    with pytest.raises(WebSafetyError, match="ssrf_blocked"):
        guard_resolved_addresses("example.test", DEFAULT_TARGET_POLICY)


def test_allowlisted_target_still_rejects_mixed_public_private_dns(monkeypatch):
    monkeypatch.setattr(
        "sluice.web.url.resolve_host",
        lambda host: ["93.184.216.34", "10.0.0.1"],
    )
    policy = {"allow_hosts": ["example.test"]}
    with pytest.raises(WebSafetyError, match="ssrf_blocked"):
        guard_resolved_addresses("example.test", policy)


def test_non_public_literal_ip_requires_cidr_not_exact_host_allowlist(monkeypatch):
    policy = {"allow_hosts": ["127.0.0.1"]}

    with pytest.raises(WebSafetyError, match="ssrf_blocked"):
        normalize_target_url("http://127.0.0.1/article", policy)

    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["127.0.0.1"])
    with pytest.raises(WebSafetyError, match="ssrf_blocked"):
        guard_resolved_addresses("127.0.0.1", policy)


def test_non_public_literal_ip_matching_cidr_is_allowed_by_static_and_dns_guards(
    monkeypatch,
):
    policy = {"allow_cidrs": ["127.0.0.0/8"]}
    target = normalize_target_url("http://127.0.0.1/article", policy)
    assert target.host == "127.0.0.1"

    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["127.0.0.1"])
    assert guard_resolved_addresses("127.0.0.1", policy) == ("127.0.0.1",)


def test_exact_hostname_allowlist_still_applies_to_private_dns_answer(monkeypatch):
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["10.0.0.8"])
    assert guard_resolved_addresses(
        "internal.example", {"allow_hosts": ["internal.example"]}
    ) == ("10.0.0.8",)


def test_protected_target_headers_are_rejected():
    with pytest.raises(WebSafetyError, match="forbidden_header"):
        validate_target_headers({"Host": "internal.test"})
    with pytest.raises(WebSafetyError, match="forbidden_header"):
        validate_target_headers({"X-Forwarded-For": "127.0.0.1"})


@dataclass
class _FakeTransport:
    responses: list[DirectResponse]
    calls: int = 0
    requests: list[tuple[str, dict[str, str]]] | None = None

    def get(self, *args, **kwargs):
        if self.requests is not None:
            self.requests.append((str(args[0]), dict(kwargs.get("headers", {}))))
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


def test_cross_host_redirect_drops_target_authorization_and_cookie_before_send(
    monkeypatch,
):
    transport = _FakeTransport(
        [
            DirectResponse(
                status_code=302,
                headers={"location": "https://other.test/next"},
                peer="93.184.216.34",
                url="https://example.test/article",
            ),
            DirectResponse(
                status_code=200,
                headers={"content-type": "text/plain"},
                body=b"ok",
                peer="93.184.216.35",
                url="https://other.test/next",
            ),
        ],
        requests=[],
    )

    def resolve(host):
        return ["93.184.216.34"] if host == "example.test" else ["93.184.216.35"]

    monkeypatch.setattr("sluice.web.url.resolve_host", resolve)
    binding = ProviderBinding(
        "direct",
        "httpx",
        target_headers={"Authorization": "Bearer secret", "X-Trace": "keep"},
        cookies={"session": "cookie-secret"},
    )
    result = bounded_manual_get(_request(), binding, transport=transport)
    assert result.body == b"ok"
    assert transport.requests is not None
    assert transport.requests[0][1]["Authorization"] == "Bearer secret"
    assert "Cookie" in transport.requests[0][1]
    assert "Authorization" not in transport.requests[1][1]
    assert "Cookie" not in transport.requests[1][1]
    assert transport.requests[1][1]["X-Trace"] == "keep"


def test_cross_host_redirect_drops_sensitive_header_name_fragments_before_send(
    monkeypatch,
):
    transport = _FakeTransport(
        [
            DirectResponse(
                status_code=302,
                headers={"location": "https://other.test/next"},
                peer="93.184.216.34",
                url="https://example.test/article",
            ),
            DirectResponse(
                status_code=200,
                headers={"content-type": "text/plain"},
                body=b"ok",
                peer="93.184.216.35",
                url="https://other.test/next",
            ),
        ],
        requests=[],
    )

    def resolve(host):
        return ["93.184.216.34"] if host == "example.test" else ["93.184.216.35"]

    monkeypatch.setattr("sluice.web.url.resolve_host", resolve)
    sensitive = {
        "X-Authorization": "Bearer authorization-secret",
        "X-Proxy-Authorization": "Basic proxy-authorization-secret",
        "X-Cookie": "cookie-secret",
        "X-Set-Cookie": "set-cookie-secret",
    }
    binding = ProviderBinding(
        "direct",
        "httpx",
        target_headers={**sensitive, "X-Trace": "keep"},
    )
    result = bounded_manual_get(_request(), binding, transport=transport)

    assert result.body == b"ok"
    assert transport.requests is not None
    assert all(name in transport.requests[0][1] for name in sensitive)
    assert all(name not in transport.requests[1][1] for name in sensitive)
    assert transport.requests[1][1]["X-Trace"] == "keep"


def test_cross_host_redirect_drops_vendor_credential_headers(monkeypatch):
    transport = _FakeTransport(
        [
            DirectResponse(
                status_code=302,
                headers={"location": "https://other.test/next"},
                peer="93.184.216.34",
                url="https://example.test/article",
            ),
            DirectResponse(
                status_code=200,
                headers={"content-type": "text/plain"},
                body=b"ok",
                peer="93.184.216.34",
                url="https://other.test/next",
            ),
        ],
        requests=[],
    )
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    sensitive = {
        "Ocp-Apim-Subscription-Key": "k1",
        "X-Access-Key": "k2",
        "X-Auth": "k3",
    }
    binding = ProviderBinding(
        "direct", "httpx", target_headers={**sensitive, "X-Trace": "keep"}
    )
    bounded_manual_get(_request(), binding, transport=transport)

    assert transport.requests is not None
    assert all(name in transport.requests[0][1] for name in sensitive)
    assert all(name not in transport.requests[1][1] for name in sensitive)
    assert transport.requests[1][1]["X-Trace"] == "keep"


def test_cross_host_redirect_keeps_session_and_private_named_headers(monkeypatch):
    transport = _FakeTransport(
        [
            DirectResponse(
                status_code=302,
                headers={"location": "https://other.test/next"},
                peer="93.184.216.34",
                url="https://example.test/article",
            ),
            DirectResponse(
                status_code=200,
                headers={"content-type": "text/plain"},
                body=b"ok",
                peer="93.184.216.34",
                url="https://other.test/next",
            ),
        ],
        requests=[],
    )
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    plain = {"X-Session-Id": "abc", "X-Private-Mode": "true"}
    binding = ProviderBinding("direct", "httpx", target_headers=plain)
    bounded_manual_get(_request(), binding, transport=transport)

    assert transport.requests is not None
    assert all(transport.requests[1][1][name] == value for name, value in plain.items())


def test_cross_host_redirect_to_service_endpoint_is_rejected_before_send(monkeypatch):
    transport = _FakeTransport(
        [
            DirectResponse(
                status_code=302,
                headers={"location": "https://service.test/next"},
                peer="93.184.216.34",
                url="https://example.test/article",
            )
        ],
        requests=[],
    )
    monkeypatch.setattr(
        "sluice.web.url.resolve_host",
        lambda host: ["93.184.216.34" if host != "service.test" else "93.184.216.36"],
    )
    binding = ProviderBinding("direct", "httpx", base_url="https://service.test")
    with pytest.raises(WebSafetyError, match="service_redirect"):
        bounded_manual_get(_request(), binding, transport=transport)
    assert transport.calls == 1


def test_service_get_rejects_target_protected_headers_and_allows_private_http(
    monkeypatch,
):
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["127.0.0.1"])
    transport = _FakeTransport(
        [
            DirectResponse(
                status_code=200, headers={"content-type": "text/plain"}, body=b"ok"
            )
        ]
    )
    binding = ProviderBinding("local", "crawl4ai", base_url="http://localhost:8080")
    with pytest.raises(WebSafetyError, match="forbidden_header"):
        bounded_service_get(
            "http://localhost:8080/v1",
            binding,
            headers={"Host": "internal.test"},
            transport=transport,
        )
    assert (
        bounded_service_get(
            "http://localhost:8080/v1", binding, transport=transport
        ).body
        == b"ok"
    )


@pytest.mark.parametrize("host", ["127.0.0.1", "10.0.0.8"])
def test_service_get_allows_operator_controlled_private_literal_endpoint(host):
    transport = _FakeTransport(
        [
            DirectResponse(
                status_code=200,
                headers={"content-type": "text/plain"},
                body=b"ok",
                peer=host,
            )
        ],
        requests=[],
    )
    base_url = f"http://{host}:8080"
    binding = ProviderBinding("local", "crawl4ai", base_url=base_url)

    response = bounded_service_get(f"{base_url}/v1", binding, transport=transport)

    assert response.body == b"ok"
    assert transport.requests == [(f"{base_url}/v1", {})]


def test_public_service_requires_https_and_transport_errors_are_safe(monkeypatch):
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    binding = ProviderBinding("cloud", "firecrawl", base_url="http://api.example.test")
    with pytest.raises(WebSafetyError, match="service_https_required"):
        bounded_service_get(
            "http://api.example.test/v1", binding, transport=_FakeTransport([])
        )

    class RaisingTransport:
        def get(self, *args, **kwargs):
            raise RuntimeError("vendor response body and Authorization secret")

    secure = ProviderBinding("cloud", "firecrawl", base_url="https://api.example.test")
    with pytest.raises(WebSafetyError, match="provider_error") as captured:
        bounded_service_get(
            "https://api.example.test/v1", secure, transport=RaisingTransport()
        )
    assert "Authorization" not in str(captured.value)


def test_web_safety_errors_from_injected_transport_are_sanitized(monkeypatch):
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])

    class RaisingSafetyTransport:
        def get(self, *args, **kwargs):
            raise WebSafetyError("Authorization: bearer vendor-secret")

    binding = ProviderBinding("cloud", "firecrawl", base_url="https://api.example.test")
    with pytest.raises(WebSafetyError, match="provider_error") as captured:
        bounded_service_get(
            "https://api.example.test/v1", binding, transport=RaisingSafetyTransport()
        )
    assert str(captured.value) == "provider_error"
    assert "vendor-secret" not in str(captured.value)


def test_manual_sender_fallback_sanitizes_external_safety_errors(monkeypatch):
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    calls: list[int] = []

    def sender(*args):
        calls.append(len(args))
        if len(args) > 2:
            raise TypeError("legacy signature")
        raise WebSafetyError("vendor body Authorization secret")

    monkeypatch.setattr("sluice.web.http.send_pinned_httpx_request", sender)
    with pytest.raises(WebSafetyError, match="provider_error") as captured:
        bounded_manual_get(_request(), ProviderBinding("direct", "httpx"))
    assert calls == [4, 2]
    assert str(captured.value) == "provider_error"


def test_public_service_rejects_private_or_mixed_dns_peers(monkeypatch):
    monkeypatch.setattr(
        "sluice.web.url.resolve_host",
        lambda host: ["93.184.216.34", "10.0.0.1"],
    )
    binding = ProviderBinding(
        "cloud", "firecrawl", base_url="https://api.firecrawl.com"
    )
    transport = _FakeTransport(
        [DirectResponse(status_code=200, body=b"private", peer="10.0.0.1")]
    )
    with pytest.raises(WebSafetyError, match="ssrf_blocked"):
        bounded_service_get(
            "https://api.firecrawl.com/v1",
            binding,
            headers={"Authorization": "Bearer secret"},
            transport=transport,
        )
    assert transport.calls == 0


def test_direct_sender_validates_protected_headers(monkeypatch):
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    target = normalize_target_url("https://example.test/article")
    request = _request(url="https://example.test/article")
    request = FetchRequest(
        url=request.url,
        mode=request.mode,
        acceptable=request.acceptable,
        quality=request.quality,
        target_headers={"Host": "internal.test"},
    )
    with pytest.raises(WebSafetyError, match="forbidden_header"):
        send_pinned_httpx_request(
            target,
            ("93.184.216.34",),
            request,
            ProviderBinding("direct", "httpx"),
            transport=_FakeTransport([DirectResponse(status_code=200, body=b"ok")]),
        )
