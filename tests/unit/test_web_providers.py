"""Task 6 red tests for fixed provider adapters and runtime catalog."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from sluice.errors import DefinitionError, WebSafetyError
from sluice.web.contracts import FetchRequest, NO_MATCH, ProviderBinding, QualityPolicy
from sluice.web.http import DirectResponse
from sluice.web.providers import build_provider
from sluice.web.registry import WebRuntimeCatalog


@dataclass
class _FakeTransport:
    body: str

    def get(self, url: str, **kwargs: object) -> DirectResponse:
        return DirectResponse(
            status_code=200,
            headers={"content-type": "text/html"},
            body=self.body.encode("utf-8"),
            url=url,
            final_url=url,
            peer="93.184.216.34",
        )


def _runtime(transport: object) -> object:
    return type("Runtime", (), {"http_client": transport})()


def _request(*, acceptable: frozenset[str] = frozenset({"text", "raw_html", "markdown"})) -> FetchRequest:
    preferred = tuple(item for item in ("markdown", "raw_html", "text") if item in acceptable)
    return FetchRequest(
        url="https://example.test/article",
        mode="article",
        acceptable=acceptable,
        preferred=preferred,
        quality=QualityPolicy(min_chars=1),
    )


def test_trafilatura_provider_returns_raw_html_and_provider_original_markdown(monkeypatch) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    monkeypatch.setattr(
        "sluice.web.providers.importlib.import_module",
        lambda name: type(
            "TrafilaturaModule",
            (),
            {"extract": staticmethod(lambda *args, **kwargs: "body")},
        )()
        if name == "trafilatura"
        else __import__(name),
    )
    provider = build_provider(
        ProviderBinding(
            name="article",
            type="trafilatura",
            capabilities=frozenset({"raw_html", "text", "markdown"}),
        ),
        _runtime(_FakeTransport("<article>body</article>")),
    )
    artifact = provider.fetch(_request())
    assert artifact.raw_html == "<article>body</article>"
    assert artifact.markdown == "body"
    assert artifact.provider_instance == "article"


def test_trafilatura_missing_dependency_returns_no_match(monkeypatch) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    original_import = __import__("importlib").import_module

    def missing(name: str, *args: object, **kwargs: object):
        if name == "trafilatura":
            raise ImportError("optional trafilatura is unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("sluice.web.providers.importlib.import_module", missing)
    provider = build_provider(
        ProviderBinding(
            name="article",
            type="trafilatura",
            capabilities=frozenset({"raw_html", "text", "markdown"}),
        ),
        _runtime(_FakeTransport("<article>fallback text</article>")),
    )
    assert provider.fetch(_request()) is NO_MATCH


def test_trafilatura_extraction_failure_returns_no_match(monkeypatch) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])

    class BrokenTrafilatura:
        @staticmethod
        def extract(*args: object, **kwargs: object):
            raise RuntimeError("extractor failed")

    monkeypatch.setattr(
        "sluice.web.providers.importlib.import_module",
        lambda name: BrokenTrafilatura() if name == "trafilatura" else __import__(name),
    )
    provider = build_provider(
        ProviderBinding(
            name="article",
            type="trafilatura",
            capabilities=frozenset({"raw_html", "text", "markdown"}),
        ),
        _runtime(_FakeTransport("<article>fallback text</article>")),
    )
    assert provider.fetch(_request()) is NO_MATCH


def test_catalog_httpx_uses_pinned_stream_and_decompressed_cap(monkeypatch) -> None:
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])

    class Stream:
        def __init__(self) -> None:
            self.closed = False

        def get_extra_info(self, name: str):
            return ("93.184.216.34", 443) if name == "server_addr" else None

    class Response:
        status_code = 200
        headers = {"content-type": "text/plain"}
        url = "https://example.test/article"

        def __init__(self) -> None:
            self.extensions = {"network_stream": Stream()}
            self.closed = False

        def iter_bytes(self):
            yield b"012"
            yield b"345"

        def close(self) -> None:
            self.closed = True

    created: list[object] = []

    class Client:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.get_calls = 0
            self.send_calls = 0
            self.request_extensions: dict[str, object] | None = None
            created.append(self)

        def get(self, url: str, **kwargs: object):
            self.get_calls += 1
            raise AssertionError("catalog must not use buffering Client.get")

        def send(self, request: object, *, stream: bool = False):
            self.send_calls += 1
            self.request_extensions = dict(getattr(request, "extensions", {}))
            assert stream is True
            return Response()

        def close(self) -> None:
            pass

    monkeypatch.setattr("httpx.Client", Client)
    binding = ProviderBinding(
        name="direct",
        type="httpx",
        capabilities=frozenset({"text"}),
        max_body_bytes=5,
    )
    catalog = WebRuntimeCatalog((binding,))
    try:
        with pytest.raises(WebSafetyError, match="body_too_large"):
            catalog.provider(binding).fetch(_request(acceptable=frozenset({"text"})))
        client = created[0]
        assert client.get_calls == 0
        assert client.send_calls == 1
        assert client.request_extensions == {"sluice.addresses": ("93.184.216.34",)}
        assert "transport" in client.kwargs
    finally:
        catalog.close()


def test_direct_provider_rejects_configured_proxy_at_startup() -> None:
    binding = ProviderBinding(
        name="direct",
        type="httpx",
        capabilities=frozenset({"text"}),
        proxy="http://proxy.example:8080",
    )
    with pytest.raises(DefinitionError, match="proxy"):
        WebRuntimeCatalog((binding,))


def test_startup_http_requires_trust_and_has_no_peer_claim() -> None:
    with pytest.raises(DefinitionError, match="trusted"):
        build_provider(
            ProviderBinding(name="compat", type="startup_http"),
            _runtime(_FakeTransport("ok")),
        )
    provider = build_provider(
        ProviderBinding(
            name="compat",
            type="startup_http",
            options={"trusted": True},
            capabilities=frozenset({"raw_html", "text", "raw_source"}),
        ),
        _runtime(_FakeTransport("ok")),
    )
    artifact = provider.fetch(_request(acceptable=frozenset({"text"})))
    assert artifact.status_code == 200
    assert artifact.final_url == "https://example.test/article"
    assert artifact.metadata.get("peer") is None


def test_unknown_provider_type_is_rejected_by_fixed_map() -> None:
    with pytest.raises(DefinitionError, match="unsupported"):
        build_provider(ProviderBinding(name="bad", type="not_a_provider"), _runtime(None))


def test_catalog_creates_one_direct_httpx_client_and_closes_it(monkeypatch) -> None:
    created: list[object] = []

    class Client:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.close_calls = 0
            self.get_calls = 0
            self.send_calls = 0
            created.append(self)

        def get(self, url: str, **kwargs: object) -> DirectResponse:
            self.get_calls += 1
            raise AssertionError("catalog must use streamed send")

        class Response:
            status_code = 200
            headers = {"content-type": "text/plain"}
            url = "https://example.test/article"
            extensions: dict[str, object] = {}

            def iter_bytes(self):
                yield b"ok"

            def close(self) -> None:
                pass

        def send(self, request: object, *, stream: bool = False):
            self.send_calls += 1
            assert stream is True
            return self.Response()

        def close(self) -> None:
            self.close_calls += 1

    monkeypatch.setattr("httpx.Client", Client)
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    binding = ProviderBinding(
        name="direct",
        type="httpx",
        capabilities=frozenset({"text"}),
    )
    catalog = WebRuntimeCatalog((binding,))
    try:
        assert len(created) == 1
        provider = catalog.provider(binding)
        provider.fetch(_request(acceptable=frozenset({"text"})))
        provider.fetch(_request(acceptable=frozenset({"text"})))
        assert created[0].send_calls == 2
    finally:
        catalog.close()
    assert created[0].close_calls == 1


def test_catalog_creates_one_curl_session_and_closes_it(monkeypatch) -> None:
    created: list[object] = []

    class Session:
        def __init__(self, **kwargs: object) -> None:
            self.close_calls = 0
            self.get_calls = 0
            created.append(self)

        def get(self, url: str, **kwargs: object) -> DirectResponse:
            self.get_calls += 1
            return DirectResponse(
                status_code=200,
                headers={"content-type": "text/plain"},
                body=b"ok",
                url=url,
                final_url=url,
                peer="93.184.216.34",
            )

        def close(self) -> None:
            self.close_calls += 1

    Module = type("Module", (), {"Session": Session})

    monkeypatch.setattr("importlib.import_module", lambda name: Module())
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    binding = ProviderBinding(
        name="browser",
        type="curl_cffi",
        capabilities=frozenset({"text"}),
    )
    catalog = WebRuntimeCatalog((binding,))
    try:
        assert len(created) == 1
        provider = catalog.provider(binding)
        provider.fetch(_request(acceptable=frozenset({"text"})))
        provider.fetch(_request(acceptable=frozenset({"text"})))
        assert created[0].get_calls == 2
    finally:
        catalog.close()
    assert created[0].close_calls == 1


def test_invalid_reported_final_url_fails_instead_of_using_request_url(monkeypatch) -> None:
    monkeypatch.setattr(
        "sluice.web.providers.bounded_manual_get",
        lambda *args, **kwargs: DirectResponse(
            status_code=200,
            headers={"content-type": "text/plain"},
            body=b"ok",
            url="https://example.test/article",
            final_url="file:///etc/passwd",
        ),
    )
    provider = build_provider(
        ProviderBinding(name="direct", type="httpx", capabilities=frozenset({"text"})),
        _runtime(_FakeTransport("unused")),
    )
    with pytest.raises(WebSafetyError) as error:
        provider.fetch(_request(acceptable=frozenset({"text"})))
    assert getattr(error.value, "reason", None) in {"bad_url", "ssrf_blocked", "provider_error"}


def test_startup_http_does_not_hide_invalid_reported_final_url() -> None:
    class InvalidFinalURL:
        def get(self, url: str, **kwargs: object) -> DirectResponse:
            return DirectResponse(
                status_code=200,
                headers={"content-type": "text/plain"},
                body=b"ok",
                url=url,
                final_url="file:///etc/passwd",
            )

    provider = build_provider(
        ProviderBinding(
            name="compat",
            type="startup_http",
            options={"trusted": True},
            capabilities=frozenset({"text"}),
        ),
        _runtime(InvalidFinalURL()),
    )
    with pytest.raises(WebSafetyError):
        provider.fetch(_request(acceptable=frozenset({"text"})))
