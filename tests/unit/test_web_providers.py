"""Task 6 red tests for fixed provider adapters and runtime catalog."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from threading import Event, Thread
from types import SimpleNamespace

import pytest

from sluice.errors import DefinitionError, WebSafetyError
from sluice.web import extractors as extractor_module
from sluice.web import registry as registry_module
from sluice.web.contracts import (
    DocumentArtifact,
    ExtractorBinding,
    FetchRequest,
    NO_MATCH,
    ProviderBinding,
    QualityPolicy,
    WebBinding,
)
from sluice.web.http import DirectResponse
from sluice.web.providers import build_provider
from sluice.web.registry import WebRuntimeCatalog, resolve_provider_binding


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


def test_trafilatura_provider_missing_dependency_fails_startup_with_instance(
    monkeypatch,
) -> None:
    original_import = __import__("importlib").import_module

    def missing(name: str, *args: object, **kwargs: object):
        if name == "trafilatura":
            raise ImportError("optional trafilatura is unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("sluice.web.providers.importlib.import_module", missing)
    with pytest.raises(DefinitionError, match="article"):
        build_provider(
            ProviderBinding(
                name="article",
                type="trafilatura",
                capabilities=frozenset({"raw_html", "text", "markdown"}),
            ),
            _runtime(_FakeTransport("<article>fallback text</article>")),
        )


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


@pytest.mark.parametrize("provider_type", ["httpx", "trafilatura"])
def test_httpx_profiles_require_remote_dns_proxy_opt_in(
    monkeypatch, provider_type: str
) -> None:
    if provider_type == "trafilatura":
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
    binding = ProviderBinding(
        name=f"{provider_type}-direct",
        type=provider_type,
        capabilities=(
            frozenset({"text"})
            if provider_type == "httpx"
            else frozenset({"raw_html", "text", "markdown"})
        ),
        proxy="http://proxy.example:8080",
    )
    with pytest.raises(DefinitionError, match="allow_remote_dns_proxy"):
        WebRuntimeCatalog((binding,))


@pytest.mark.parametrize("provider_type", ["httpx", "trafilatura"])
def test_httpx_profiles_use_resolved_proxy_but_guard_every_redirect_locally(
    monkeypatch, caplog, provider_type: str
) -> None:
    secret = "credential-that-must-not-be-logged"
    resolved_proxy = f"http://proxy-user:{secret}@proxy.example:8080"
    monkeypatch.setenv("DIRECT_PROXY", resolved_proxy)
    if provider_type == "trafilatura":
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

    requests: list[object] = []
    created: list[object] = []

    class Stream:
        def get_extra_info(self, name: str):
            return ("10.0.0.8", 8080) if name == "server_addr" else None

    class Response:
        status_code = 302
        headers = {"location": "http://127.0.0.1/metadata"}

        def __init__(self, request: object) -> None:
            self.url = getattr(request, "url")
            self.extensions = {"network_stream": Stream()}

        def iter_bytes(self):
            yield b""

        def close(self) -> None:
            pass

    class Client:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            created.append(self)

        def send(self, request: object, *, stream: bool = False):
            assert stream is True
            requests.append(request)
            return Response(request)

        def close(self) -> None:
            pass

    monkeypatch.setattr("httpx.Client", Client)
    resolutions: list[str] = []

    def resolve(host: str) -> list[str]:
        resolutions.append(host)
        return ["93.184.216.34"]

    monkeypatch.setattr("sluice.web.url.resolve_host", resolve)
    binding = ProviderBinding(
        name=f"{provider_type}-remote-proxy",
        type=provider_type,
        capabilities=(
            frozenset({"text"})
            if provider_type == "httpx"
            else frozenset({"raw_html", "text", "markdown"})
        ),
        proxy="env:DIRECT_PROXY",
        target_policy={"allow_remote_dns_proxy": True},
    )

    with caplog.at_level(logging.WARNING, logger="sluice.web.registry"):
        catalog = WebRuntimeCatalog((binding,))
    try:
        with pytest.raises(WebSafetyError, match="ssrf_blocked"):
            catalog.provider(binding).fetch(
                _request(acceptable=frozenset({"text"}))
            )

        assert resolutions == ["example.test"]
        assert len(requests) == 1
        assert str(getattr(requests[0], "url")) == "https://example.test/article"
        assert "sluice.addresses" not in getattr(requests[0], "extensions")
        assert created[0].kwargs["proxy"] == resolved_proxy
        assert any(
            provider_type in warning
            and "pinning cannot be verified" in warning
            for warning in catalog.warnings
        )
        persisted = str(WebBinding(fetch_chain=(binding,)).semantic_projection())
        assert "env:DIRECT_PROXY" in persisted
        assert secret not in persisted
        assert secret not in str(catalog.warnings)
        assert secret not in caplog.text
    finally:
        catalog.close()


def test_httpx_socks5_proxy_keeps_approved_target_address_and_original_identity(
    monkeypatch,
) -> None:
    requests: list[object] = []
    created: list[object] = []

    class Stream:
        def get_extra_info(self, name: str):
            return ("10.0.0.8", 1080) if name == "server_addr" else None

    class Response:
        status_code = 200
        headers = {"content-type": "text/plain"}

        def __init__(self, request: object) -> None:
            self.url = getattr(request, "url")
            self.extensions = {"network_stream": Stream()}

        def iter_bytes(self):
            yield b"ok"

        def close(self) -> None:
            pass

    class Client:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            created.append(self)

        def send(self, request: object, *, stream: bool = False):
            assert stream is True
            requests.append(request)
            if len(requests) == 1:
                raise OSError("first approved target address is unavailable")
            return Response(request)

        def close(self) -> None:
            pass

    monkeypatch.setattr("httpx.Client", Client)
    monkeypatch.setattr(
        "sluice.web.url.resolve_host",
        lambda host: ["93.184.216.34", "93.184.216.35"],
    )
    binding = ProviderBinding(
        name="httpx-local-proxy",
        type="httpx",
        capabilities=frozenset({"text"}),
        proxy="socks5://proxy.example:1080",
    )

    catalog = WebRuntimeCatalog((binding,))
    try:
        artifact = catalog.provider(binding).fetch(
            _request(acceptable=frozenset({"text"}))
        )

        assert artifact.text == "ok"
        assert artifact.final_url == "https://example.test/article"
        assert created[0].kwargs["proxy"] == "socks5://proxy.example:1080"
        assert [str(getattr(sent, "url")) for sent in requests] == [
            "https://93.184.216.34/article",
            "https://93.184.216.35/article",
        ]
        sent = requests[-1]
        assert getattr(sent, "headers")["host"] == "example.test"
        assert getattr(sent, "extensions")["sni_hostname"] == "example.test"
        assert not catalog.warnings
    finally:
        catalog.close()


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


def test_catalog_closes_new_owned_transport_when_close_wins_creation_race(
    monkeypatch,
) -> None:
    constructing = Event()
    release = Event()
    created: list[object] = []

    class BlockingTransport:
        def __init__(self, **kwargs: object) -> None:
            self.close_calls = 0
            created.append(self)
            constructing.set()
            assert release.wait(2)

        def close(self) -> None:
            self.close_calls += 1

    monkeypatch.setattr(
        registry_module, "_PooledPinnedHTTPXClient", BlockingTransport
    )
    catalog = WebRuntimeCatalog()
    binding = ProviderBinding(name="direct", type="httpx")
    failures: list[BaseException] = []

    def create_transport() -> None:
        try:
            catalog.transport_for(binding)
        except BaseException as error:
            failures.append(error)

    worker = Thread(target=create_transport)
    worker.start()
    assert constructing.wait(2)
    catalog.close()
    release.set()
    worker.join(2)

    assert not worker.is_alive()
    assert len(failures) == 1
    assert isinstance(failures[0], DefinitionError)
    assert str(failures[0]) == "web runtime catalog is closed"
    assert len(created) == 1
    assert created[0].close_calls == 1


def test_catalog_close_logs_only_a_safe_fixed_code(monkeypatch) -> None:
    class RaisingTransport:
        def close(self) -> None:
            raise RuntimeError("Authorization bearer private-close-secret")

    class RecordingLogger:
        def __init__(self) -> None:
            self.debug_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

        def debug(self, *args: object, **kwargs: object) -> None:
            self.debug_calls.append((args, kwargs))

    monkeypatch.setattr(
        registry_module,
        "_PooledPinnedHTTPXClient",
        lambda **kwargs: RaisingTransport(),
    )
    logger = RecordingLogger()
    catalog = WebRuntimeCatalog(
        (ProviderBinding(name="direct", type="httpx"),),
        logger=logger,
    )

    catalog.close()

    assert logger.debug_calls == [(("web_runtime_close_failed",), {})]


def test_resolve_provider_binding_only_expands_secret_capable_schema_fields(
    monkeypatch,
) -> None:
    monkeypatch.setenv("API_SECRET", "resolved-api-secret")
    monkeypatch.setenv("HEADER_SECRET", "resolved-header-secret")
    monkeypatch.setenv("ACTION_LITERAL", "must-not-be-expanded")
    monkeypatch.delenv("ORDINARY_LITERAL", raising=False)
    action = {"write": {"text": "env:ACTION_LITERAL"}}
    binding = ProviderBinding(
        name="cloud",
        type="firecrawl",
        options={
            "api_key": "env:API_SECRET",
            "ordinary_text": "env:ORDINARY_LITERAL",
            "actions": (action,),
        },
        service_headers={"X-Token": "env:HEADER_SECRET"},
        wait_for="env:ORDINARY_LITERAL",
        actions=(action,),
    )

    resolved = resolve_provider_binding(binding)

    assert resolved.options["api_key"] == "resolved-api-secret"
    assert resolved.service_headers["X-Token"] == "resolved-header-secret"
    assert resolved.options["ordinary_text"] == "env:ORDINARY_LITERAL"
    assert resolved.options["actions"][0]["write"]["text"] == "env:ACTION_LITERAL"
    assert resolved.wait_for == "env:ORDINARY_LITERAL"
    assert resolved.actions[0]["write"]["text"] == "env:ACTION_LITERAL"


def test_catalog_built_extractor_options_are_deeply_immutable() -> None:
    authored_options = {
        "mode": "article",
        "nested": {"selectors": ["article", "main"]},
    }
    binding = ExtractorBinding(
        name="local-article",
        type="beautifulsoup",
        options=authored_options,
    )
    catalog = WebRuntimeCatalog()
    try:
        extractor = catalog.extractor(binding)
        with pytest.raises(TypeError):
            extractor.options["mode"] = "full"
        with pytest.raises(TypeError):
            extractor.options["nested"]["selectors"] = ("body",)
        assert extractor.options["nested"]["selectors"] == ("article", "main")
        assert authored_options == {
            "mode": "article",
            "nested": {"selectors": ["article", "main"]},
        }
    finally:
        catalog.close()


def test_catalog_curl_retains_per_worker_sessions_and_uses_pinned_callback_stream(
    monkeypatch,
) -> None:
    created: list[object] = []

    class CurlOpt:
        RESOLVE = 1
        FRESH_CONNECT = 2
        FORBID_REUSE = 3

    class Response:
        status_code = 200
        headers = {"content-type": "text/plain"}
        primary_ip = "93.184.216.34"

        def __init__(self, url: str) -> None:
            self.url = url

    class Session:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.curl_options = dict(kwargs.get("curl_options", {}))
            self.close_calls = 0
            self.get_calls = 0
            self.request_calls = 0
            created.append(self)

        def get(self, url: str, **kwargs: object) -> DirectResponse:
            self.get_calls += 1
            raise AssertionError("curl provider must not use eager Session.get")

        def request(self, method: str, url: str, **kwargs: object) -> Response:
            self.request_calls += 1
            assert method == "GET"
            assert kwargs["allow_redirects"] is False
            assert kwargs["accept_encoding"] == "gzip, deflate, br"
            assert kwargs.get("stream") is not True
            callback = kwargs["content_callback"]
            assert callable(callback)
            callback(b"ok")
            assert self.curl_options == {
                CurlOpt.RESOLVE: [
                    "example.test:443:93.184.216.34,[2606:2800:220:1:248:1893:25c8:1946]"
                ],
                CurlOpt.FRESH_CONNECT: 1,
                CurlOpt.FORBID_REUSE: 1,
            }
            return Response(url)

        def close(self) -> None:
            self.close_calls += 1

    requests_module = SimpleNamespace(Session=Session)
    curl_module = SimpleNamespace(CurlOpt=CurlOpt)
    original_import = __import__("importlib").import_module

    def fake_import(name: str, *args: object, **kwargs: object):
        if name == "curl_cffi.requests":
            return requests_module
        if name == "curl_cffi":
            return curl_module
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("importlib.import_module", fake_import)
    monkeypatch.setattr(
        "sluice.web.url.resolve_host",
        lambda host: [
            "93.184.216.34",
            "2606:2800:220:1:248:1893:25c8:1946",
        ],
    )
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
        assert created[0].request_calls == 2
        failures: list[BaseException] = []

        def fetch_twice() -> None:
            try:
                provider.fetch(_request(acceptable=frozenset({"text"})))
                provider.fetch(_request(acceptable=frozenset({"text"})))
            except BaseException as error:
                failures.append(error)

        worker = Thread(target=fetch_twice)
        worker.start()
        worker.join(2)
        assert not worker.is_alive()
        assert failures == []
        assert len(created) == 2
        assert created[1].request_calls == 2
        assert all(session.get_calls == 0 for session in created)
        assert all(session.kwargs["trust_env"] is False for session in created)
    finally:
        catalog.close()
    assert [session.close_calls for session in created] == [1, 1]


def test_catalog_curl_caps_decompressed_callback_body(monkeypatch) -> None:
    class CurlOpt:
        RESOLVE = 1
        FRESH_CONNECT = 2
        FORBID_REUSE = 3

    class Session:
        def __init__(self, **kwargs: object) -> None:
            self.curl_options = dict(kwargs.get("curl_options", {}))

        def request(self, method: str, url: str, **kwargs: object) -> object:
            callback = kwargs["content_callback"]
            callback(b"012")
            try:
                callback(b"345")
            except Exception:
                # curl_cffi may surface a callback abort as its own write
                # error instead of propagating the callback exception.
                raise RuntimeError("curl write callback aborted") from None
            raise AssertionError("body cap must abort curl before completion")

        def close(self) -> None:
            pass

    original_import = __import__("importlib").import_module

    def fake_import(name: str, *args: object, **kwargs: object):
        if name == "curl_cffi.requests":
            return SimpleNamespace(Session=Session)
        if name == "curl_cffi":
            return SimpleNamespace(CurlOpt=CurlOpt)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("importlib.import_module", fake_import)
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34"])
    binding = ProviderBinding(
        name="browser",
        type="curl_cffi",
        capabilities=frozenset({"text"}),
        max_body_bytes=5,
    )
    catalog = WebRuntimeCatalog((binding,))
    try:
        with pytest.raises(WebSafetyError, match="body_too_large"):
            catalog.provider(binding).fetch(
                _request(acceptable=frozenset({"text"}))
            )
    finally:
        catalog.close()


def test_catalog_rejects_curl_runtime_without_safe_pinned_callback_api(
    monkeypatch,
) -> None:
    class CurlOpt:
        RESOLVE = 1
        FRESH_CONNECT = 2
        FORBID_REUSE = 3

    class UnsafeSession:
        def __init__(self, **kwargs: object) -> None:
            pass

        def get(self, url: str, **kwargs: object) -> object:
            return object()

        def close(self) -> None:
            pass

    original_import = __import__("importlib").import_module

    def fake_import(name: str, *args: object, **kwargs: object):
        if name == "curl_cffi.requests":
            return SimpleNamespace(Session=UnsafeSession)
        if name == "curl_cffi":
            return SimpleNamespace(CurlOpt=CurlOpt)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("importlib.import_module", fake_import)
    binding = ProviderBinding(
        name="unsafe-browser",
        type="curl_cffi",
        capabilities=frozenset({"text"}),
    )
    with pytest.raises(DefinitionError, match="unsafe-browser"):
        WebRuntimeCatalog((binding,))


def test_curl_remote_dns_proxy_requires_opt_in_and_records_unpinned_warning(
    monkeypatch,
) -> None:
    class CurlOpt:
        RESOLVE = 1
        FRESH_CONNECT = 2
        FORBID_REUSE = 3

    requests: list[dict[str, object]] = []
    sessions: list[object] = []

    class Session:
        def __init__(self, **kwargs: object) -> None:
            self.curl_options = dict(kwargs.get("curl_options", {}))
            sessions.append(self)

        def request(self, method: str, url: str, **kwargs: object) -> object:
            requests.append(dict(kwargs))
            callback = kwargs["content_callback"]
            callback(b"ok")
            return SimpleNamespace(
                status_code=200,
                headers={"content-type": "text/plain"},
                url=url,
                primary_ip="10.0.0.8",
            )

        def close(self) -> None:
            pass

    original_import = __import__("importlib").import_module

    def fake_import(name: str, *args: object, **kwargs: object):
        if name == "curl_cffi.requests":
            return SimpleNamespace(Session=Session)
        if name == "curl_cffi":
            return SimpleNamespace(CurlOpt=CurlOpt)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("importlib.import_module", fake_import)
    resolutions: list[str] = []

    def resolve(host: str) -> list[str]:
        resolutions.append(host)
        return ["93.184.216.34"]

    monkeypatch.setattr("sluice.web.url.resolve_host", resolve)
    denied = ProviderBinding(
        name="remote-proxy-denied",
        type="curl_cffi",
        capabilities=frozenset({"text"}),
        proxy="http://proxy.example:8080",
    )
    with pytest.raises(DefinitionError, match="allow_remote_dns_proxy"):
        WebRuntimeCatalog((denied,))

    allowed = ProviderBinding(
        name="remote-proxy",
        type="curl_cffi",
        capabilities=frozenset({"text"}),
        proxy="http://proxy.example:8080",
        target_policy={"allow_remote_dns_proxy": True},
    )
    catalog = WebRuntimeCatalog((allowed,))
    try:
        artifact = catalog.provider(allowed).fetch(
            _request(acceptable=frozenset({"text"}))
        )
        assert artifact.text == "ok"
        assert resolutions == ["example.test"]
        assert requests[0]["proxy"] == "http://proxy.example:8080"
        assert requests[0]["allow_redirects"] is False
        assert CurlOpt.RESOLVE not in sessions[0].curl_options
        assert any(
            "remote-proxy" in warning and "pinning cannot be verified" in warning
            for warning in catalog.warnings
        )
    finally:
        catalog.close()


@pytest.mark.parametrize(
    ("extractor_type", "module_attribute", "instance_name"),
    [
        ("trafilatura", "trafilatura", "article-extractor"),
        ("newspaper4k", "newspaper", "news-extractor"),
        ("resiliparse", "resiliparse", "fast-extractor"),
    ],
)
def test_catalog_rejects_missing_configured_optional_extractor_by_instance(
    monkeypatch,
    extractor_type: str,
    module_attribute: str,
    instance_name: str,
) -> None:
    monkeypatch.setattr(extractor_module, module_attribute, None)
    catalog = WebRuntimeCatalog()
    try:
        with pytest.raises(DefinitionError, match=instance_name):
            catalog.extractor(
                ExtractorBinding(name=instance_name, type=extractor_type)
            )
    finally:
        catalog.close()


def _raw_html_artifact() -> DocumentArtifact:
    return DocumentArtifact(
        raw_html="<article>body</article>",
        source_url="https://example.test/article",
        final_url="https://example.test/article",
        content_type="text/html",
        capabilities=frozenset({"raw_html"}),
    )


def test_optional_extractors_return_no_match_after_validated_runtime_failure(
    monkeypatch,
) -> None:
    class BrokenTrafilatura:
        @staticmethod
        def extract(*args: object, **kwargs: object) -> object:
            raise RuntimeError("must remain private")

    class BrokenArticle:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.config = SimpleNamespace()

        def download(self, **kwargs: object) -> None:
            pass

        def parse(self) -> None:
            raise RuntimeError("must remain private")

    class BrokenResiliparse:
        extract = SimpleNamespace(
            html2text=SimpleNamespace(
                extract_plain_text=staticmethod(
                    lambda *args, **kwargs: (_ for _ in ()).throw(
                        RuntimeError("must remain private")
                    )
                )
            )
        )

    monkeypatch.setattr(extractor_module, "trafilatura", BrokenTrafilatura())
    monkeypatch.setattr(
        extractor_module,
        "newspaper",
        SimpleNamespace(article=SimpleNamespace(Article=BrokenArticle)),
    )
    monkeypatch.setattr(extractor_module, "resiliparse", BrokenResiliparse())
    catalog = WebRuntimeCatalog()
    try:
        for name, extractor_type in (
            ("article", "trafilatura"),
            ("news", "newspaper4k"),
            ("fast", "resiliparse"),
        ):
            extractor = catalog.extractor(
                ExtractorBinding(name=name, type=extractor_type)
            )
            assert extractor.extract(_raw_html_artifact()) is NO_MATCH
    finally:
        catalog.close()


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
