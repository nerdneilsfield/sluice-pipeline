"""Task 6 red tests for fixed provider adapters and runtime catalog."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from sluice.errors import DefinitionError
from sluice.web.contracts import FetchRequest, ProviderBinding, QualityPolicy
from sluice.web.http import DirectResponse
from sluice.web.providers import build_provider


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
