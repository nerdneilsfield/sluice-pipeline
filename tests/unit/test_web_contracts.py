import pytest
import math
from datetime import timedelta

from sluice.canonical import canonical_json

from sluice.web.contracts import (
    DocumentArtifact,
    FetchChainExhausted,
    ExtractorBinding,
    FetchRequest,
    NO_MATCH,
    ProviderBinding,
    QualityPolicy,
    WebAttemptFailure,
    WebBinding,
)


def test_artifact_is_frozen_and_no_match_is_not_none():
    artifact = DocumentArtifact(
        source_url="https://example.test/a",
        final_url="https://example.test/a",
        status_code=200,
        content_type="text/html",
        raw_html="<p>a</p>",
        capabilities=frozenset({"raw_html"}),
        provider_instance="direct",
        provider_type="httpx",
    )
    assert artifact.selected_format is None
    assert NO_MATCH is not None
    with pytest.raises((AttributeError, TypeError)):
        artifact.final_url = "https://example.test/b"


def test_request_enforces_mode_and_list_selector_shape():
    with pytest.raises(ValueError):
        FetchRequest(
            url="https://example.test",
            mode="article",
            acceptable=frozenset(),
        )
    with pytest.raises(ValueError):
        FetchRequest(
            url="https://example.test",
            mode="article",
            acceptable=frozenset({"text"}),
            list_selector="article",
        )


def test_article_request_rejects_raw_source_capability():
    with pytest.raises(ValueError, match="raw_source"):
        FetchRequest(
            url="https://example.test",
            mode="article",
            acceptable=frozenset({"raw_source"}),
        )


def test_request_preferred_capabilities_must_be_acceptable():
    with pytest.raises(ValueError, match="preferred"):
        FetchRequest(
            url="https://example.test",
            mode="article",
            acceptable=frozenset({"text"}),
            preferred=("markdown",),
        )


@pytest.mark.parametrize("deadline", [True, False, 0, -1, math.nan, math.inf])
def test_request_stage_deadline_must_be_finite_and_positive(deadline):
    with pytest.raises(ValueError, match="stage_deadline"):
        FetchRequest(
            url="https://example.test",
            mode="article",
            acceptable=frozenset({"text"}),
            stage_deadline=deadline,
        )


def test_raw_source_request_rejects_list_selector():
    with pytest.raises(ValueError, match="list_selector"):
        FetchRequest(
            url="https://example.test",
            mode="raw_source",
            acceptable=frozenset({"raw_source"}),
            list_selector="article",
        )


def test_provider_timeout_seconds_is_integer_and_matches_timeout():
    normalized = ProviderBinding(
        "p",
        "httpx",
        timeout=timedelta(seconds=30),
        timeout_seconds=30.0,
    )
    assert normalized.timeout_seconds == 30
    assert isinstance(normalized.timeout_seconds, int)

    with pytest.raises(ValueError, match="timeout_seconds"):
        ProviderBinding(
            "p",
            "httpx",
            timeout=timedelta(seconds=30),
            timeout_seconds=31,
        )
    with pytest.raises(ValueError, match="timeout_seconds"):
        ProviderBinding(
            "p",
            "httpx",
            timeout=timedelta(seconds=30),
            timeout_seconds=30.5,
        )


def test_provider_connection_timeout_cannot_exceed_timeout():
    with pytest.raises(ValueError, match="connect_timeout"):
        ProviderBinding(
            "p",
            "httpx",
            timeout=timedelta(seconds=30),
            connect_timeout=timedelta(seconds=31),
        )


def _attempt() -> WebAttemptFailure:
    return WebAttemptFailure(
        provider_instance="direct",
        provider_type="httpx",
        attempt_ordinal=1,
        reason="network",
    )


def test_chain_exhaustion_keeps_only_safe_attempts_without_exception_message():
    failure = _attempt()
    error = FetchChainExhausted((failure,))

    assert error.attempts == (failure,)
    assert error.args == ()


def test_web_binding_rejects_malformed_chain_members():
    with pytest.raises(TypeError):
        WebBinding(fetch_chain=({"name": "not-a-provider"},))
    with pytest.raises(TypeError):
        WebBinding(extract_chain=(ProviderBinding("p", "httpx"),))


@pytest.mark.parametrize(
    ("factory", "field"),
    [
        (lambda value: WebBinding(version=value), "version"),
        (lambda value: ProviderBinding("p", "httpx", concurrency=value), "concurrency"),
        (lambda value: WebAttemptFailure("p", "httpx", value, "network"), "attempt_ordinal"),
        (lambda value: WebAttemptFailure("p", "httpx", 1, "network", elapsed_ms=value), "elapsed_ms"),
    ],
)
def test_contract_numeric_fields_reject_float_values(factory, field):
    with pytest.raises((TypeError, ValueError), match=field):
        factory(1.5)


def test_attempt_identifiers_and_detail_codes_are_bounded_safe_strings():
    with pytest.raises(ValueError):
        WebAttemptFailure("provider\nraw exception", "httpx", 1, "network")
    with pytest.raises(ValueError):
        WebAttemptFailure("provider", "httpx", 1, "network", detail_code="raw exception")
    with pytest.raises(ValueError):
        WebAttemptFailure("provider", "httpx", 1, "network", detail_code="x" * 129)

    valid = WebAttemptFailure(
        "provider_1",
        "httpx-v2",
        1,
        "network",
        detail_code="connect_timeout",
    )
    assert valid.detail_code == "connect_timeout"


def test_binding_projection_is_secret_safe_and_keeps_order():
    binding = WebBinding(
        fetch_chain=(
            ProviderBinding(
                "first",
                "firecrawl",
                options={"api_key": "actual-secret", "timeout": 60},
            ),
            ProviderBinding("second", "httpx"),
        ),
        fetch_chain_name="ordered",
    )
    projection = canonical_json(binding.semantic_projection())
    assert "actual-secret" not in projection
    assert "<redacted>" in projection
    assert projection.index("first") < projection.index("second")
    assert canonical_json(binding.runtime_digest()) == projection


def test_binding_projection_redacts_nested_cookies_and_credentialed_proxy():
    binding = WebBinding(
        fetch_chain=(
            ProviderBinding(
                "p",
                "httpx",
                cookies={"sessionid": "actual-cookie"},
                proxy="http://user:actual-password@example.test:8080",
            ),
        )
    )
    projection = canonical_json(binding.semantic_projection())
    assert "actual-cookie" not in projection
    assert "actual-password" not in projection


def test_binding_projection_redacts_sensitive_header_name_fragments():
    secrets = {
        "X-Authorization": "authorization-secret",
        "X-Proxy-Authorization": "proxy-authorization-secret",
        "X-Cookie": "cookie-secret",
        "X-Set-Cookie": "set-cookie-secret",
    }
    binding = WebBinding(
        fetch_chain=(
            ProviderBinding("p", "httpx", target_headers=secrets),
        )
    )

    projection = canonical_json(binding.semantic_projection())

    for secret in secrets.values():
        assert secret not in projection


def test_binding_projection_redacts_proxy_in_typed_and_options_paths():
    binding = WebBinding(
        fetch_chain=(
            ProviderBinding(
                "p",
                "httpx",
                proxy="http://user:typed-password@example.test:8080",
                options={"proxy": "http://user:options-password@example.test:8080"},
            ),
        )
    )
    projection = canonical_json(binding.semantic_projection())
    assert "typed-password" not in projection
    assert "options-password" not in projection


def test_binding_projection_preserves_only_exact_env_references():
    binding = WebBinding(
        fetch_chain=(
            ProviderBinding(
                "p",
                "firecrawl",
                proxy="env:proxy-runtime-secret",
                options={
                    "proxy": "env:options-runtime-secret",
                    "api_key": "env:api-runtime-secret",
                    "zone": "env:VALID_ZONE",
                },
            ),
        )
    )
    projection = canonical_json(binding.semantic_projection())
    assert "proxy-runtime-secret" not in projection
    assert "options-runtime-secret" not in projection
    assert "api-runtime-secret" not in projection
    assert "env:VALID_ZONE" in projection


def test_binding_projection_redacts_proxy_query_and_fragment_secrets():
    binding = WebBinding(
        fetch_chain=(
            ProviderBinding(
                "p",
                "httpx",
                proxy="https://proxy.example/?token=query-secret#fragment-secret",
                options={
                    "proxy": "https://proxy.example/?token=options-secret#fragment-secret"
                },
            ),
        )
    )
    projection = canonical_json(binding.semantic_projection())
    assert "query-secret" not in projection
    assert "options-secret" not in projection
    assert "fragment-secret" not in projection
