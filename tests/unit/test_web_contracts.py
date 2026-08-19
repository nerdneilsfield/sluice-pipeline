import pytest

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
