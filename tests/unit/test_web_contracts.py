import pytest

from sluice.web.contracts import DocumentArtifact, FetchRequest, NO_MATCH


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
