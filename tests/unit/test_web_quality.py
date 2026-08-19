"""Red tests for normalized web quality and metadata sanitization."""

import pytest

from sluice.web.contracts import DocumentArtifact, QualityPolicy
from sluice.web.quality import evaluate_quality
from sluice.web.sanitize import sanitize_markdown, sanitize_metadata, sanitize_web_error


def _artifact(html: str, **kwargs):
    values = dict(
        source_url="https://example.test/article",
        final_url="https://example.test/article",
        status_code=200,
        content_type="text/html",
        raw_html=html,
        capabilities=frozenset({"raw_html"}),
        provider_instance="direct",
        provider_type="httpx",
    )
    values.update(kwargs)
    return DocumentArtifact(**values)


def test_quality_uses_visible_text_and_link_ratio():
    result = evaluate_quality(
        _artifact("<script>" + "x" * 1000 + "</script><article>Hello&nbsp;world " + "content " * 20 + "</article>"),
        QualityPolicy(min_chars=11),
    )
    assert result.quality.passed is True
    assert result.quality.quality_chars > 11


def test_challenge_and_empty_shell_fail_before_minimum_length():
    challenge = evaluate_quality(
        _artifact("<div class='cf-turnstile'></div>"), QualityPolicy(min_chars=1)
    )
    assert challenge.quality.reason == "challenge"
    shell = evaluate_quality(
        _artifact("<html><body><div id='app'></div><script src='app.js'></script></body></html>"),
        QualityPolicy(min_chars=1),
    )
    assert shell.quality.reason == "empty_shell"


def test_quality_rejects_bad_status_mime_and_short_text():
    status = evaluate_quality(
        _artifact("<article>good</article>", status_code=204), QualityPolicy(min_chars=1)
    )
    assert status.quality.reason == "http_status"
    mime = evaluate_quality(
        _artifact("<article>good</article>", content_type="application/pdf"),
        QualityPolicy(min_chars=1),
    )
    assert mime.quality.reason == "bad_content_type"
    short = evaluate_quality(_artifact("<article>" + "word " * 20 + "</article>"), QualityPolicy(min_chars=200))
    assert short.quality.reason == "too_short"


def test_challenge_phrase_and_link_ratio_are_detected():
    phrase = evaluate_quality(
        _artifact("<article>Checking   your browser</article>"), QualityPolicy(min_chars=1)
    )
    assert phrase.quality.reason == "challenge"
    links = evaluate_quality(
        _artifact("<article><a href='https://example.test'>" + "x" * 90 + "</a>" + "y" * 10 + "</article>"),
        QualityPolicy(min_chars=1, max_link_ratio=0.5),
    )
    assert links.quality.reason == "too_short"


def test_unsafe_markdown_links_images_and_raw_html_are_removed():
    sanitized = sanitize_markdown(
        "[x](javascript:alert(1)) ![alt](data:image/png;base64,AAAA) <script>x</script> <div>raw page body</div>",
        "https://example.test/",
    )
    assert "javascript:" not in sanitized
    assert "data:" not in sanitized
    assert "<script>" not in sanitized
    assert "raw page body" not in sanitized
    assert "x" in sanitized and "alt" in sanitized


def test_markdown_code_literals_are_not_treated_as_raw_html():
    sanitized = sanitize_markdown("```html\n<script>literal</script>\n```", "https://example.test/")
    assert "<script>literal</script>" in sanitized


def test_metadata_is_structurally_allowlisted_bounded_and_safe():
    result = sanitize_metadata(
        {
            "open_graph": {
                "title": "<b>Title</b>",
                "description": "<script>secret</script>Visible",
                "unknown": "discard me",
                "image": "javascript:alert(1)",
            },
            "links": [
                {"text": "good", "url": "https://example.test/a"},
                {"text": "duplicate", "url": "https://example.test/a#fragment"},
                {"text": "bad", "url": "javascript:alert(1)"},
            ],
            "json_ld": {"articleBody": "raw page body", "headline": "<i>Headline</i>"},
            "warnings": ["not-a-warning", "sanitizer_rejected_link"],
        },
        "https://example.test/",
    )
    assert result["open_graph"]["title"] == "Title"
    assert "secret" not in str(result)
    assert "unknown" not in result["open_graph"]
    assert len(result["links"]) == 1
    assert "articleBody" not in result["json_ld"]
    assert "sanitizer_rejected_link" in result["warnings"]
    assert "sanitizer_rejected_image" in result["warnings"]


def test_web_errors_are_stable_and_never_include_exception_text():
    error = RuntimeError("Authorization: bearer top-secret vendor body")
    safe = sanitize_web_error(error)
    assert safe == "provider_error"
    assert "top-secret" not in safe and "vendor" not in safe
