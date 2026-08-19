"""Strict authoring and expansion tests for the v4 web configuration."""

from __future__ import annotations

import json

import pytest

from sluice.canonical import canonical_json
from sluice.config import load_definition
from sluice.errors import DefinitionError


def _write(tmp_path, text: str):
    path = tmp_path / "definition.yml"
    path.write_text(text, encoding="utf-8")
    return path


def _base_web(*, chain: tuple[str, ...] = ("http",), min_chars: int = 500, cache: int = 1) -> str:
    providers = "\n".join(
        f"        - {name}" for name in chain
    )
    provider_rows = "\n".join(
        f"    {name}:\n      type: startup_http\n      trusted: true"
        for name in dict.fromkeys(chain)
    )
    return f"""version: 1
web:
  providers:
{provider_rows}
  fetch_chains:
    robust:
      providers:
{providers}
  extractors:
    identity:
      type: identity
  extract_chains:
    article:
      extractors: [identity]
sources:
  page:
    use: sluice.sources.webpage
    with:
      url: https://example.test/article
      fetch_chain: robust
      extract_chain: article
      output: markdown
      web_cache_version: {cache}
stages:
  article:
    input: each
    tasks:
      - use: sluice.tasks.fetch_webpage
        with:
          fetch_chain: robust
          extract_chain: article
          min_chars: {min_chars}
      - use: sluice.tasks.to_markdown
pipelines: {{}}
"""


def test_implicit_webpage_and_fetch_config_is_rejected(tmp_path):
    path = _write(
        tmp_path,
        """version: 1
stages:
  article:
    input: each
    tasks:
      - use: sluice.tasks.fetch_webpage
        with: {url_field: url}
""",
    )
    with pytest.raises(DefinitionError, match="fetch_chain"):
        load_definition(path)


def test_expansion_materializes_defaults_without_resolving_secret(monkeypatch, tmp_path):
    monkeypatch.setenv("TEST_KEY", "actual-secret")
    path = _write(
        tmp_path,
        """version: 1
web:
  providers:
    hosted:
      type: firecrawl
      api_key: env:TEST_KEY
  fetch_chains:
    robust: {providers: [hosted]}
  extractors: {identity: {type: identity}}
  extract_chains: {article: {extractors: [identity]}}
stages:
  article:
    input: each
    tasks:
      - use: sluice.tasks.fetch_webpage
        with: {fetch_chain: robust, extract_chain: article}
      - use: sluice.tasks.to_markdown
""",
    )
    definition = load_definition(path)
    binding = definition.stages["article"].tasks[0].web_binding
    assert binding is not None
    projection = canonical_json(binding.semantic_projection())
    assert "env:TEST_KEY" in projection
    assert "actual-secret" not in projection
    assert binding.quality.min_chars == 500
    assert binding.providers[0].timeout is not None
    assert binding.providers[0].timeout.total_seconds() == 60


def test_missing_env_reference_is_retained_without_resolution(monkeypatch, tmp_path):
    monkeypatch.delenv("MISSING_KEY", raising=False)
    path = _write(
        tmp_path,
        """version: 1
web:
  providers:
    hosted: {type: firecrawl, api_key: env:MISSING_KEY}
  fetch_chains: {robust: {providers: [hosted]}}
  extractors: {identity: {type: identity}}
  extract_chains: {article: {extractors: [identity]}}
stages:
  article:
    input: each
    tasks:
      - {use: sluice.tasks.fetch_webpage, with: {fetch_chain: robust, extract_chain: article}}
      - {use: sluice.tasks.to_markdown}
""",
    )
    definition = load_definition(path)
    assert "env:MISSING_KEY" in canonical_json(
        definition.stages["article"].tasks[0].web_binding.semantic_projection()  # type: ignore[union-attr]
    )


def test_literal_proxy_query_and_fragment_are_rejected(tmp_path):
    path = _write(
        tmp_path,
        """version: 1
web:
  providers:
    proxied:
      type: startup_http
      trusted: true
      proxy: https://proxy.example/?token=query-secret#fragment-secret
""",
    )
    with pytest.raises(DefinitionError, match="proxy"):
        load_definition(path)


@pytest.mark.parametrize(
    ("provider_fields", "field"),
    [
        ("      type: firecrawl\n      api_key: env:lower_key", "api_key"),
        ("      type: httpx\n      proxy: env:lower_proxy", "proxy"),
    ],
)
def test_lowercase_env_references_are_rejected(tmp_path, provider_fields, field):
    path = _write(
        tmp_path,
        f"""version: 1
web:
  providers:
    invalid:
{provider_fields}
""",
    )
    with pytest.raises(DefinitionError, match=field):
        load_definition(path)


def test_provider_order_and_minimum_change_stage_key(tmp_path):
    left = load_definition(_write(tmp_path, _base_web(chain=("a", "b"), min_chars=100)))
    reordered = load_definition(_write(tmp_path, _base_web(chain=("b", "a"), min_chars=100)))
    threshold = load_definition(_write(tmp_path, _base_web(chain=("a", "b"), min_chars=101)))
    assert left.stages["article"].canonical_config_json != reordered.stages["article"].canonical_config_json
    assert left.stages["article"].canonical_config_json != threshold.stages["article"].canonical_config_json


def test_web_source_cache_version_participates_in_semantic_binding(tmp_path):
    first = load_definition(_write(tmp_path, _base_web(cache=1))).sources["page"]
    second = load_definition(_write(tmp_path, _base_web(cache=2))).sources["page"]
    assert first.web_binding is not None
    assert second.web_binding is not None
    assert first.with_config["web_cache_version"] == 1
    assert second.with_config["web_cache_version"] == 2
    assert first.web_binding.semantic_projection() != second.web_binding.semantic_projection()


def test_non_web_task_canonical_shape_contains_null_binding(tmp_path):
    definition = load_definition(
        _write(
            tmp_path,
            """version: 1
stages:
  pass:
    input: each
    tasks: [{use: custom.task, with: {value: 1}}]
""",
        )
    )
    snapshot = json.loads(definition.stages["pass"].canonical_config_json)
    assert snapshot["tasks"] == [{"use": "custom.task", "with": {"value": 1}, "web_binding": None}]
