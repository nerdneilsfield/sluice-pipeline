"""Literal credentials are rejected when the definition loads, before SQLite sees them."""

import pytest

from sluice.app import open_app
from sluice.config import load_definition
from sluice.errors import DefinitionError

SECRET = "ghp_literal-secret-value"


def write_one(tmp_path, source=None, deliver=None):
    """A definition with exactly one built-in under test."""
    use, with_ = source or (
        "sluice.sources.json_api",
        "url: 'https://a.example.test/x', id_field: id",
    )
    sink = deliver or "url: 'https://h.example.test/in'"
    path = tmp_path / "d.yml"
    path.write_text(f"""version: 1
sources:
  feed: {{use: {use}, with: {{{with_}}}}}
pipelines:
  p:
    nodes:
      hook:
        from: source.feed
        deliver: {{use: sluice.sinks.webhook, with: {{{sink}}}}}
""")
    return path


def assert_rejected_and_not_persisted(tmp_path, path):
    with pytest.raises(DefinitionError) as caught:
        load_definition(path)
    assert SECRET not in str(caught.value)
    with pytest.raises(DefinitionError):
        open_app(path, tmp_path / "state.db")
    assert all(
        SECRET.encode() not in f.read_bytes() for f in tmp_path.glob("state.db*")
    )


@pytest.mark.parametrize("token", [SECRET, "env:", "env:1BAD"])
def test_github_activity_literal_token_is_rejected(tmp_path, token):
    path = write_one(
        tmp_path, ("sluice.sources.github_activity", f"repo: o/r, token: '{token}'")
    )
    with pytest.raises(DefinitionError) as caught:
        load_definition(path)
    assert token not in str(caught.value) or token.startswith("env:")
    if token == SECRET:
        assert_rejected_and_not_persisted(tmp_path, path)


def test_github_activity_env_token_is_accepted(tmp_path):
    load_definition(
        write_one(
            tmp_path,
            ("sluice.sources.github_activity", "repo: o/r, token: env:GH_TOKEN"),
        )
    )


@pytest.mark.parametrize(
    "name", ["authorization", "Authorization", "X-Api-Key", "X-Auth-Token"]
)
def test_json_api_sensitive_literal_header_is_rejected(tmp_path, name):
    path = write_one(
        tmp_path,
        (
            "sluice.sources.json_api",
            f"url: 'https://a.example.test/x', id_field: id, headers: {{'{name}': {SECRET}}}",
        ),
    )
    assert_rejected_and_not_persisted(tmp_path, path)


@pytest.mark.parametrize(
    "name", ["authorization", "Authorization", "X-Api-Key", "X-Auth-Token"]
)
def test_webhook_sensitive_literal_header_is_rejected(tmp_path, name):
    path = write_one(
        tmp_path,
        deliver=f"url: 'https://h.example.test/in', headers: {{'{name}': {SECRET}}}",
    )
    assert_rejected_and_not_persisted(tmp_path, path)


def test_env_and_plain_headers_are_accepted(tmp_path):
    headers = "headers: {Authorization: env:TOK, X-Api-Key: env:KEY, X-Source: sluice}"
    load_definition(
        write_one(
            tmp_path,
            (
                "sluice.sources.json_api",
                f"url: 'https://a.example.test/x', id_field: id, {headers}",
            ),
            f"url: 'https://h.example.test/in', {headers}",
        )
    )
