from sluice.core.env import resolve_env


def test_resolve_env_literal_passthrough():
    assert resolve_env("plain-string") == "plain-string"


def test_resolve_env_reads_environment(monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "secret")
    assert resolve_env("env:MY_TOKEN") == "secret"


def test_resolve_env_missing_returns_empty(monkeypatch):
    monkeypatch.delenv("ABSENT_TOKEN", raising=False)
    assert resolve_env("env:ABSENT_TOKEN") == ""
