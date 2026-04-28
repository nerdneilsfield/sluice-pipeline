import random

import pytest

from sluice.config import BaseEndpoint, KeyConfig, ModelEntry, Provider
from sluice.llm.provider import ProviderRuntime, parse_model_spec


def make_provider():
    return Provider(
        name="glm",
        type="openai_compatible",
        base=[
            BaseEndpoint(
                url="https://a",
                weight=3,
                key=[KeyConfig(value="env:K1", weight=2), KeyConfig(value="env:K2", weight=1)],
            ),
            BaseEndpoint(url="https://b", weight=1, key=[KeyConfig(value="env:K3", weight=1)]),
        ],
        models=[
            ModelEntry(
                model_name="glm-4-flash", input_price_per_1k=0.0001, output_price_per_1k=0.0001
            )
        ],
    )


def test_parse_model_spec():
    assert parse_model_spec("glm/glm-4-flash") == ("glm", "glm-4-flash")
    with pytest.raises(ValueError):
        parse_model_spec("noslash")


def test_weighted_distribution(monkeypatch):
    monkeypatch.setenv("K1", "v1")
    monkeypatch.setenv("K2", "v2")
    monkeypatch.setenv("K3", "v3")
    rt = ProviderRuntime(make_provider())
    rng = random.Random(0)
    counts = {"https://a": 0, "https://b": 0}
    for _ in range(4000):
        ep = rt.pick_endpoint("glm-4-flash", rng=rng)
        counts[ep.base_url] += 1
    ratio = counts["https://a"] / counts["https://b"]
    assert 2.5 <= ratio <= 3.5


def test_quota_cooldown(monkeypatch):
    monkeypatch.setenv("K1", "v1")
    monkeypatch.setenv("K2", "v2")
    monkeypatch.setenv("K3", "v3")
    p = make_provider()
    p.base[0].key[0].quota_duration = 60
    rt = ProviderRuntime(p)
    rt.cool_down_key(p.base[0], p.base[0].key[0])
    rng = random.Random(0)
    for _ in range(50):
        ep = rt.pick_endpoint("glm-4-flash", rng=rng)
        assert ep.api_key != "v1"  # K1 is cooling down


def test_active_windows_filter_bases(monkeypatch):
    monkeypatch.setenv("K1", "v1")
    monkeypatch.setenv("K2", "v2")
    monkeypatch.setenv("K3", "v3")
    monkeypatch.setattr("time.time", lambda: 1767225600)  # 2026-01-01 00:00:00 UTC
    p = make_provider()
    p.base[0].active_timezone = "UTC"
    p.base[0].active_windows = ["01:00-02:00"]
    p.base[1].active_timezone = "UTC"
    p.base[1].active_windows = ["23:00-01:00"]
    rt = ProviderRuntime(p)
    ep = rt.pick_endpoint("glm-4-flash", rng=random.Random(0))
    assert ep.base_url == "https://b"
