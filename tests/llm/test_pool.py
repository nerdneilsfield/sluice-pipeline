import pytest
from sluice.config import ProvidersConfig, Provider, BaseEndpoint, KeyConfig, ModelEntry
from sluice.llm.pool import ProviderPool


def make_cfg():
    return ProvidersConfig(
        providers=[
            Provider(
                name="glm",
                type="openai_compatible",
                base=[
                    BaseEndpoint(
                        url="https://x", weight=1, key=[KeyConfig(value="env:K", weight=1)]
                    )
                ],
                models=[ModelEntry(model_name="m1")],
            ),
        ]
    )


def test_pool_resolves(monkeypatch):
    monkeypatch.setenv("K", "v")
    pool = ProviderPool(make_cfg())
    ep = pool.acquire("glm/m1")
    assert ep.base_url == "https://x" and ep.api_key == "v"
