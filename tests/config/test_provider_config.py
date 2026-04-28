import tomllib

import pytest

from sluice.config import ProvidersConfig

SAMPLE = """
[[providers]]
name = "glm"
type = "openai_compatible"

[[providers.base]]
url = "https://open.bigmodel.cn/api/paas/v4"
weight = 1
key = [
  { value = "env:GLM_API_KEY",   weight = 3 },
  { value = "env:GLM_API_KEY_2", weight = 1, quota_duration = 18000 },
]

[[providers.models]]
model_name = "glm-4-flash"
input_price_per_1k  = 0.0001
output_price_per_1k = 0.0001
"""


def test_parse_provider():
    data = tomllib.loads(SAMPLE)
    cfg = ProvidersConfig.model_validate(data)
    p = cfg.providers[0]
    assert p.name == "glm"
    assert p.type == "openai_compatible"
    assert p.base[0].url.startswith("https://")
    assert p.base[0].key[0].value == "env:GLM_API_KEY"
    assert p.base[0].key[1].quota_duration == 18000
    assert p.models[0].input_price_per_1k == 0.0001


def test_unknown_provider_type_rejected():
    bad = SAMPLE.replace("openai_compatible", "made_up")
    with pytest.raises(Exception):
        ProvidersConfig.model_validate(tomllib.loads(bad))
