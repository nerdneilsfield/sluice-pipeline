from sluice.core.errors import ConfigError
from sluice.llm.pool import ProviderPool
from sluice.llm.provider import parse_model_spec


def model_price(pool: ProviderPool, model_spec: str) -> tuple[float, float]:
    prov, model = parse_model_spec(model_spec)
    rt = pool.runtimes.get(prov)
    if rt is None:
        raise ConfigError(f"unknown provider in model spec: {model_spec}")
    m = rt._models.get(model)
    if m is None:
        raise ConfigError(f"unknown model in model spec: {model_spec}")
    return (m.input_price_per_1k, m.output_price_per_1k)
