import random

import httpx

from sluice.config import ProvidersConfig
from sluice.core.errors import ConfigError
from sluice.llm.provider import Endpoint, ProviderRuntime, parse_model_spec


class ProviderPool:
    def __init__(self, cfg: ProvidersConfig, *, seed: int | None = None):
        self.runtimes = {p.name: ProviderRuntime(p) for p in cfg.providers}
        self._rng = random.Random(seed)
        self._client: httpx.AsyncClient | None = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient()
        return self._client

    async def aclose(self):
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def acquire(self, model_spec: str) -> Endpoint:
        prov, model = parse_model_spec(model_spec)
        if prov not in self.runtimes:
            raise ConfigError(f"unknown provider: {prov}")
        return self.runtimes[prov].pick_endpoint(model, rng=self._rng)

    def cool_down(self, ep: Endpoint):
        self.runtimes[ep.provider_name].cool_down_key(ep._base_ref, ep._key_ref)
