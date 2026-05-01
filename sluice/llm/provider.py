import time
from dataclasses import dataclass
from datetime import datetime
from typing import Tuple
from zoneinfo import ZoneInfo

from sluice.config import BaseEndpoint, KeyConfig, ModelEntry, Provider


def parse_model_spec(spec: str) -> Tuple[str, str]:
    if "/" not in spec:
        raise ValueError(f"model spec must be 'provider/model': {spec}")
    prov, model = spec.split("/", 1)
    return prov, model


@dataclass
class Endpoint:
    base_url: str
    api_key: str
    extra_headers: dict
    model_entry: ModelEntry
    provider_name: str
    _base_ref: BaseEndpoint
    _key_ref: KeyConfig


class ProviderRuntime:
    def __init__(self, provider: Provider):
        self.provider = provider
        self._models = {m.model_name: m for m in provider.models}
        self._cooldowns: dict[tuple[int, int], float] = {}
        self._build_weights()

    def _build_weights(self):

        self._base_weights = []
        total = 0
        for i, b in enumerate(self.provider.base):
            total += b.weight
            self._base_weights.append((i, b, total))
        self._base_total = total

        self._key_weights: dict[int, list[tuple[int, KeyConfig, int]]] = {}
        self._key_totals: dict[int, int] = {}
        for bi, b in enumerate(self.provider.base):
            ktotal = 0
            kw = []
            for ki, k in enumerate(b.key):
                ktotal += k.weight
                kw.append((ki, k, ktotal))
            self._key_weights[bi] = kw
            self._key_totals[bi] = ktotal

    def _resolve_key(self, key_cfg: KeyConfig) -> str:
        if key_cfg.value.startswith("env:"):
            import os

            v = os.environ.get(key_cfg.value[4:])
            if v is None:
                raise ValueError(f"env var {key_cfg.value[4:]} not set")
            return v
        return key_cfg.value

    @staticmethod
    def _parse_clock(s: str):
        hour, minute = s.split(":", 1)
        return int(hour), int(minute)

    def _base_is_active(self, base: BaseEndpoint, now_ts: float) -> bool:
        if not base.active_windows:
            return True
        tz = ZoneInfo(base.active_timezone or "UTC")
        now = datetime.fromtimestamp(now_ts, tz)
        current = (now.hour, now.minute)
        for window in base.active_windows:
            start_s, sep, end_s = window.partition("-")
            if not sep:
                raise ValueError(f"invalid active window {window!r}; expected HH:MM-HH:MM")
            start = self._parse_clock(start_s)
            end = self._parse_clock(end_s)
            if start <= end:
                if start <= current < end:
                    return True
            elif current >= start or current < end:
                return True
        return False

    def pick_endpoint(self, model: str, rng) -> Endpoint:
        model_entry = self._models.get(model)
        if model_entry is None:
            raise ValueError(f"unknown model {model!r} in provider {self.provider.name!r}")

        now = time.time()
        # Pick base by weight
        active_bases = [(bi, b) for bi, b, _ in self._base_weights if self._base_is_active(b, now)]
        if not active_bases:
            raise ValueError(f"no active base endpoints for provider {self.provider.name!r}")
        active_total = sum(b.weight for _, b in active_bases)
        r = rng.randint(1, active_total)
        for bi, b in active_bases:
            if r <= b.weight:
                break
            r -= b.weight
        else:
            bi, b = active_bases[0]

        # Pick key by weight, respecting cooldowns
        kw = self._key_weights[bi]
        # Filter out keys in cooldown
        available = []
        for ki, k, cw in kw:
            cooldown_until = self._cooldowns.get((bi, ki), 0)
            if now >= cooldown_until:
                available.append((ki, k, cw))
        if not available:
            # All keys cooling down — pick the one that expires soonest
            soonest = min((self._cooldowns.get((bi, ki), float("inf")), ki, k) for ki, k, _ in kw)
            _, ki, k = soonest
            api_key = self._resolve_key(k)
            return Endpoint(
                base_url=b.url,
                api_key=api_key,
                extra_headers={**self.provider.extra_headers, **b.extra_headers},
                model_entry=model_entry,
                provider_name=self.provider.name,
                _base_ref=b,
                _key_ref=k,
            )

        # Weighted random among available keys
        atotal = sum(k.weight for _, k, _ in available)
        r = rng.randint(1, max(1, atotal))
        for ki, k, _ in available:
            if r <= k.weight:
                break
            r -= k.weight
        else:
            ki, k = available[0][0], available[0][1]

        api_key = self._resolve_key(k)
        return Endpoint(
            base_url=b.url,
            api_key=api_key,
            extra_headers={**self.provider.extra_headers, **b.extra_headers},
            model_entry=model_entry,
            provider_name=self.provider.name,
            _base_ref=b,
            _key_ref=k,
        )

    def get_model_entry(self, model: str) -> ModelEntry | None:
        return self._models.get(model)

    def cool_down_key(self, base_ref: BaseEndpoint, key_ref: KeyConfig):
        now = time.time()
        for bi, b in enumerate(self.provider.base):
            if b is base_ref:
                for ki, k in enumerate(b.key):
                    if k is key_ref:
                        duration = k.quota_duration or 60
                        self._cooldowns[(bi, ki)] = now + duration
                        return
