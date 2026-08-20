"""Startup-owned provider/extractor catalog.

The catalog is the only place where configured web bindings become live
clients.  Its persisted-facing keys are semantic/runtime digests; resolved
credentials remain in process memory and are never part of those keys.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
import logging
import os
import re
import threading
import time
from types import MappingProxyType, SimpleNamespace
from typing import Any

from ..errors import DefinitionError
from .contracts import ExtractorBinding, ProviderBinding, WebBinding
from .extractors import EXTRACTOR_FORMATS, build_extractor
from .providers import CORE_PROVIDER_BUILDERS, WebProvider, build_provider


_ENV_REFERENCE = re.compile(r"^env:[A-Z_][A-Z0-9_]*$")
_LOG = logging.getLogger("sluice.web.registry")


def _runtime_value(runtime: object, name: str, default: Any = None) -> Any:
    if runtime is None:
        return default
    if isinstance(runtime, Mapping):
        return runtime.get(name, default)
    return getattr(runtime, name, default)


def _canonical_json(value: object) -> str:
    from ..canonical import canonical_json

    return canonical_json(value)


def _digest(value: object) -> str:
    from ..canonical import digest

    return digest(value)


def _provider_identity(binding: ProviderBinding) -> str:
    # ProviderBinding intentionally keeps identity projection on the enclosing
    # WebBinding contract.  Reuse that canonical implementation for catalog
    # deduplication instead of hashing repr() or resolved values.
    projection = WebBinding(fetch_chain=(binding,), fetch_chain_name="catalog").semantic_projection()
    return _canonical_json(projection["fetch_chain"])


def _resolve_secret(value: Any, *, field: str, allow_missing: bool = False) -> Any:
    if isinstance(value, str):
        match = _ENV_REFERENCE.fullmatch(value)
        if match is None:
            return value
        name = value[4:]
        resolved = os.environ.get(name)
        if resolved is None:
            if allow_missing:
                return value
            raise DefinitionError(f"missing environment secret {name}")
        return resolved
    if isinstance(value, Mapping):
        return {str(key): _resolve_secret(item, field=f"{field}.{key}", allow_missing=allow_missing) for key, item in value.items()}
    if isinstance(value, tuple):
        return tuple(_resolve_secret(item, field=field, allow_missing=allow_missing) for item in value)
    if isinstance(value, list):
        return [_resolve_secret(item, field=field, allow_missing=allow_missing) for item in value]
    return value


def resolve_provider_binding(
    binding: ProviderBinding,
    *,
    allow_missing_secrets: bool = False,
) -> ProviderBinding:
    """Resolve a binding's exact env references into an ephemeral copy."""

    if not isinstance(binding, ProviderBinding):
        raise DefinitionError("provider binding must be a ProviderBinding")
    try:
        return ProviderBinding(
            name=binding.name,
            type=binding.type,
            options=_resolve_secret(binding.options, field=f"provider {binding.name}", allow_missing=allow_missing_secrets),
            base_url=binding.base_url,
            timeout=binding.timeout,
            connect_timeout=binding.connect_timeout,
            transport_retries=binding.transport_retries,
            max_body_bytes=binding.max_body_bytes,
            concurrency=binding.concurrency,
            cooldown=binding.cooldown,
            capabilities=binding.capabilities,
            service_headers=_resolve_secret(binding.service_headers, field=f"provider {binding.name}.service_headers", allow_missing=allow_missing_secrets),
            target_headers=_resolve_secret(binding.target_headers, field=f"provider {binding.name}.target_headers", allow_missing=allow_missing_secrets),
            cookies=_resolve_secret(binding.cookies, field=f"provider {binding.name}.cookies", allow_missing=allow_missing_secrets),
            proxy=_resolve_secret(binding.proxy, field=f"provider {binding.name}.proxy", allow_missing=allow_missing_secrets),
            target_policy=binding.target_policy,
            timeout_seconds=binding.timeout_seconds,
            only_main_content=binding.only_main_content,
            wait_for_selector=binding.wait_for_selector,
            wait_for=_resolve_secret(binding.wait_for, field=f"provider {binding.name}.wait_for", allow_missing=allow_missing_secrets),
            mobile=binding.mobile,
            actions=binding.actions,
        )
    except (TypeError, ValueError) as error:
        raise DefinitionError(f"invalid provider {binding.name!r}: {error}") from error


@dataclass(slots=True)
class ProviderLimiter:
    """Process-local semaphore and cooldown for one runtime digest."""

    concurrency: int
    cooldown_seconds: float
    _semaphore: threading.BoundedSemaphore = field(init=False, repr=False)
    _lock: threading.Lock = field(init=False, repr=False)
    _cooldown_until: float = field(init=False, repr=False, default=0.0)

    def __post_init__(self) -> None:
        self._semaphore = threading.BoundedSemaphore(self.concurrency)
        self._lock = threading.Lock()
        self._cooldown_until = 0.0

    def acquire(self, timeout: float | None = None) -> bool:
        with self._lock:
            if time.monotonic() < self._cooldown_until:
                return False
        return self._semaphore.acquire(timeout=timeout)

    def release(self) -> None:
        try:
            self._semaphore.release()
        except ValueError:
            pass

    def cooldown(self, seconds: float | None = None) -> None:
        value = self.cooldown_seconds if seconds is None else max(0.0, float(seconds))
        with self._lock:
            self._cooldown_until = max(self._cooldown_until, time.monotonic() + value)

    @property
    def cooldown_until(self) -> float:
        with self._lock:
            return self._cooldown_until


def _binding_runtime_key(binding: ProviderBinding) -> str:
    # semantic_projection intentionally keeps env reference names while
    # removing resolved values.  It includes operational limiter settings.
    return _digest(_provider_identity(binding))


class WebRuntimeCatalog:
    """Immutable-definition, mutable-process runtime catalog."""

    def __init__(
        self,
        bindings: Iterable[ProviderBinding] = (),
        runtime: object | None = None,
        *,
        http_client: object | None = None,
        web_content_client: object | None = None,
        resolve_secrets: bool = True,
        logger: logging.Logger | None = None,
    ) -> None:
        # Accept ``WebRuntimeCatalog(runtime)`` as a convenient startup form
        # while retaining the explicit ``bindings`` form used by open_app.
        if isinstance(bindings, ProviderBinding):
            bindings = (bindings,)
        elif isinstance(bindings, Mapping) and all(
            isinstance(value, ProviderBinding) for value in bindings.values()
        ):
            bindings = tuple(bindings.values())
        if runtime is None and not isinstance(bindings, (list, tuple, set, frozenset)):
            candidate = bindings
            binding_mapping = isinstance(candidate, Mapping) and all(
                isinstance(value, ProviderBinding) for value in candidate.values()
            )
            if binding_mapping:
                bindings = tuple(candidate.values())
            elif hasattr(candidate, "http_client") or (isinstance(candidate, Mapping) and not binding_mapping):
                runtime = candidate
                bindings = ()
        if runtime is not None and http_client is None:
            http_client = _runtime_value(runtime, "http_client")
        self._lock = threading.RLock()
        self._close_condition = threading.Condition(self._lock)
        self._closing = False
        self._closed = False
        self._http_client = http_client
        self._providers: dict[str, WebProvider] = {}
        self._provider_bindings: dict[str, ProviderBinding] = {}
        self._limiters: dict[str, ProviderLimiter] = {}
        self._extractors: dict[tuple[str, str], object] = {}
        self._warnings: list[str] = []
        self._logger = logger or _LOG
        self._resolve_secrets = bool(resolve_secrets)
        self._runtime = runtime if runtime is not None else SimpleNamespace(http_client=http_client, catalog=self)
        self._web_content_client = web_content_client
        try:
            for binding in bindings:
                self.register_provider(binding)
        except BaseException:
            # A partially assembled startup catalog still owns any clients
            # created before a later binding failed validation.
            self.close()
            raise

    @classmethod
    def from_bindings(
        cls,
        bindings: Iterable[ProviderBinding],
        runtime: object | None = None,
        *,
        http_client: object | None = None,
        web_content_client: object | None = None,
        resolve_secrets: bool = True,
    ) -> "WebRuntimeCatalog":
        return cls(bindings, runtime, http_client=http_client, web_content_client=web_content_client, resolve_secrets=resolve_secrets)

    @classmethod
    def from_definition(
        cls,
        definition: object,
        *,
        http_client: object | None = None,
        web_content_client: object | None = None,
    ) -> "WebRuntimeCatalog":
        bindings: list[ProviderBinding] = []
        extractor_bindings: list[ExtractorBinding] = []
        seen: set[str] = set()

        def add(binding: ProviderBinding | None) -> None:
            if binding is None:
                return
            key = _provider_identity(binding)
            if key not in seen:
                seen.add(key)
                bindings.append(binding)

        for source in getattr(definition, "sources", {}).values():
            web = getattr(source, "web_binding", None)
            if isinstance(web, WebBinding):
                add(web.direct_provider)
                for provider in web.providers:
                    add(provider)
                # Construct local extractor objects once per expanded binding
                # so unsupported types/dependencies fail at startup.
                for extractor in web.extractors:
                    extractor_bindings.append(extractor)
        for stage in getattr(definition, "stages", {}).values():
            for task in stage.tasks:
                web = getattr(task, "web_binding", None)
                if isinstance(web, WebBinding):
                    add(web.direct_provider)
                    for provider in web.providers:
                        add(provider)
                    for extractor in web.extractors:
                        extractor_bindings.append(extractor)
        catalog = cls(bindings, http_client=http_client, web_content_client=web_content_client)
        try:
            for extractor in extractor_bindings:
                catalog.extractor(extractor)
        except BaseException:
            catalog.close()
            raise
        return catalog

    def register_provider(self, binding: ProviderBinding, *, allow_missing_secrets: bool | None = None) -> WebProvider:
        if not isinstance(binding, ProviderBinding):
            raise DefinitionError("provider binding must be a ProviderBinding")
        with self._lock:
            if self._closed or self._closing:
                raise DefinitionError("web runtime catalog is closed")
        missing = self._resolve_secrets is False if allow_missing_secrets is None else allow_missing_secrets
        runtime_binding = resolve_provider_binding(binding, allow_missing_secrets=missing)
        key = _binding_runtime_key(binding)
        with self._lock:
            if self._closed or self._closing:
                raise DefinitionError("web runtime catalog is closed")
            existing = self._providers.get(key)
            if existing is not None:
                return existing
            provider = build_provider(runtime_binding, self._runtime)
            self._providers[key] = provider
            self._provider_bindings[key] = binding
            self._limiters[key] = ProviderLimiter(binding.concurrency, binding.cooldown.total_seconds())
            if binding.type == "startup_http":
                warning = f"trusted startup_http provider configured: {binding.name}"
                if warning not in self._warnings:
                    self._warnings.append(warning)
                    self._logger.warning(warning)
            return provider

    def validate_provider_binding(self, binding: ProviderBinding) -> None:
        """Validate a retained terminal binding without opening a client.

        Terminal history is read-only at startup.  Its provider type and
        immutable contract must still be checked against the fixed map, but a
        retired optional dependency or environment secret must not prevent the
        application from opening.  ``rebuild_stage`` calls
        :meth:`register_provider` for the strict, executable path.
        """

        if not isinstance(binding, ProviderBinding):
            raise DefinitionError("provider binding must be a ProviderBinding")
        if binding.type not in CORE_PROVIDER_BUILDERS:
            raise DefinitionError(
                f"unsupported web provider type {binding.type!r} for {binding.name!r}"
            )
        # Reconstruct the immutable binding so malformed nested values are
        # rejected while unresolved env references remain untouched.
        resolve_provider_binding(binding, allow_missing_secrets=True)
        if binding.type == "startup_http" and binding.options.get("trusted") is not True:
            raise DefinitionError(
                f"startup_http provider {binding.name!r} requires trusted: true"
            )

    validate_provider = validate_provider_binding

    def provider(self, binding: ProviderBinding) -> WebProvider:
        return self.register_provider(binding)

    provider_for = provider
    get_provider = provider

    def limiter(self, binding: ProviderBinding) -> ProviderLimiter:
        self.register_provider(binding)
        return self._limiters[_binding_runtime_key(binding)]

    limiter_for = limiter

    def extractor(self, binding: ExtractorBinding) -> object:
        if not isinstance(binding, ExtractorBinding):
            raise DefinitionError("extractor binding must be an ExtractorBinding")
        key = (binding.name, _canonical_json(binding.semantic_projection() if hasattr(binding, "semantic_projection") else {"name": binding.name, "type": binding.type, "options": binding.options}))
        with self._lock:
            if self._closed or self._closing:
                raise DefinitionError("web runtime catalog is closed")
            if key not in self._extractors:
                self._extractors[key] = build_extractor(binding)
            return self._extractors[key]

    extractor_for = extractor

    def validate_extractor_binding(self, binding: ExtractorBinding) -> None:
        if not isinstance(binding, ExtractorBinding):
            raise DefinitionError("extractor binding must be an ExtractorBinding")
        supported = {"identity", *EXTRACTOR_FORMATS}
        if binding.type not in supported:
            raise DefinitionError(f"unsupported web extractor {binding.type!r}")

    validate_extractor = validate_extractor_binding

    @property
    def providers(self) -> Mapping[str, WebProvider]:
        return MappingProxyType(self._providers)

    @property
    def limiters(self) -> Mapping[str, ProviderLimiter]:
        return MappingProxyType(self._limiters)

    @property
    def warnings(self) -> tuple[str, ...]:
        return tuple(self._warnings)

    @property
    def web_content_client(self) -> object:
        with self._lock:
            if self._closed or self._closing:
                raise DefinitionError("web runtime catalog is closed")
            if self._web_content_client is None:
                from .client import WebContentClient

                self._web_content_client = WebContentClient(self)
            return self._web_content_client

    @property
    def client(self) -> object:
        return self.web_content_client

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            if self._closing:
                while not self._closed:
                    self._close_condition.wait()
                return
            self._closing = True
            clients: list[object] = []
            seen_ids: set[int] = set()
            for provider in self._providers.values():
                marker = id(provider)
                if marker not in seen_ids:
                    seen_ids.add(marker)
                    clients.append(provider)
            content_client = self._web_content_client
            transport = self._http_client
            runtime = self._runtime
            runtime_values: list[object] = []
            for name in ("http_client", "httpx_client", "transport", "curl_session"):
                value = _runtime_value(runtime, name)
                if value is not None:
                    runtime_values.append(value)
            transports = _runtime_value(runtime, "transports")
            if isinstance(transports, Mapping):
                runtime_values.extend(value for value in transports.values() if value is not None)
        if content_client is not None and id(content_client) not in seen_ids:
            clients.append(content_client)
            seen_ids.add(id(content_client))
        if transport is not None and id(transport) not in seen_ids:
            clients.append(transport)
            seen_ids.add(id(transport))
        for runtime_value in runtime_values:
            if id(runtime_value) not in seen_ids:
                clients.append(runtime_value)
                seen_ids.add(id(runtime_value))
        try:
            for client in clients:
                close = getattr(client, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:
                        self._logger.debug("web runtime close failed", exc_info=True)
        finally:
            with self._lock:
                self._closing = False
                self._closed = True
                self._close_condition.notify_all()


__all__ = ["ProviderLimiter", "WebRuntimeCatalog", "resolve_provider_binding"]
