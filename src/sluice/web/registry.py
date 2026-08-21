"""Startup-owned provider/extractor catalog.

The catalog is the only place where configured web bindings become live
clients.  Its persisted-facing keys are semantic/runtime digests; resolved
credentials remain in process memory and are never part of those keys.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
import importlib
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
from .http import _PooledPinnedHTTPXClient
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


class _ThreadLocalCurlSession:
    """Catalog-owned curl sessions, isolated per worker thread."""

    def __init__(self, module: object, impersonate: object) -> None:
        self._module = module
        self._impersonate = impersonate
        self._lock = threading.RLock()
        self._sessions: dict[int, object] = {}
        self._closed = False
        # Construct the registering thread's session eagerly.  Other worker
        # threads get their own session on first use.
        self._session_for_current_thread()

    def _session_for_current_thread(self) -> object:
        thread_id = threading.get_ident()
        with self._lock:
            if self._closed:
                raise DefinitionError("curl session is closed")
            session = self._sessions.get(thread_id)
            if session is None:
                factory = getattr(self._module, "Session", None)
                if not callable(factory):
                    raise DefinitionError("curl_cffi dependency is unavailable")
                session = factory(impersonate=self._impersonate)
                self._sessions[thread_id] = session
            return session

    def get(self, *args: object, **kwargs: object) -> object:
        session = self._session_for_current_thread()
        getter = getattr(session, "get", None)
        if not callable(getter):
            raise DefinitionError("curl session has no GET operation")
        return getter(*args, **kwargs)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            sessions = tuple(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            close = getattr(session, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    _LOG.debug("curl session close failed", exc_info=True)


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
        self._transports: dict[str, object] = {}
        self._owned_transports: dict[int, object] = {}
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
            # Construct the direct transport once at registration time.  A
            # provider fetch must never fall back to constructing an unowned
            # HTTPX client or curl session for an individual Item.
            transport = self._transport_for_binding(binding, key)
            provider_runtime = SimpleNamespace(
                http_client=self._http_client,
                curl_session=transport if binding.type == "curl_cffi" else None,
                catalog=self,
                transport_for=lambda _binding, value=transport: value,
            )
            provider = build_provider(runtime_binding, provider_runtime)
            self._providers[key] = provider
            self._provider_bindings[key] = binding
            self._limiters[key] = ProviderLimiter(binding.concurrency, binding.cooldown.total_seconds())
            if binding.type == "startup_http":
                warning = f"trusted startup_http provider configured: {binding.name}"
                if warning not in self._warnings:
                    self._warnings.append(warning)
                    self._logger.warning(warning)
            return provider

    def _configured_transport(self, binding: ProviderBinding) -> object | None:
        """Return an explicitly supplied transport, without creating one."""

        runtime = self._runtime
        resolver = _runtime_value(runtime, "transport_for")
        if callable(resolver):
            try:
                found = resolver(binding)
            except TypeError:
                found = resolver(binding.name)
            if found is not None:
                return found
        transports = _runtime_value(runtime, "transports")
        if isinstance(transports, Mapping):
            found = transports.get(binding.name)
            if found is not None:
                return found
        # ``http_client`` is the explicitly trusted compatibility boundary
        # for startup_http only.  Direct providers get catalog-owned clients.
        if binding.type == "startup_http":
            return self._http_client
        for name in ("httpx_client", "curl_session", "transport"):
            found = _runtime_value(runtime, name)
            if found is not None:
                return found
        return None

    def _transport_for_binding(self, binding: ProviderBinding, key: str) -> object | None:
        with self._lock:
            if self._closed or self._closing:
                raise DefinitionError("web runtime catalog is closed")
            if key in self._transports:
                return self._transports[key]
        configured = self._configured_transport(binding)
        if configured is not None:
            transport = configured
            owned = False
        elif binding.type in {"httpx", "trafilatura"}:
            try:
                import httpx

                timeout = None
                if binding.timeout is not None:
                    timeout = httpx.Timeout(
                        binding.timeout.total_seconds(),
                        connect=(binding.connect_timeout or binding.timeout).total_seconds(),
                    )
                transport = _PooledPinnedHTTPXClient(timeout=timeout)
            except Exception as error:
                raise DefinitionError(
                    f"cannot create HTTPX client for provider {binding.name!r}"
                ) from error
            owned = True
        elif binding.type == "curl_cffi":
            try:
                module = importlib.import_module("curl_cffi.requests")
                transport = _ThreadLocalCurlSession(
                    module,
                    binding.options.get("impersonate", "chrome"),
                )
            except Exception:
                raise DefinitionError(
                    f"curl_cffi dependency is unavailable for provider {binding.name!r}"
                ) from None
            owned = True
        else:
            # startup_http has already been validated in the provider
            # constructor; retaining None here gives that constructor the
            # precise missing-boundary error.
            transport = None
            owned = False
        with self._lock:
            existing = self._transports.get(key)
            if existing is not None:
                if owned and existing is not transport:
                    close = getattr(transport, "close", None)
                    if callable(close):
                        close()
                return existing
            self._transports[key] = transport
            if owned and transport is not None:
                self._owned_transports[id(transport)] = transport
            return transport

    def transport_for(self, binding: ProviderBinding) -> object | None:
        """Resolve a provider's already-owned transport for direct callers."""

        if not isinstance(binding, ProviderBinding):
            raise DefinitionError("provider binding must be a ProviderBinding")
        key = _binding_runtime_key(binding)
        return self._transport_for_binding(binding, key)

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
        if binding.proxy is not None:
            raise DefinitionError(
                f"provider {binding.name!r} configures proxy, but direct provider "
                "proxy transport is unavailable"
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
            content_client = self._web_content_client
            owned_transports = tuple(self._owned_transports.values())
        if content_client is not None and id(content_client) not in seen_ids:
            clients.append(content_client)
            seen_ids.add(id(content_client))
        for transport in owned_transports:
            if id(transport) not in seen_ids:
                clients.append(transport)
                seen_ids.add(id(transport))
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
