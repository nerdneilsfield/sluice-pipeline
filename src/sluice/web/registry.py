"""Startup-owned provider/extractor catalog.

The catalog is the only place where configured web bindings become live
clients.  Its persisted-facing keys are semantic/runtime digests; resolved
credentials remain in process memory and are never part of those keys.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import dataclass, field
import importlib
import inspect
import ipaddress
import logging
import os
import re
import threading
import time
from types import MappingProxyType, SimpleNamespace
from typing import Any

from ..errors import DefinitionError, WebSafetyError
from .contracts import ExtractorBinding, FetchRequest, ProviderBinding, WebBinding
from .extractors import (
    EXTRACTOR_FORMATS,
    build_extractor,
    validate_extractor_runtime,
)
from .http import (
    DirectResponse,
    MAX_BODY_BYTES,
    _PooledPinnedHTTPXClient,
    _check_execution_probe,
)
from .providers import (
    CORE_PROVIDER_BUILDERS,
    WebProvider,
    _curl_remote_dns_proxy,
    _direct_remote_dns_proxy,
    build_provider,
)
from .url import NormalizedURL


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


_SECRET_OPTION_FIELDS = frozenset(
    {
        "api_key",
        "zone",
        "proxy",
        "service_headers",
        "target_headers",
        "target_cookies",
    }
)


def _resolve_provider_options(
    binding: ProviderBinding,
    *,
    allow_missing: bool,
) -> dict[str, Any]:
    """Resolve only option fields whose configuration schema permits secrets."""

    resolved: dict[str, Any] = {}
    for key, value in binding.options.items():
        if key in _SECRET_OPTION_FIELDS:
            value = _resolve_secret(
                value,
                field=f"provider {binding.name}.{key}",
                allow_missing=allow_missing,
            )
        resolved[str(key)] = value
    return resolved


def resolve_provider_binding(
    binding: ProviderBinding,
    *,
    allow_missing_secrets: bool = False,
) -> ProviderBinding:
    """Resolve a binding's exact env references into an ephemeral copy."""

    if not isinstance(binding, ProviderBinding):
        raise DefinitionError("provider binding must be a ProviderBinding")
    try:
        resolved_options = _resolve_provider_options(
            binding,
            allow_missing=allow_missing_secrets,
        )
        resolved_service_headers = _resolve_secret(
            binding.service_headers,
            field=f"provider {binding.name}.service_headers",
            allow_missing=allow_missing_secrets,
        )
        if "api_key" in resolved_options and any(
            str(name).casefold() == "authorization"
            for name in resolved_service_headers
        ):
            raise DefinitionError(
                f"provider {binding.name!r} cannot combine api_key with Authorization"
            )
        return ProviderBinding(
            name=binding.name,
            type=binding.type,
            options=resolved_options,
            base_url=binding.base_url,
            timeout=binding.timeout,
            connect_timeout=binding.connect_timeout,
            transport_retries=binding.transport_retries,
            max_body_bytes=binding.max_body_bytes,
            concurrency=binding.concurrency,
            cooldown=binding.cooldown,
            capabilities=binding.capabilities,
            service_headers=resolved_service_headers,
            target_headers=_resolve_secret(binding.target_headers, field=f"provider {binding.name}.target_headers", allow_missing=allow_missing_secrets),
            cookies=_resolve_secret(binding.cookies, field=f"provider {binding.name}.cookies", allow_missing=allow_missing_secrets),
            proxy=_resolve_secret(binding.proxy, field=f"provider {binding.name}.proxy", allow_missing=allow_missing_secrets),
            target_policy=binding.target_policy,
            timeout_seconds=binding.timeout_seconds,
            only_main_content=binding.only_main_content,
            wait_for_selector=binding.wait_for_selector,
            wait_for=binding.wait_for,
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
        acquired = self._semaphore.acquire(timeout=timeout)
        if not acquired:
            return False
        # A different in-flight request may enter cooldown while this caller
        # waits for the semaphore.  Do not spend the newly available permit on
        # a request that should now advance to another provider.
        with self._lock:
            cooling_down = time.monotonic() < self._cooldown_until
        if cooling_down:
            self._semaphore.release()
            return False
        return True

    def in_cooldown(self) -> bool:
        with self._lock:
            return time.monotonic() < self._cooldown_until

    def cooldown_remaining(self) -> float:
        with self._lock:
            return max(0.0, self._cooldown_until - time.monotonic())

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

    def __init__(
        self,
        module: object,
        curl_module: object,
        *,
        provider_name: str,
        impersonate: object,
        http_version: object,
    ) -> None:
        self._module = module
        self._provider_name = provider_name
        self._impersonate = impersonate
        self._http_version = (
            None if http_version == "auto" else f"v{http_version}"
        )
        curl_options = getattr(curl_module, "CurlOpt", None)
        self._resolve_option = getattr(curl_options, "RESOLVE", None)
        self._fresh_connect_option = getattr(curl_options, "FRESH_CONNECT", None)
        self._forbid_reuse_option = getattr(curl_options, "FORBID_REUSE", None)
        if any(
            option is None
            for option in (
                self._resolve_option,
                self._fresh_connect_option,
                self._forbid_reuse_option,
            )
        ):
            raise DefinitionError(
                f"curl_cffi runtime cannot enforce pinned streaming for provider "
                f"{provider_name!r}"
            )
        self._lock = threading.RLock()
        self._sessions: dict[int, object] = {}
        self._closed = False
        # Construct the registering thread's session eagerly.  Other worker
        # threads get their own session on first use.
        try:
            self._session_for_current_thread()
        except BaseException:
            self.close()
            raise

    def _invalid_runtime(self) -> DefinitionError:
        return DefinitionError(
            f"curl_cffi runtime cannot enforce pinned streaming for provider "
            f"{self._provider_name!r}"
        )

    def _new_session(self) -> object:
        factory = getattr(self._module, "Session", None)
        if not callable(factory):
            raise self._invalid_runtime()
        options: dict[object, object] = {}
        kwargs: dict[str, object] = {
            "impersonate": self._impersonate,
            "trust_env": False,
            "allow_redirects": False,
            "retry": 0,
            "curl_options": options,
        }
        if self._http_version is not None:
            kwargs["http_version"] = self._http_version
        try:
            session = factory(**kwargs)
        except Exception:
            raise self._invalid_runtime() from None
        requester = getattr(session, "request", None)
        configured_options = getattr(session, "curl_options", None)
        if not callable(requester) or not isinstance(
            configured_options, MutableMapping
        ):
            close = getattr(session, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
            raise self._invalid_runtime()
        try:
            parameters = inspect.signature(requester).parameters.values()
        except (TypeError, ValueError):
            raise self._invalid_runtime() from None
        accepts_kwargs = any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        )
        names = {parameter.name for parameter in parameters}
        required = {
            "allow_redirects",
            "accept_encoding",
            "content_callback",
            "proxy",
        }
        if not accepts_kwargs and not required.issubset(names):
            close = getattr(session, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
            raise self._invalid_runtime()
        return session

    def _session_for_current_thread(self) -> object:
        thread_id = threading.get_ident()
        with self._lock:
            if self._closed:
                raise DefinitionError("curl session is closed")
            session = self._sessions.get(thread_id)
            if session is None:
                session = self._new_session()
                self._sessions[thread_id] = session
            return session

    @staticmethod
    def _web_error(reason: str) -> WebSafetyError:
        error = WebSafetyError(reason)
        try:
            setattr(error, "reason", reason)
        except Exception:
            pass
        return error

    @staticmethod
    def _resolve_rule(
        target: NormalizedURL, addresses: tuple[str, ...]
    ) -> str | None:
        try:
            ipaddress.ip_address(target.host)
        except ValueError:
            formatted = [
                f"[{address}]" if ":" in address else address
                for address in addresses
            ]
            if not formatted:
                raise _ThreadLocalCurlSession._web_error("ssrf_blocked")
            return f"{target.host}:{target.port}:{','.join(formatted)}"
        return None

    def get_pinned(
        self,
        target: NormalizedURL,
        addresses: tuple[str, ...],
        request: FetchRequest,
        binding: ProviderBinding,
    ) -> DirectResponse:
        session = self._session_for_current_thread()
        requester = getattr(session, "request", None)
        options = getattr(session, "curl_options", None)
        if not callable(requester) or not isinstance(options, MutableMapping):
            raise self._web_error("provider_error")
        try:
            remote_proxy = _curl_remote_dns_proxy(binding)
        except DefinitionError:
            raise self._web_error("provider_error") from None
        options.clear()
        if not remote_proxy:
            rule = self._resolve_rule(target, addresses)
            if rule is not None:
                options[self._resolve_option] = [rule]
        # A retained Session still owns per-worker cookies and browser state,
        # but each guarded hop opens a fresh connection so a prior DNS answer
        # cannot be reused after a rebind.
        options[self._fresh_connect_option] = 1
        options[self._forbid_reuse_option] = 1

        limit = min(
            binding.max_body_bytes,
            request.quality.max_body_bytes,
            MAX_BODY_BYTES,
        )
        chunks: list[bytes] = []
        total = 0
        overflowed = False
        timed_out = False

        deadline = request.stage_deadline
        now = time.monotonic()
        if deadline is None and binding.timeout is not None:
            deadline = now + binding.timeout.total_seconds()
        remaining: float | None = None
        if deadline is not None:
            remaining = deadline - now
            if remaining <= 0:
                raise self._web_error("timeout")

        class BodyTooLarge(Exception):
            pass

        class BodyTimedOut(Exception):
            pass

        def check_deadline(*, callback: bool = False) -> None:
            nonlocal timed_out
            if deadline is not None and time.monotonic() >= deadline:
                timed_out = True
                if callback:
                    raise BodyTimedOut
                raise self._web_error("timeout")

        def receive(chunk: object) -> None:
            nonlocal overflowed, total
            check_deadline(callback=True)
            try:
                piece = bytes(chunk)
            except Exception:
                raise self._web_error("provider_error") from None
            next_total = total + len(piece)
            if next_total > limit:
                overflowed = True
                raise BodyTooLarge
            total = next_total
            chunks.append(piece)
            check_deadline(callback=True)

        kwargs: dict[str, object] = {
            "headers": dict(request.target_headers),
            "allow_redirects": False,
            "accept_encoding": "gzip, deflate, br",
            "content_callback": receive,
        }
        if remaining is not None:
            configured_connect = binding.connect_timeout or binding.timeout
            connect = (
                remaining
                if configured_connect is None
                else min(remaining, configured_connect.total_seconds())
            )
            # curl_cffi accepts ``(connect, read)``.  The callback's absolute
            # check additionally prevents a sequence of successful reads from
            # resetting the provider's total budget.
            kwargs["timeout"] = (connect, remaining)
        if binding.proxy is not None:
            kwargs["proxy"] = binding.proxy
        check_deadline()
        _check_execution_probe()
        try:
            response = requester("GET", str(target), **kwargs)
        except BodyTooLarge:
            raise self._web_error("body_too_large") from None
        except BodyTimedOut:
            raise self._web_error("timeout") from None
        except WebSafetyError:
            raise
        except BaseException:
            if overflowed:
                raise self._web_error("body_too_large") from None
            if timed_out or (
                deadline is not None and time.monotonic() >= deadline
            ):
                raise self._web_error("timeout") from None
            raise self._web_error("provider_error") from None
        check_deadline()
        _check_execution_probe()
        try:
            status_code = int(getattr(response, "status_code", 0) or 0)
            headers = dict(getattr(response, "headers", {}) or {})
            response_url = getattr(response, "url", None)
            peer = None
            if binding.proxy is None:
                primary_ip = getattr(response, "primary_ip", None)
                if primary_ip:
                    peer = str(primary_ip)
            return DirectResponse(
                status_code=status_code,
                headers=headers,
                body=b"".join(chunks),
                peer=peer,
                url=str(response_url) if response_url is not None else str(target),
                final_url=(
                    str(response_url) if response_url is not None else str(target)
                ),
            )
        except WebSafetyError:
            raise
        except BaseException:
            raise self._web_error("provider_error") from None

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
                    _LOG.debug("curl_session_close_failed")


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
            transport = self._transport_for_binding(
                binding, key, runtime_binding=runtime_binding
            )
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
            if (
                binding.type in {"httpx", "curl_cffi", "trafilatura"}
                and _direct_remote_dns_proxy(runtime_binding)
            ):
                warning = (
                    f"{binding.type} provider {binding.name} uses a remote-DNS proxy; "
                    "target DNS/peer pinning cannot be verified"
                )
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

    def _transport_for_binding(
        self,
        binding: ProviderBinding,
        key: str,
        *,
        runtime_binding: ProviderBinding | None = None,
    ) -> object | None:
        with self._lock:
            if self._closed or self._closing:
                raise DefinitionError("web runtime catalog is closed")
            if key in self._transports:
                return self._transports[key]
        configured = self._configured_transport(binding)
        executable_binding = runtime_binding or binding
        if configured is not None:
            transport = configured
            owned = False
        elif binding.type in {
            "httpx",
            "trafilatura",
            "firecrawl",
            "crawl4ai",
            "jina_reader",
            "zyte",
            "bright_data",
        }:
            try:
                import httpx

                timeout = None
                if executable_binding.timeout is not None:
                    timeout = httpx.Timeout(
                        executable_binding.timeout.total_seconds(),
                        connect=(
                            executable_binding.connect_timeout
                            or executable_binding.timeout
                        ).total_seconds(),
                    )
                direct_provider = binding.type in {"httpx", "trafilatura"}
                remote_dns_proxy = (
                    _direct_remote_dns_proxy(executable_binding)
                    if direct_provider
                    else False
                )
                transport = _PooledPinnedHTTPXClient(
                    timeout=timeout,
                    proxy=executable_binding.proxy if direct_provider else None,
                    remote_dns_proxy=remote_dns_proxy,
                )
            except DefinitionError:
                raise
            except Exception:
                raise DefinitionError(
                    f"cannot create HTTPX client for provider {binding.name!r}"
                ) from None
            owned = True
        elif binding.type == "curl_cffi":
            try:
                module = importlib.import_module("curl_cffi.requests")
                curl_module = importlib.import_module("curl_cffi")
                transport = _ThreadLocalCurlSession(
                    module,
                    curl_module,
                    provider_name=binding.name,
                    impersonate=binding.options.get("impersonate", "chrome"),
                    http_version=binding.options.get("http_version", "auto"),
                )
            except DefinitionError:
                raise
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
        discard: object | None = None
        reject_closed = False
        result = transport
        with self._lock:
            if self._closed or self._closing:
                reject_closed = True
                if owned:
                    discard = transport
            else:
                existing = self._transports.get(key)
                if existing is not None:
                    result = existing
                    if owned and existing is not transport:
                        discard = transport
                else:
                    self._transports[key] = transport
                    if owned and transport is not None:
                        self._owned_transports[id(transport)] = transport
        if discard is not None:
            close = getattr(discard, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    self._logger.debug("web_runtime_close_failed")
        if reject_closed:
            raise DefinitionError("web runtime catalog is closed")
        return result

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
        if binding.proxy is not None and binding.type not in {
            "httpx",
            "curl_cffi",
            "trafilatura",
        }:
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
        if binding.proxy is not None and not str(binding.proxy).startswith("env:"):
            _direct_remote_dns_proxy(binding)

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
        validate_extractor_runtime(binding)
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
                        self._logger.debug("web_runtime_close_failed")
        finally:
            with self._lock:
                self._closing = False
                self._closed = True
                self._close_condition.notify_all()


__all__ = ["ProviderLimiter", "WebRuntimeCatalog", "resolve_provider_binding"]
