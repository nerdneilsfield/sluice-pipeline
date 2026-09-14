"""Bounded direct HTTP transport used by built-in web providers.

The public helper deliberately uses a small response value object.  Provider
adapters may inject a transport in tests or for the trusted compatibility
adapter, while production calls use an HTTPX client with redirects disabled.
"""

from __future__ import annotations

import ipaddress
import json
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpcore
import httpx

from ..errors import WebSafetyError
from .contracts import (
    STABLE_FAILURE_REASONS,
    FetchRequest,
    ProviderBinding,
    WebAttemptCancelled,
    WebStageDeadlineExpired,
    current_web_execution_probe,
)
from .url import (
    MAX_REDIRECTS,
    NormalizedURL,
    TargetPolicy,
    guard_resolved_addresses,
    is_non_public_address,
    join_target_url,
    normalize_target_url,
    validate_target_headers,
)

MAX_BODY_BYTES = 10_485_760
_SENSITIVE_HEADER_NAMES = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
    }
)
_SENSITIVE_HEADER_PARTS = (
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "api-key",
    "apikey",
    "token",
    "secret",
    "signature",
)
_INTERNAL_SAFETY_REASONS = frozenset(STABLE_FAILURE_REASONS) | frozenset(
    {
        "peer_mismatch",
        "forbidden_header",
        "service_redirect",
        "too_many_redirects",
        "service_https_required",
    }
)


class _PinnedSyncBackend(httpcore.SyncBackend):
    """HTTPCore backend that dials only the already-approved address set."""

    def __init__(self, addresses: tuple[str, ...]):
        super().__init__()
        self._addresses = addresses

    def connect_tcp(self, host: str, port: int, **kwargs: Any):
        last_error: BaseException | None = None
        for address in self._addresses:
            try:
                return super().connect_tcp(address, port, **kwargs)
            except (OSError, httpcore.ConnectError, httpcore.ConnectTimeout) as error:
                last_error = error
        if last_error is not None:
            raise last_error
        raise OSError("no approved peer address")


class _PinnedResponseStream(httpx.SyncByteStream):
    def __init__(self, stream: Any):
        self._stream = stream

    def __iter__(self):
        return iter(self._stream)

    def close(self) -> None:
        self._stream.close()

    def get_extra_info(self, info: str) -> Any:
        getter = getattr(self._stream, "get_extra_info", None)
        return getter(info) if callable(getter) else None


class _PinnedHTTPTransport(httpx.BaseTransport):
    """Small HTTPX adapter retaining the original host for Host/TLS/SNI."""

    def __init__(self, addresses: tuple[str, ...] | None = None):
        self._lock = threading.RLock()
        self._closed = False
        self._addresses = tuple(addresses or ())
        self._pools: dict[tuple[str, ...], httpcore.ConnectionPool] = {}
        # Keep the historical attribute for fixed-address callers while the
        # catalog-owned transport lazily creates one pooled connection pool
        # per approved DNS answer set.
        self._pool: httpcore.ConnectionPool | None = None
        if self._addresses:
            self._pool = self._pool_for(self._addresses)

    def _pool_for(self, addresses: tuple[str, ...]) -> httpcore.ConnectionPool:
        addresses = tuple(addresses)
        if not addresses:
            raise OSError("no approved peer address")
        with self._lock:
            if self._closed:
                raise OSError("pinned transport is closed")
            existing = self._pools.get(addresses)
            if existing is not None:
                return existing
            pool = httpcore.ConnectionPool(
                network_backend=_PinnedSyncBackend(addresses)
            )
            self._pools[addresses] = pool
            if self._pool is None:
                self._pool = pool
            return pool

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        extension_addresses = request.extensions.get("sluice.addresses")
        if extension_addresses is None:
            addresses = self._addresses
        else:
            try:
                addresses = tuple(str(address) for address in extension_addresses)
            except (TypeError, ValueError):
                raise OSError("invalid approved peer addresses") from None
        pool = self._pool_for(addresses)
        core_request = httpcore.Request(
            method=request.method,
            url=httpcore.URL(
                scheme=request.url.raw_scheme,
                host=request.url.raw_host,
                port=request.url.port,
                target=request.url.raw_path,
            ),
            headers=request.headers.raw,
            content=request.stream,
            extensions=request.extensions,
        )
        response = pool.handle_request(core_request)
        return httpx.Response(
            status_code=response.status,
            headers=response.headers,
            stream=_PinnedResponseStream(response.stream),
            extensions=response.extensions,
            request=request,
        )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            pools = tuple(self._pools.values())
            self._pools.clear()
            self._pool = None
        for pool in pools:
            pool.close()


class _PooledPinnedHTTPXClient:
    """Reusable HTTPX client that streams through the pinned transport.

    DNS answers are supplied by :func:`bounded_manual_get` for every redirect
    hop.  The transport keeps a connection pool per approved answer set while
    preserving the target hostname for HTTP Host/TLS/SNI.
    """

    def __init__(
        self,
        *,
        timeout: Any = None,
        proxy: str | None = None,
        remote_dns_proxy: bool = False,
    ):
        self._proxy = proxy is not None
        self._remote_dns_proxy = bool(remote_dns_proxy)
        client_options: dict[str, Any] = {
            "follow_redirects": False,
            "timeout": timeout,
            "trust_env": False,
        }
        if proxy is None:
            self._transport: _PinnedHTTPTransport | None = _PinnedHTTPTransport()
            client_options["transport"] = self._transport
        else:
            self._transport = None
            client_options["proxy"] = proxy
        self._client = httpx.Client(**client_options)
        self._lock = threading.RLock()
        self._closed = False

    def get_pinned(
        self,
        target: NormalizedURL,
        addresses: tuple[str, ...],
        request: FetchRequest,
        binding: ProviderBinding,
    ) -> DirectResponse:
        return self._request_pinned(
            "GET", target, addresses, request, binding, payload=None
        )

    def post_pinned(
        self,
        target: NormalizedURL,
        addresses: tuple[str, ...],
        request: FetchRequest,
        binding: ProviderBinding,
        payload: Mapping[str, Any],
    ) -> DirectResponse:
        return self._request_pinned(
            "POST", target, addresses, request, binding, payload=payload
        )

    def _request_pinned(
        self,
        method: str,
        target: NormalizedURL,
        addresses: tuple[str, ...],
        request: FetchRequest,
        binding: ProviderBinding,
        *,
        payload: Mapping[str, Any] | None,
    ) -> DirectResponse:
        with self._lock:
            if self._closed:
                raise OSError("pinned HTTPX client is closed")
            client = self._client
        outgoing_headers = _validate_transport_headers(request.target_headers, binding)
        local_proxy = self._proxy and not self._remote_dns_proxy
        if local_proxy and not addresses:
            raise _error("ssrf_blocked")
        candidates: tuple[str | None, ...] = (
            tuple(addresses) if local_proxy else (None,)
        )
        response: Any = None
        for index, address in enumerate(candidates):
            request_url = str(target)
            extensions: dict[str, Any] = {}
            candidate_headers = dict(outgoing_headers)
            if not self._proxy:
                extensions["sluice.addresses"] = tuple(addresses)
            elif address is not None:
                address_host = f"[{address}]" if ":" in address else address
                default_port = (target.scheme == "http" and target.port == 80) or (
                    target.scheme == "https" and target.port == 443
                )
                netloc = (
                    address_host if default_port else f"{address_host}:{target.port}"
                )
                request_url = urlunsplit(
                    (target.scheme, netloc, target.path or "/", target.query, "")
                )
                # SOCKS receives only an approved literal address.  HTTP
                # still identifies the original target, and HTTPS verifies
                # it with the original TLS SNI name.
                candidate_headers["Host"] = target.netloc
                if target.scheme == "https":
                    extensions["sni_hostname"] = target.host
            request_options: dict[str, Any] = {
                "headers": candidate_headers,
                "extensions": extensions,
            }
            timeout_extension = _request_timeout_extension(request, binding)
            if timeout_extension is not None:
                extensions["timeout"] = timeout_extension
            if payload is not None:
                request_options["json"] = dict(payload)
            http_request = httpx.Request(method, request_url, **request_options)
            try:
                response = client.send(http_request, stream=True)
                break
            except WebSafetyError:
                raise
            except httpx.TimeoutException:
                if local_proxy and index + 1 < len(candidates):
                    continue
                raise _error("timeout") from None
            except (httpx.HTTPError, OSError):
                if local_proxy and index + 1 < len(candidates):
                    continue
                raise _error("network") from None
            except Exception:
                raise _error("provider_error") from None
        if response is None:
            raise _error("network")
        try:
            # The cap is applied while iterating the decompressed response
            # stream; HTTPX never gets a chance to buffer an unbounded body.
            try:
                detached = read_decompressed_limited(
                    response,
                    min(
                        binding.max_body_bytes,
                        request.quality.max_body_bytes,
                        MAX_BODY_BYTES,
                    ),
                    deadline=request.stage_deadline,
                )
                if self._proxy:
                    # The observable socket peer is the declared proxy, not
                    # the target.  Do not misrepresent it as target evidence.
                    detached.peer = None
                    detached.url = str(target)
                    detached.final_url = str(target)
                return detached
            except WebSafetyError:
                raise
            except httpx.TimeoutException:
                raise _error("timeout") from None
            except (httpx.HTTPError, OSError):
                raise _error("network") from None
            except Exception:
                raise _error("provider_error") from None
        finally:
            response.close()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            client = self._client
        client.close()


def _error(reason: str) -> WebSafetyError:
    exc = WebSafetyError(reason)
    try:
        exc.reason = reason
    except Exception:
        pass
    return exc


def _safe_transport_safety_error(error: WebSafetyError) -> WebSafetyError:
    """Keep only reasons produced by this module across provider boundaries."""

    reason = getattr(error, "reason", None)
    if reason in _INTERNAL_SAFETY_REASONS and (
        not error.args or error.args[0] == reason
    ):
        return error
    return _error("provider_error")


@dataclass
class DirectResponse:
    """Detached response returned after bounded decompression.

    ``body`` is always decompressed bytes and never references a live HTTPX
    response.  ``content`` is accepted as a compatibility constructor alias.
    """

    status_code: int = 0
    headers: Mapping[str, str] = field(default_factory=dict)
    body: bytes = b""
    peer: str | None = None
    url: str | None = None
    final_url: str | None = None
    content: bytes | str | None = None
    elapsed_ms: int | None = None
    service: bool = False
    response: Any = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.content is not None and self.body == b"":
            self.body = (
                self.content.encode("utf-8")
                if isinstance(self.content, str)
                else bytes(self.content)
            )
        elif not isinstance(self.body, bytes):
            self.body = (
                self.body.encode("utf-8")
                if isinstance(self.body, str)
                else bytes(self.body)
            )
        self.content = self.body
        self.headers = MappingProxyType(
            {str(k).lower(): str(v) for k, v in dict(self.headers).items()}
        )
        if self.final_url is None:
            self.final_url = self.url

    @property
    def is_redirect(self) -> bool:
        return 300 <= self.status_code < 400

    @property
    def location(self) -> str | None:
        return self.headers.get("location")

    @property
    def content_type(self) -> str | None:
        value = self.headers.get("content-type")
        return value.split(";", 1)[0].strip().lower() if value else None

    @property
    def raw_body(self) -> bytes:
        return self.body

    @property
    def peer_address(self) -> str | None:
        return self.peer

    @property
    def status(self) -> int:
        return self.status_code

    @property
    def text(self) -> str:
        encoding = "utf-8"
        content_type = self.headers.get("content-type", "")
        for item in content_type.split(";")[1:]:
            key, _, value = item.partition("=")
            if key.strip().lower() == "charset" and value.strip():
                encoding = value.strip().strip("\"'")
                break
        try:
            return self.body.decode(encoding, errors="replace")
        except (LookupError, UnicodeError):
            return self.body.decode("utf-8", errors="replace")


def _header(headers: Mapping[str, Any], key: str) -> str | None:
    lowered = key.casefold()
    for name, value in headers.items():
        if str(name).casefold() == lowered:
            return str(value)
    return None


def _peer_from_response(response: Any) -> str | None:
    peer = getattr(response, "peer", None)
    if peer is not None:
        return str(peer[0] if isinstance(peer, tuple) else peer)
    extensions = getattr(response, "extensions", None)
    stream = (
        extensions.get("network_stream") if isinstance(extensions, Mapping) else None
    )
    if stream is not None:
        try:
            value = stream.get_extra_info("server_addr")
            if isinstance(value, tuple):
                return str(value[0])
            if value is not None:
                return str(value)
        except Exception:
            pass
    return None


def _check_absolute_deadline(deadline: float | None) -> None:
    if deadline is not None and time.monotonic() >= deadline:
        raise _error("timeout")


def _check_execution_probe() -> None:
    probe = current_web_execution_probe()
    if probe is not None:
        probe.check()


def _remaining_budget(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise _error("timeout")
    return remaining


def _request_timeout_values(
    request: FetchRequest,
    binding: ProviderBinding,
) -> tuple[float, float] | None:
    remaining = _remaining_budget(request.stage_deadline)
    if remaining is None:
        if binding.timeout is None:
            return None
        remaining = binding.timeout.total_seconds()
    configured_connect = binding.connect_timeout or binding.timeout
    connect = (
        remaining
        if configured_connect is None
        else min(remaining, configured_connect.total_seconds())
    )
    return remaining, connect


def _request_timeout_extension(
    request: FetchRequest,
    binding: ProviderBinding,
) -> dict[str, float] | None:
    # A catalog-owned client already has the provider defaults.  Per-request
    # extensions are needed only when the coordinator supplied an earlier
    # absolute bound.
    if request.stage_deadline is None:
        return None
    values = _request_timeout_values(request, binding)
    if values is None:
        return None
    remaining, connect = values
    return {
        "connect": connect,
        "read": remaining,
        "write": remaining,
        "pool": remaining,
    }


def _httpx_timeout(request: FetchRequest, binding: ProviderBinding) -> Any:
    values = _request_timeout_values(request, binding)
    if values is None:
        return None
    total, connect = values
    return httpx.Timeout(total, connect=connect)


def _read_bytes(
    response: Any,
    limit: int,
    *,
    deadline: float | None = None,
) -> bytes:
    """Read a response's decompressed stream and enforce *limit* exactly."""

    _check_absolute_deadline(deadline)
    content_length = _header(getattr(response, "headers", {}), "content-length")
    if content_length is not None:
        try:
            if int(content_length) > limit:
                raise _error("body_too_large")
        except ValueError:
            # Malformed length is treated as an unknown length; stream checks
            # still enforce the cap.
            pass
    chunks: list[bytes] = []
    total = 0
    iterator = getattr(response, "iter_bytes", None)
    if callable(iterator):
        try:
            pieces = iterator()
        except TypeError:
            pieces = iterator
        for chunk in pieces:
            _check_absolute_deadline(deadline)
            if chunk is None:
                continue
            piece = bytes(chunk)
            total += len(piece)
            if total > limit:
                raise _error("body_too_large")
            chunks.append(piece)
            _check_absolute_deadline(deadline)
        return b"".join(chunks)
    raw = getattr(response, "content", None)
    if raw is None:
        raw = getattr(response, "body", b"")
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    raw = bytes(raw or b"")
    if len(raw) > limit:
        raise _error("body_too_large")
    _check_absolute_deadline(deadline)
    return raw


def read_decompressed_limited(
    response: Any,
    limit: int,
    *,
    deadline: float | None = None,
) -> DirectResponse:
    """Detach *response* while enforcing a decompressed byte limit."""

    _check_absolute_deadline(deadline)
    limit = min(int(limit), MAX_BODY_BYTES)
    if limit <= 0:
        raise _error("body_too_large")
    if isinstance(response, DirectResponse):
        if len(response.body) > limit:
            raise _error("body_too_large")
        _check_absolute_deadline(deadline)
        return response
    if isinstance(response, Mapping):
        response = DirectResponse(
            status_code=int(
                response.get("status_code", response.get("status", 0)) or 0
            ),
            headers=response.get("headers", {}),
            body=response.get("body", response.get("content", b"")),
            peer=response.get("peer"),
            url=response.get("url"),
            final_url=response.get("final_url"),
        )
    elif isinstance(response, tuple) and len(response) >= 3:
        response = DirectResponse(
            status_code=int(response[0] or 0),
            headers=response[1] or {},
            body=response[2] or b"",
            peer=response[3] if len(response) > 3 else None,
            url=response[4] if len(response) > 4 else None,
        )
    headers = dict(getattr(response, "headers", {}) or {})
    peer = _peer_from_response(response)
    body = _read_bytes(response, limit, deadline=deadline)
    status = int(getattr(response, "status_code", 0) or 0)
    raw_url = getattr(response, "url", None)
    url = str(raw_url) if raw_url is not None else None
    return DirectResponse(
        status_code=status,
        headers=headers,
        body=body,
        peer=peer,
        url=url,
        final_url=url,
        response=response,
    )


def _invoke_callable(
    callable_obj: Callable[..., Any],
    target: NormalizedURL,
    addresses: tuple[str, ...],
    request: FetchRequest,
    binding: ProviderBinding,
) -> Any:
    """Invoke injected transports with a small amount of signature tolerance."""

    attempts = (
        (target, addresses, request, binding),
        (str(target), addresses, request, binding),
        (target, request, binding),
        (str(target), request, binding),
        (str(target),),
    )
    last_type_error: TypeError | None = None
    for args in attempts:
        try:
            return callable_obj(*args)
        except TypeError as error:
            last_type_error = error
            continue
    if last_type_error is not None:
        raise last_type_error
    raise _error("network")


def _sensitive_header(name: str) -> bool:
    lowered = name.casefold()
    return lowered in _SENSITIVE_HEADER_NAMES or any(
        part in lowered for part in _SENSITIVE_HEADER_PARTS
    )


def _merge_target_headers(
    request: FetchRequest, binding: ProviderBinding
) -> dict[str, str]:
    """Validate and merge provider/request target headers for one hop."""

    try:
        binding_headers = validate_target_headers(binding.target_headers)
        request_headers = validate_target_headers(request.target_headers)
    except WebSafetyError as error:
        raise _safe_transport_safety_error(error) from None
    merged = dict(binding_headers)
    seen = {name.casefold() for name in binding_headers}
    for name, value in request_headers.items():
        if name.casefold() in seen:
            raise _error("forbidden_header")
        seen.add(name.casefold())
        merged[name] = value
    cookie = _cookie_header(binding)
    if cookie is not None:
        merged["Cookie"] = cookie
    return merged


def merge_target_headers(
    request: FetchRequest, binding: ProviderBinding
) -> dict[str, str]:
    """Return validated provider/request target headers and declared cookies."""

    return _merge_target_headers(request, binding)


def _cookie_header(binding: ProviderBinding) -> str | None:
    if not binding.cookies:
        return None
    cookie_parts: list[str] = []
    for name, value in binding.cookies.items():
        if (
            not isinstance(name, str)
            or not name.strip()
            or any(ord(char) < 0x21 or char in ";=\r\n" for char in name)
        ):
            raise _error("forbidden_header")
        if not isinstance(value, str) or any(
            ord(char) < 0x20 or char in ";\r\n" for char in value
        ):
            raise _error("forbidden_header")
        cookie_parts.append(f"{name}={value}")
    return "; ".join(cookie_parts) if cookie_parts else None


def _validate_transport_headers(
    headers: Mapping[str, str], binding: ProviderBinding
) -> dict[str, str]:
    """Validate public sender headers, allowing only its own cookie header."""

    if not isinstance(headers, Mapping):
        raise _error("forbidden_header")
    cookie_name: str | None = None
    cookie_value: str | None = None
    ordinary: dict[str, str] = {}
    for name, value in headers.items():
        if str(name).casefold() == "cookie":
            if cookie_name is not None:
                raise _error("forbidden_header")
            cookie_name, cookie_value = str(name), value
        else:
            ordinary[name] = value
    validated = validate_target_headers(ordinary)
    if cookie_name is not None:
        expected = _cookie_header(binding)
        if expected is None or cookie_value != expected:
            raise _error("forbidden_header")
        validated[cookie_name] = cookie_value
    return validated


def _strip_sensitive_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {
        name: value
        for name, value in headers.items()
        if not _sensitive_header(str(name))
    }


def _request_with_headers(
    request: FetchRequest, headers: Mapping[str, str]
) -> FetchRequest:
    return FetchRequest(
        url=request.url,
        mode=request.mode,
        acceptable=request.acceptable,
        preferred=request.preferred,
        list_selector=request.list_selector,
        target_headers=dict(headers),
        quality=request.quality,
        stage_deadline=request.stage_deadline,
    )


def _service_endpoint(value: str) -> tuple[NormalizedURL, TargetPolicy]:
    """Normalize a service endpoint with an exact-host private-network policy."""

    try:
        parsed = urlsplit(value)
        host = parsed.hostname
    except (TypeError, ValueError):
        raise _error("bad_url") from None
    if not host:
        raise _error("bad_url")
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if isinstance(literal, ipaddress.IPv6Address) and literal.ipv4_mapped is not None:
        literal = literal.ipv4_mapped
    # Service bases are operator-controlled.  Give a private literal only its
    # exact /32 or /128 service exception; target ``allow_hosts`` semantics
    # continue to require an explicitly configured CIDR for literal targets.
    service_cidrs = (
        (str(literal),)
        if literal is not None and is_non_public_address(str(literal))
        else ()
    )
    policy = TargetPolicy(allow_hosts=(host,), allow_cidrs=service_cidrs)
    return normalize_target_url(value, policy), policy


def _explicit_private_service_host(host: str) -> bool:
    """Recognize operator-explicit self-host names without trusting DNS alone."""

    if is_non_public_address(host):
        return True
    normalized = host.casefold().rstrip(".")
    if normalized in {"localhost", "localhost.localdomain", "ip6-localhost"}:
        return True
    # Unqualified and private-name suffixes are conventional self-host
    # configuration.  A public DNS name resolving to a private peer is not.
    return "." not in normalized or normalized.endswith(
        (".local", ".localhost", ".internal", ".lan", ".home")
    )


def _guard_service_addresses(
    service: NormalizedURL, policy: TargetPolicy
) -> tuple[str, ...]:
    addresses = guard_resolved_addresses(service.host, policy)
    private = [is_non_public_address(address) for address in addresses]
    if any(private) and (
        not _explicit_private_service_host(service.host) or not all(private)
    ):
        raise _error("ssrf_blocked")
    return addresses


def _send_injected(
    transport: Any,
    target: NormalizedURL,
    addresses: tuple[str, ...],
    request: FetchRequest,
    binding: ProviderBinding,
) -> Any:
    pinned_get = getattr(transport, "get_pinned", None)
    if callable(pinned_get):
        # Catalog-owned HTTPX clients must use this path.  Passing their
        # ordinary ``Client.get`` would let HTTPX resolve the target again
        # and would buffer the response before the decompressed cap runs.
        return pinned_get(target, addresses, request, binding)
    if callable(transport):
        return _invoke_callable(transport, target, addresses, request, binding)
    getter = getattr(transport, "get", None)
    if callable(getter):
        try:
            return getter(
                str(target),
                headers=dict(request.target_headers),
                follow_redirects=False,
            )
        except TypeError:
            # Never retry without the explicit redirect guard: clients that
            # reject this keyword may otherwise auto-follow cross-host hops.
            raise _error("provider_error") from None
    sender = getattr(transport, "send", None)
    if callable(sender):
        http_request = httpx.Request(
            "GET", str(target), headers=dict(request.target_headers)
        )
        return sender(http_request, follow_redirects=False)
    raise _error("network")


def _service_mapping_response(result: Any, target: NormalizedURL) -> Any:
    """Encode a decoded fake-service result as the JSON HTTP body it represents."""

    if not isinstance(result, Mapping):
        return result
    envelope_keys = {
        "status",
        "status_code",
        "headers",
        "body",
        "content",
        "url",
        "final_url",
        "peer",
    }
    if envelope_keys.intersection(result):
        return result
    try:
        body = json.dumps(result).encode("utf-8")
    except (TypeError, ValueError):
        raise _error("provider_error") from None
    return DirectResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        body=body,
        url=str(target),
        final_url=str(target),
    )


def _send_injected_post(
    transport: Any,
    target: NormalizedURL,
    addresses: tuple[str, ...],
    request: FetchRequest,
    binding: ProviderBinding,
    payload: Mapping[str, Any],
) -> Any:
    """Issue exactly one injected POST; never signature-retry after a send."""

    pinned_post = getattr(transport, "post_pinned", None)
    if callable(pinned_post):
        return pinned_post(target, addresses, request, binding, payload)
    post = getattr(transport, "post", None)
    if callable(post):
        return post(
            str(target),
            json=dict(payload),
            headers=dict(request.target_headers),
            follow_redirects=False,
        )
    requester = getattr(transport, "request", None)
    if callable(requester):
        return requester(
            "POST",
            str(target),
            json=dict(payload),
            headers=dict(request.target_headers),
            follow_redirects=False,
        )
    if isinstance(transport, Mapping):
        callback = transport.get("post")
        if callable(callback):
            return callback(
                str(target),
                json=dict(payload),
                headers=dict(request.target_headers),
                follow_redirects=False,
            )
        callback = transport.get("request")
        if callable(callback):
            return callback(
                "POST",
                str(target),
                json=dict(payload),
                headers=dict(request.target_headers),
                follow_redirects=False,
            )
    sender = getattr(transport, "send", None)
    if callable(sender):
        http_request = httpx.Request(
            "POST",
            str(target),
            headers=dict(request.target_headers),
            json=dict(payload),
        )
        return sender(http_request, follow_redirects=False)
    if callable(transport):
        return transport(
            "POST",
            str(target),
            json=dict(payload),
            headers=dict(request.target_headers),
            follow_redirects=False,
        )
    raise _error("network")


def send_pinned_httpx_request(
    target: NormalizedURL,
    addresses: tuple[str, ...],
    request: FetchRequest | None = None,
    binding: ProviderBinding | None = None,
    *,
    transport: Any = None,
    headers: Mapping[str, str] | None = None,
) -> DirectResponse:
    """Perform one redirect-disabled GET and return a detached response.

    A custom ``transport`` is intentionally an explicit call argument; it is
    useful for deterministic tests and is not serializable provider state.
    """

    request = request or FetchRequest(
        url=str(target), mode="article", acceptable=frozenset({"text"})
    )
    binding = binding or ProviderBinding("direct", "httpx")
    source_headers = headers if headers is not None else request.target_headers
    outgoing_headers = _validate_transport_headers(source_headers, binding)
    if transport is not None:
        try:
            request_for_transport = request
            if headers is not None:
                request_for_transport = _request_with_headers(request, outgoing_headers)
            result = _send_injected(
                transport, target, addresses, request_for_transport, binding
            )
            detached = read_decompressed_limited(
                result,
                min(binding.max_body_bytes, MAX_BODY_BYTES),
                deadline=request.stage_deadline,
            )
            if detached.url is None:
                detached.url = str(target)
                detached.final_url = str(target)
            return detached
        except (WebAttemptCancelled, WebStageDeadlineExpired):
            raise
        except WebSafetyError as error:
            raise _safe_transport_safety_error(error) from None
        except Exception:
            # Injected transports are provider boundaries; vendor exception
            # text and response bodies must never escape this function.
            raise _error("provider_error") from None

    timeout = _httpx_timeout(request, binding)
    try:
        pinned_transport = _PinnedHTTPTransport(addresses)
        with httpx.Client(
            transport=pinned_transport,
            follow_redirects=False,
            timeout=timeout,
            trust_env=False,
        ) as client, client.stream(
            "GET", str(target), headers=outgoing_headers
        ) as response:
            return read_decompressed_limited(
                response,
                min(binding.max_body_bytes, MAX_BODY_BYTES),
                deadline=request.stage_deadline,
            )
    except (WebAttemptCancelled, WebStageDeadlineExpired):
        raise
    except WebSafetyError as error:
        raise _safe_transport_safety_error(error) from None
    except httpx.TimeoutException:
        raise _error("timeout") from None
    except (httpx.HTTPError, OSError):
        raise _error("network") from None
    except Exception:
        # No third-party exception text is allowed to cross this boundary.
        raise _error("provider_error") from None


def send_pinned_httpx_post(
    target: NormalizedURL,
    addresses: tuple[str, ...],
    payload: Mapping[str, Any],
    request: FetchRequest,
    binding: ProviderBinding,
    *,
    transport: Any = None,
) -> DirectResponse:
    """Perform one redirect-disabled JSON POST and return a bounded response."""

    outgoing_headers = validate_target_headers(request.target_headers)
    request = _request_with_headers(request, outgoing_headers)
    limit = min(binding.max_body_bytes, request.quality.max_body_bytes, MAX_BODY_BYTES)
    if transport is not None:
        try:
            result = _send_injected_post(
                transport, target, addresses, request, binding, payload
            )
            result = _service_mapping_response(result, target)
            detached = read_decompressed_limited(
                result,
                limit,
                deadline=request.stage_deadline,
            )
            if detached.url is None:
                detached.url = str(target)
                detached.final_url = str(target)
            return detached
        except (WebAttemptCancelled, WebStageDeadlineExpired):
            raise
        except WebSafetyError as error:
            raise _safe_transport_safety_error(error) from None
        except Exception:
            raise _error("provider_error") from None

    timeout = _httpx_timeout(request, binding)
    try:
        pinned_transport = _PinnedHTTPTransport(addresses)
        with (
            httpx.Client(
                transport=pinned_transport,
                follow_redirects=False,
                timeout=timeout,
                trust_env=False,
            ) as client,
            client.stream(
                "POST",
                str(target),
                headers=outgoing_headers,
                json=dict(payload),
            ) as response,
        ):
            return read_decompressed_limited(
                response,
                limit,
                deadline=request.stage_deadline,
            )
    except (WebAttemptCancelled, WebStageDeadlineExpired):
        raise
    except WebSafetyError as error:
        raise _safe_transport_safety_error(error) from None
    except httpx.TimeoutException:
        raise _error("timeout") from None
    except (httpx.HTTPError, OSError):
        raise _error("network") from None
    except Exception:
        raise _error("provider_error") from None


def _transport_for_binding(binding: ProviderBinding, explicit: Any) -> Any:
    if explicit is not None:
        return explicit
    # A few test harnesses attach an ephemeral transport after construction.
    # ProviderBinding itself rejects non-JSON options, so this path is only an
    # in-memory attribute hook and is never retained in a semantic snapshot.
    return getattr(binding, "transport", None)


def bounded_manual_get(
    request: FetchRequest,
    binding: ProviderBinding,
    transport: Any = None,
) -> DirectResponse:
    """GET a target with guarded DNS, peers, redirects, headers, and body size."""

    if not isinstance(request, FetchRequest) or not isinstance(
        binding, ProviderBinding
    ):
        raise _error("provider_error")
    target_policy = TargetPolicy.from_value(binding.target_policy)
    headers = _merge_target_headers(request, binding)
    request = _request_with_headers(request, headers)

    current = normalize_target_url(request.url, target_policy)
    injected = _transport_for_binding(binding, transport)
    service_target: NormalizedURL | None = None
    if binding.base_url:
        try:
            service_target, _service_policy = _service_endpoint(binding.base_url)
        except WebSafetyError:
            # A manual target is still governed by its target policy.  An
            # invalid optional service URL cannot become a redirect target.
            service_target = None
    redirect_count = 0
    while True:
        _check_absolute_deadline(request.stage_deadline)
        addresses = guard_resolved_addresses(current.host, target_policy)
        _check_absolute_deadline(request.stage_deadline)
        # DNS may block long enough for Stage ownership to be lost.  Recheck
        # immediately before each paid/direct send, including redirect hops.
        _check_execution_probe()
        try:
            if injected is None:
                response = send_pinned_httpx_request(
                    current, addresses, request, binding
                )
            else:
                response = send_pinned_httpx_request(
                    current,
                    addresses,
                    request,
                    binding,
                    transport=injected,
                )
        except TypeError:
            # Monkeypatched senders commonly expose the two documented core
            # arguments.  Retry only for a signature mismatch, never for a
            # network failure.
            try:
                response = send_pinned_httpx_request(current, addresses)
            except (WebAttemptCancelled, WebStageDeadlineExpired):
                raise
            except WebSafetyError as error:
                raise _safe_transport_safety_error(error) from None
            except Exception:
                raise _error("provider_error") from None
        except (WebAttemptCancelled, WebStageDeadlineExpired):
            raise
        except WebSafetyError as error:
            raise _safe_transport_safety_error(error) from None
        except Exception:
            raise _error("provider_error") from None

        response = read_decompressed_limited(
            response,
            min(binding.max_body_bytes, request.quality.max_body_bytes, MAX_BODY_BYTES),
            deadline=request.stage_deadline,
        )
        _check_execution_probe()
        if response.peer is not None and response.peer not in addresses:
            raise _error("peer_mismatch")
        reported = response.final_url or response.url
        if reported:
            try:
                reported_target = normalize_target_url(reported, target_policy)
            except WebSafetyError:
                raise _error("ssrf_blocked") from None
            if (
                reported_target.host != current.host
                or reported_target.port != current.port
                or reported_target.scheme != current.scheme
            ):
                # A transport must not silently change the target host without
                # going through a guarded redirect hop.
                raise _error("peer_mismatch")
            response.final_url = str(reported_target)
        else:
            response.final_url = str(current)
        if not response.is_redirect:
            return response
        if redirect_count >= MAX_REDIRECTS:
            raise _error("too_many_redirects")
        location = response.location
        if not location:
            raise _error("http_status")
        try:
            redirect_policy = target_policy
            if (
                service_target is not None
                and service_target.host not in target_policy.allow_hosts
            ):
                # Permit normalization of the configured service host solely
                # so it can be rejected as ``service_redirect`` below.  No
                # other private host is added to the target policy.
                redirect_policy = TargetPolicy(
                    allow_hosts=target_policy.allow_hosts + (service_target.host,),
                    allow_cidrs=target_policy.allow_cidrs,
                    allow_remote_dns_proxy=target_policy.allow_remote_dns_proxy,
                )
            next_target = join_target_url(current, location, redirect_policy)
        except WebSafetyError as error:
            raise _safe_transport_safety_error(error) from None
        # A target redirect to the configured service host is rejected before
        # DNS resolution or a request to that endpoint.  In particular this
        # check also handles a private localhost service URL.
        if service_target is not None and next_target.host == service_target.host:
            raise _error("service_redirect")
        # Never carry credentials across an origin boundary.  Ordinary
        # tracing headers remain available, while authorization/cookie-style
        # headers are stripped before the next transport call.
        if (
            next_target.host != current.host
            or next_target.port != current.port
            or next_target.scheme != current.scheme
        ):
            request = _request_with_headers(
                request, _strip_sensitive_headers(request.target_headers)
            )
        current = next_target
        redirect_count += 1


def bounded_service_get(
    url: str,
    binding: ProviderBinding,
    headers: Mapping[str, str] | None = None,
    transport: Any = None,
    *,
    deadline: float | None = None,
) -> DirectResponse:
    """Call a provider service endpoint exactly once (no redirects)."""

    try:
        if not isinstance(binding, ProviderBinding) or not binding.base_url:
            raise _error("provider_error")
        # Service calls have their own protected-header validation.  This is
        # intentionally performed before URL policy checks so a caller cannot
        # use a private service URL to bypass the header boundary.
        validate_target_headers(binding.target_headers)
        validated_headers = validate_target_headers(headers)
        service, service_policy = _service_endpoint(binding.base_url)
        target = normalize_target_url(url, service_policy)
        if target.host != service.host or target.port != service.port:
            raise _error("service_redirect")
        # A service configured for HTTPS must never be downgraded by a caller
        # supplied HTTP URL.  Public HTTP service bases are rejected below.
        if service.scheme == "https" and target.scheme != "https":
            raise _error("service_redirect")
        addresses = _guard_service_addresses(service, service_policy)
        if service.scheme == "http" and any(
            not is_non_public_address(address) for address in addresses
        ):
            raise _error("service_https_required")
        _check_absolute_deadline(deadline)
        _check_execution_probe()
        request = FetchRequest(
            url=str(target),
            mode="article",
            acceptable=frozenset({"text"}),
            target_headers=validated_headers,
            stage_deadline=deadline,
        )
        if transport is None:
            response = send_pinned_httpx_request(target, addresses, request, binding)
        else:
            response = send_pinned_httpx_request(
                target, addresses, request, binding, transport=transport
            )
        _check_execution_probe()
        if response.peer is not None and response.peer not in addresses:
            raise _error("peer_mismatch")
        reported = response.final_url or response.url
        if reported:
            try:
                reported_target = normalize_target_url(reported, service_policy)
            except WebSafetyError:
                raise _error("service_redirect") from None
            if (
                reported_target.host != target.host
                or reported_target.port != target.port
                or reported_target.scheme != target.scheme
            ):
                raise _error("service_redirect")
            response.final_url = str(reported_target)
        if response.is_redirect:
            raise _error("service_redirect")
        return read_decompressed_limited(
            response,
            min(binding.max_body_bytes, MAX_BODY_BYTES),
            deadline=deadline,
        )
    except (WebAttemptCancelled, WebStageDeadlineExpired):
        raise
    except WebSafetyError as error:
        raise _safe_transport_safety_error(error) from None
    except Exception:
        # Keep injected/direct transport details out of provider-facing errors.
        raise _error("provider_error") from None


def bounded_service_post(
    url: str,
    binding: ProviderBinding,
    payload: Mapping[str, Any],
    headers: Mapping[str, str] | None = None,
    transport: Any = None,
    *,
    deadline: float | None = None,
) -> DirectResponse:
    """POST JSON to a provider service endpoint exactly once (no redirects)."""

    try:
        if (
            not isinstance(binding, ProviderBinding)
            or not binding.base_url
            or not isinstance(payload, Mapping)
        ):
            raise _error("provider_error")
        # Validate target-side options before a vendor request is issued.  The
        # values themselves are mapped into the vendor payload by adapters and
        # are never sent as headers to the service endpoint.
        validate_target_headers(binding.target_headers)
        validated_headers = validate_target_headers(headers)
        service, service_policy = _service_endpoint(binding.base_url)
        target = normalize_target_url(url, service_policy)
        if target.host != service.host or target.port != service.port:
            raise _error("service_redirect")
        if target.scheme != service.scheme:
            raise _error("service_redirect")
        addresses = _guard_service_addresses(service, service_policy)
        if service.scheme == "http" and any(
            not is_non_public_address(address) for address in addresses
        ):
            raise _error("service_https_required")
        _check_absolute_deadline(deadline)
        _check_execution_probe()
        request = FetchRequest(
            url=str(target),
            mode="article",
            acceptable=frozenset({"text"}),
            target_headers=validated_headers,
            stage_deadline=deadline,
        )
        response = send_pinned_httpx_post(
            target,
            addresses,
            payload,
            request,
            binding,
            transport=transport,
        )
        _check_execution_probe()
        if response.peer is not None and response.peer not in addresses:
            raise _error("peer_mismatch")
        reported = response.final_url or response.url
        if reported:
            try:
                reported_target = normalize_target_url(reported, service_policy)
            except WebSafetyError:
                raise _error("service_redirect") from None
            if (
                reported_target.host != target.host
                or reported_target.port != target.port
                or reported_target.scheme != target.scheme
                or reported_target.path != target.path
                or reported_target.query != target.query
            ):
                raise _error("service_redirect")
            response.final_url = str(reported_target)
        if response.is_redirect:
            raise _error("service_redirect")
        return read_decompressed_limited(
            response,
            min(binding.max_body_bytes, MAX_BODY_BYTES),
            deadline=deadline,
        )
    except (WebAttemptCancelled, WebStageDeadlineExpired):
        raise
    except WebSafetyError as error:
        raise _safe_transport_safety_error(error) from None
    except Exception:
        raise _error("provider_error") from None


__all__ = [
    "MAX_BODY_BYTES",
    "DirectResponse",
    "bounded_manual_get",
    "bounded_service_get",
    "bounded_service_post",
    "merge_target_headers",
    "read_decompressed_limited",
    "send_pinned_httpx_post",
    "send_pinned_httpx_request",
]
