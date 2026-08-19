"""Bounded direct HTTP transport used by built-in web providers.

The public helper deliberately uses a small response value object.  Provider
adapters may inject a transport in tests or for the trusted compatibility
adapter, while production calls use an HTTPX client with redirects disabled.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, Mapping

import httpx
import httpcore

from ..errors import WebSafetyError
from .contracts import FetchRequest, ProviderBinding
from .url import (
    MAX_REDIRECTS,
    NormalizedURL,
    TargetPolicy,
    guard_resolved_addresses,
    join_target_url,
    normalize_target_url,
    validate_target_headers,
)


MAX_BODY_BYTES = 10_485_760


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

    def __init__(self, addresses: tuple[str, ...]):
        self._pool = httpcore.ConnectionPool(network_backend=_PinnedSyncBackend(addresses))

    def handle_request(self, request: httpx.Request) -> httpx.Response:
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
        response = self._pool.handle_request(core_request)
        return httpx.Response(
            status_code=response.status,
            headers=response.headers,
            stream=_PinnedResponseStream(response.stream),
            extensions=response.extensions,
            request=request,
        )

    def close(self) -> None:
        self._pool.close()


def _error(reason: str) -> WebSafetyError:
    exc = WebSafetyError(reason)
    try:
        setattr(exc, "reason", reason)
    except Exception:
        pass
    return exc


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
            self.body = self.content.encode("utf-8") if isinstance(self.content, str) else bytes(self.content)
        elif not isinstance(self.body, bytes):
            self.body = self.body.encode("utf-8") if isinstance(self.body, str) else bytes(self.body)
        self.content = self.body
        self.headers = MappingProxyType({str(k).lower(): str(v) for k, v in dict(self.headers).items()})
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
                encoding = value.strip().strip('"\'')
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
    stream = extensions.get("network_stream") if isinstance(extensions, Mapping) else None
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


def _read_bytes(response: Any, limit: int) -> bytes:
    """Read a response's decompressed stream and enforce *limit* exactly."""

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
            if chunk is None:
                continue
            piece = bytes(chunk)
            total += len(piece)
            if total > limit:
                raise _error("body_too_large")
            chunks.append(piece)
        return b"".join(chunks)
    raw = getattr(response, "content", None)
    if raw is None:
        raw = getattr(response, "body", b"")
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    raw = bytes(raw or b"")
    if len(raw) > limit:
        raise _error("body_too_large")
    return raw


def read_decompressed_limited(response: Any, limit: int) -> DirectResponse:
    """Detach *response* while enforcing a decompressed byte limit."""

    limit = min(int(limit), MAX_BODY_BYTES)
    if limit <= 0:
        raise _error("body_too_large")
    if isinstance(response, DirectResponse):
        if len(response.body) > limit:
            raise _error("body_too_large")
        return response
    if isinstance(response, Mapping):
        response = DirectResponse(
            status_code=int(response.get("status_code", response.get("status", 0)) or 0),
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
    body = _read_bytes(response, limit)
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


def _invoke_callable(callable_obj: Callable[..., Any], target: NormalizedURL, addresses: tuple[str, ...], request: FetchRequest, binding: ProviderBinding) -> Any:
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


def _send_injected(transport: Any, target: NormalizedURL, addresses: tuple[str, ...], request: FetchRequest, binding: ProviderBinding) -> Any:
    if callable(transport):
        return _invoke_callable(transport, target, addresses, request, binding)
    getter = getattr(transport, "get", None)
    if callable(getter):
        try:
            return getter(str(target), headers=dict(request.target_headers), follow_redirects=False)
        except TypeError:
            try:
                return getter(str(target), headers=dict(request.target_headers))
            except TypeError:
                return getter(str(target))
    sender = getattr(transport, "send", None)
    if callable(sender):
        http_request = httpx.Request("GET", str(target), headers=dict(request.target_headers))
        return sender(http_request, follow_redirects=False)
    raise _error("network")


def send_pinned_httpx_request(
    target: NormalizedURL,
    addresses: tuple[str, ...],
    request: FetchRequest | None = None,
    binding: ProviderBinding | None = None,
    *,
    transport: Any = None,
) -> DirectResponse:
    """Perform one redirect-disabled GET and return a detached response.

    A custom ``transport`` is intentionally an explicit call argument; it is
    useful for deterministic tests and is not serializable provider state.
    """

    request = request or FetchRequest(
        url=str(target), mode="article", acceptable=frozenset({"text"})
    )
    binding = binding or ProviderBinding("direct", "httpx")
    if transport is not None:
        result = _send_injected(transport, target, addresses, request, binding)
        detached = read_decompressed_limited(result, min(binding.max_body_bytes, MAX_BODY_BYTES))
        if detached.url is None:
            detached.url = str(target)
            detached.final_url = str(target)
        return detached

    timeout = None
    if binding.timeout is not None:
        timeout = httpx.Timeout(binding.timeout.total_seconds(), connect=(binding.connect_timeout or binding.timeout).total_seconds())
    try:
        pinned_transport = _PinnedHTTPTransport(addresses)
        with httpx.Client(
            transport=pinned_transport,
            follow_redirects=False,
            timeout=timeout,
            trust_env=False,
        ) as client:
            with client.stream("GET", str(target), headers=dict(request.target_headers)) as response:
                return read_decompressed_limited(response, min(binding.max_body_bytes, MAX_BODY_BYTES))
    except WebSafetyError:
        raise
    except httpx.TimeoutException:
        raise _error("timeout") from None
    except (httpx.HTTPError, OSError):
        raise _error("network") from None
    except Exception:
        # No third-party exception text is allowed to cross this boundary.
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

    if not isinstance(request, FetchRequest) or not isinstance(binding, ProviderBinding):
        raise _error("provider_error")
    target_policy = TargetPolicy.from_value(binding.target_policy)
    headers = validate_target_headers(request.target_headers)
    if headers != dict(request.target_headers):
        # Keep the normalized validation result in a detached request-like
        # value without mutating the caller's frozen contract.
        request = FetchRequest(
            url=request.url,
            mode=request.mode,
            acceptable=request.acceptable,
            preferred=request.preferred,
            list_selector=request.list_selector,
            target_headers=headers,
            quality=request.quality,
            stage_deadline=request.stage_deadline,
        )

    current = normalize_target_url(request.url, target_policy)
    injected = _transport_for_binding(binding, transport)
    redirect_count = 0
    while True:
        addresses = guard_resolved_addresses(current.host, target_policy)
        try:
            if injected is None:
                response = send_pinned_httpx_request(current, addresses, request, binding)
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
            except WebSafetyError:
                raise
            except Exception:
                raise _error("network") from None
        except WebSafetyError:
            raise
        except Exception:
            raise _error("network") from None

        response = read_decompressed_limited(
            response, min(binding.max_body_bytes, request.quality.max_body_bytes, MAX_BODY_BYTES)
        )
        if response.peer is not None and response.peer not in addresses:
            raise _error("peer_mismatch")
        reported = response.final_url or response.url
        if reported:
            try:
                reported_target = normalize_target_url(reported, target_policy)
            except WebSafetyError:
                raise _error("ssrf_blocked") from None
            if reported_target.host != current.host or reported_target.port != current.port:
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
            next_target = join_target_url(current, location, target_policy)
        except WebSafetyError:
            raise
        # A service endpoint never follows a redirect because credentials could
        # otherwise be sent to an unexpected host.
        if binding.base_url:
            try:
                service_target = normalize_target_url(binding.base_url, target_policy)
                if current.host == service_target.host and current.port == service_target.port:
                    raise _error("service_redirect")
            except WebSafetyError as error:
                if getattr(error, "reason", None) == "service_redirect":
                    raise
        current = next_target
        redirect_count += 1


def bounded_service_get(
    url: str,
    binding: ProviderBinding,
    headers: Mapping[str, str] | None = None,
    transport: Any = None,
) -> DirectResponse:
    """Call a provider service endpoint exactly once (no redirects)."""

    if not binding.base_url:
        raise _error("provider_error")
    target = normalize_target_url(url, binding.target_policy)
    service = normalize_target_url(binding.base_url, binding.target_policy)
    if target.host != service.host or target.port != service.port:
        raise _error("service_redirect")
    request = FetchRequest(
        url=str(target), mode="article", acceptable=frozenset({"text"}), target_headers=headers or {}
    )
    addresses = guard_resolved_addresses(target.host, binding.target_policy)
    if transport is None:
        response = send_pinned_httpx_request(target, addresses, request, binding)
    else:
        response = send_pinned_httpx_request(
            target, addresses, request, binding, transport=transport
        )
    if response.is_redirect:
        raise _error("service_redirect")
    return read_decompressed_limited(response, min(binding.max_body_bytes, MAX_BODY_BYTES))


__all__ = [
    "DirectResponse",
    "MAX_BODY_BYTES",
    "bounded_manual_get",
    "bounded_service_get",
    "read_decompressed_limited",
    "send_pinned_httpx_request",
]
