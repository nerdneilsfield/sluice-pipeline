"""Fixed startup-bound web provider adapters."""

from __future__ import annotations

import base64
import binascii
import importlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Protocol
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

from ..errors import DefinitionError, ExecutionError, WebSafetyError
from .contracts import (
    NO_MATCH,
    DocumentArtifact,
    ExtractorBinding,
    FetchRequest,
    ProviderBinding,
    WebAttemptCancelled,
    WebStageDeadlineExpired,
)
from .extractors import build_extractor as _build_extractor
from .feed import validate_raw_feed
from .http import (
    DirectResponse,
    bounded_manual_get,
    bounded_service_post,
    merge_target_headers,
)
from .url import TargetPolicy, normalize_target_url, validate_target_headers


class WebProvider(Protocol):
    binding: ProviderBinding

    def fetch(self, request: FetchRequest) -> DocumentArtifact:
        raise NotImplementedError


def _runtime_value(runtime: object, name: str, default: Any = None) -> Any:
    if runtime is None:
        return default
    if isinstance(runtime, Mapping):
        return runtime.get(name, default)
    return getattr(runtime, name, default)


def _transport_for(
    runtime: object,
    binding: ProviderBinding,
    *,
    binding_specific_only: bool = False,
) -> Any:
    resolver = _runtime_value(runtime, "transport_for")
    if callable(resolver):
        try:
            found = resolver(binding)
        except TypeError:
            found = resolver(binding.name)
        if found is not None:
            return found
    catalog = _runtime_value(runtime, "catalog")
    resolver = getattr(catalog, "transport_for", None)
    if callable(resolver):
        found = resolver(binding)
        if found is not None:
            return found
    transports = _runtime_value(runtime, "transports")
    if isinstance(transports, Mapping) and transports.get(binding.name) is not None:
        return transports[binding.name]
    if binding_specific_only:
        return None
    for name in ("httpx_client", "http_client", "transport"):
        found = _runtime_value(runtime, name)
        if found is not None:
            return found
    return None


def _response_body(response: Any) -> tuple[int, Mapping[str, str], bytes, str | None]:
    if isinstance(response, str):
        return 200, {}, response.encode("utf-8"), None
    if isinstance(response, (bytes, bytearray, memoryview)):
        return 200, {}, bytes(response), None
    if isinstance(response, DirectResponse):
        return (
            response.status_code,
            response.headers,
            response.body,
            response.final_url or response.url,
        )
    if isinstance(response, Mapping):
        raw = response.get("body", response.get("content", b""))
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        return (
            int(response.get("status_code", response.get("status", 200)) or 0),
            {str(k): str(v) for k, v in dict(response.get("headers", {})).items()},
            bytes(raw or b""),
            None
            if response.get("final_url", response.get("url")) is None
            else str(response.get("final_url", response.get("url"))),
        )
    status = int(
        getattr(response, "status_code", getattr(response, "status", 200)) or 0
    )
    headers = {
        str(k): str(v) for k, v in dict(getattr(response, "headers", {}) or {}).items()
    }
    body = getattr(response, "content", None)
    if body is None:
        body = getattr(response, "body", None)
    if body is None:
        body = getattr(response, "text", "")
    if isinstance(body, str):
        body = body.encode("utf-8")
    final = getattr(response, "final_url", None)
    if final is None:
        final = getattr(response, "url", None)
    return status, headers, bytes(body or b""), None if final is None else str(final)


def _header(headers: Mapping[str, str], name: str) -> str | None:
    wanted = name.casefold()
    for key, value in headers.items():
        if str(key).casefold() == wanted:
            return str(value)
    return None


def _decode(body: bytes, headers: Mapping[str, str]) -> str:
    charset = "utf-8"
    match = re.search(
        r"charset\s*=\s*[\"']?([^;\"'\s]+)",
        _header(headers, "content-type") or "",
        flags=re.IGNORECASE,
    )
    if match:
        charset = match.group(1)
    try:
        return body.decode(charset, errors="replace")
    except (LookupError, UnicodeError):
        return body.decode("utf-8", errors="replace")


def _sniff_content_type(body: bytes, supplied: str | None) -> str:
    if supplied:
        return supplied.split(";", 1)[0].strip().lower()
    prefix = body.lstrip()[:512].lower()
    if prefix.startswith((b"<?xml", b"<rss", b"<feed", b"<rdf:rdf")):
        return "application/xml"
    if prefix.startswith(b"<") and re.match(
        rb"<\s*(?:!doctype\s+html|html|article|main|body|div|p)\b", prefix
    ):
        return "text/html"
    return "text/plain"


def _allowed_representation(request: FetchRequest, content_type: str) -> str | None:
    if request.mode == "raw_source":
        return "raw_source" if "raw_source" in request.acceptable else None
    if (
        content_type
        in {
            "application/xml",
            "text/xml",
            "application/rss+xml",
            "application/atom+xml",
        }
        and "raw_source" in request.acceptable
    ):
        return "raw_source"
    is_html = content_type in {"text/html", "application/xhtml+xml"}
    if is_html and "raw_html" in request.acceptable:
        return "raw_html"
    if "text" in request.acceptable:
        return "text"
    if "markdown" in request.acceptable:
        return "markdown"
    return None


def _artifact_from_response(
    binding: ProviderBinding,
    request: FetchRequest,
    response: Any,
    *,
    default_status: int | None = None,
    force_final_url: str | None = None,
) -> DocumentArtifact:
    status, headers, body, response_url = _response_body(response)
    if default_status is not None and status == 0:
        status = default_status
    content_type = _sniff_content_type(body, _header(headers, "content-type"))
    final_url = force_final_url if force_final_url is not None else response_url
    if final_url is None:
        final_url = request.url
    try:
        final_url = str(normalize_target_url(final_url, binding.target_policy))
    except WebSafetyError:
        raise
    except Exception:
        raise WebSafetyError("provider_error") from None
    representation = _allowed_representation(request, content_type)
    fields: dict[str, Any] = {
        "source_url": request.url,
        "final_url": final_url,
        "status_code": status,
        "content_type": content_type,
        "provider_instance": binding.name,
        "provider_type": binding.type,
        "capabilities": frozenset(),
    }
    if representation == "raw_source":
        fields["raw_source"] = bytes(body)
    elif representation == "raw_html":
        fields["raw_html"] = _decode(body, headers)
    elif representation == "markdown":
        fields["markdown"] = _decode(body, headers)
    elif representation == "text":
        fields["text"] = _decode(body, headers)
    if representation is not None:
        fields["capabilities"] = frozenset({representation})
    return DocumentArtifact(**fields)


def _safe_fetch(call: Any) -> Any:
    try:
        return call()
    except (WebAttemptCancelled, WebStageDeadlineExpired):
        raise
    except WebSafetyError:
        raise
    except ExecutionError:
        raise
    except BaseException:
        raise WebSafetyError("provider_error") from None


@dataclass(frozen=True, slots=True)
class HTTPXProvider:
    binding: ProviderBinding
    runtime: object | None = None

    def fetch(self, request: FetchRequest) -> DocumentArtifact:
        response = _safe_fetch(
            lambda: bounded_manual_get(
                request,
                self.binding,
                transport=_transport_for(self.runtime, self.binding),
            )
        )
        return _safe_fetch(
            lambda: _artifact_from_response(self.binding, request, response)
        )


HttpxProvider = HTTPXProvider


@dataclass(frozen=True, slots=True)
class StartupHTTPProvider:
    binding: ProviderBinding
    runtime: object | None = None

    def __post_init__(self) -> None:
        if self.binding.options.get("trusted") is not True:
            raise DefinitionError(
                f"startup_http provider {self.binding.name!r} requires trusted: true"
            )
        # This compatibility adapter is deliberately explicit: it is only
        # valid when startup supplied the process HTTP boundary.  A later
        # fetch must not discover or construct an arbitrary client.
        client = _runtime_value(self.runtime, "http_client")
        if client is None or (
            not callable(getattr(client, "get", None)) and not callable(client)
        ):
            raise DefinitionError(
                f"startup_http provider {self.binding.name!r} requires injected HTTP client"
            )

    def fetch(self, request: FetchRequest) -> DocumentArtifact:
        client = _transport_for(self.runtime, self.binding)
        getter = getattr(client, "get", None) if client is not None else None
        if not callable(getter) and not callable(client):
            raise ExecutionError(
                f"startup_http provider {self.binding.name!r} has no injected HTTP client"
            )
        try:
            response = (
                client(request.url)
                if callable(client) and not callable(getter)
                else getter(request.url)
            )
        except BaseException:
            raise WebSafetyError("provider_error") from None
        force_final_url: str | None = None
        if isinstance(response, str):
            response = DirectResponse(
                status_code=200,
                body=response.encode("utf-8"),
                url=request.url,
                final_url=request.url,
            )
            force_final_url = request.url
        return _safe_fetch(
            lambda: _artifact_from_response(
                self.binding,
                request,
                response,
                default_status=200,
                force_final_url=force_final_url,
            )
        )


StartupHttpProvider = StartupHTTPProvider
StartupHttp = StartupHTTPProvider


@dataclass(frozen=True, slots=True)
class CurlCffiProvider:
    binding: ProviderBinding
    runtime: object | None = None
    session: object | None = None

    def __post_init__(self) -> None:
        transport = (
            self.session
            or _runtime_value(self.runtime, "curl_session")
            or _transport_for(self.runtime, self.binding)
        )
        if transport is None or not callable(getattr(transport, "get_pinned", None)):
            raise DefinitionError(
                f"curl_cffi runtime cannot enforce pinned streaming for provider "
                f"{self.binding.name!r}"
            ) from None
        _curl_remote_dns_proxy(self.binding)

    def fetch(self, request: FetchRequest) -> DocumentArtifact:
        transport = (
            self.session
            or _runtime_value(self.runtime, "curl_session")
            or _transport_for(self.runtime, self.binding)
        )
        if transport is None or not callable(getattr(transport, "get_pinned", None)):
            raise DefinitionError(
                f"curl_cffi runtime cannot enforce pinned streaming for provider "
                f"{self.binding.name!r}"
            )
        response = _safe_fetch(
            lambda: bounded_manual_get(request, self.binding, transport=transport)
        )
        return _safe_fetch(
            lambda: _artifact_from_response(self.binding, request, response)
        )


def _curl_remote_dns_proxy(binding: ProviderBinding) -> bool:
    """Validate curl's proxy mode and report whether target DNS is remote."""

    return _direct_remote_dns_proxy(binding)


def _direct_remote_dns_proxy(binding: ProviderBinding) -> bool:
    """Validate a direct-provider proxy and report its target DNS mode."""

    proxy = binding.proxy
    if proxy is None:
        return False
    try:
        parsed = urlsplit(proxy)
        _ = parsed.port
    except (TypeError, ValueError):
        raise DefinitionError(
            f"invalid proxy configuration for provider {binding.name!r}"
        ) from None
    if (
        parsed.scheme not in {"http", "https", "socks5", "socks5h"}
        or not parsed.hostname
        or parsed.query
        or parsed.fragment
    ):
        raise DefinitionError(
            f"invalid proxy configuration for provider {binding.name!r}"
        )
    remote_dns = parsed.scheme in {"http", "https", "socks5h"}
    policy = TargetPolicy.from_value(binding.target_policy)
    if remote_dns and not policy.allow_remote_dns_proxy:
        raise DefinitionError(
            f"{binding.type} provider {binding.name!r} requires "
            "allow_remote_dns_proxy: true for this proxy mode"
        )
    return remote_dns


@dataclass(frozen=True, slots=True)
class TrafilaturaProvider:
    binding: ProviderBinding
    runtime: object | None = None

    def __post_init__(self) -> None:
        try:
            module = importlib.import_module("trafilatura")
        except Exception:
            raise DefinitionError(
                f"trafilatura dependency is unavailable for provider "
                f"{self.binding.name!r}"
            ) from None
        if not callable(getattr(module, "extract", None)):
            raise DefinitionError(
                f"trafilatura dependency is unavailable for provider "
                f"{self.binding.name!r}"
            )

    def fetch(self, request: FetchRequest) -> DocumentArtifact:
        internal = request
        if "raw_html" not in request.acceptable:
            internal = FetchRequest(
                url=request.url,
                mode=request.mode,
                acceptable=frozenset(set(request.acceptable) | {"raw_html"}),
                preferred=request.preferred,
                list_selector=request.list_selector,
                target_headers=request.target_headers,
                quality=request.quality,
                stage_deadline=request.stage_deadline,
            )
        response = _safe_fetch(
            lambda: bounded_manual_get(
                internal,
                self.binding,
                transport=_transport_for(self.runtime, self.binding),
            )
        )
        base = _safe_fetch(
            lambda: _artifact_from_response(self.binding, internal, response)
        )
        if request.mode != "article":
            # List and raw-feed requests are representation-constrained.  The
            # combined profile still performs only the one bounded fetch, but
            # must not run an article extractor on a raw winner.
            return _safe_fetch(
                lambda: _artifact_from_response(self.binding, request, response)
            )
        if base.raw_html is None:
            return _safe_fetch(
                lambda: _artifact_from_response(self.binding, request, response)
            )
        if not {"markdown", "text"}.intersection(request.acceptable):
            return _safe_fetch(
                lambda: _artifact_from_response(self.binding, request, response)
            )
        markdown = _trafilatura_markdown(base.raw_html, self.binding.options)
        if markdown is NO_MATCH:
            return NO_MATCH
        fields: dict[str, Any] = {
            "source_url": base.source_url,
            "final_url": base.final_url,
            "status_code": base.status_code,
            "content_type": base.content_type,
            "provider_instance": self.binding.name,
            "provider_type": self.binding.type,
            "capabilities": frozenset(),
        }
        if "raw_html" in request.acceptable:
            fields["raw_html"] = base.raw_html
            fields["capabilities"] = frozenset({"raw_html"})
        if markdown and "markdown" in request.acceptable:
            fields["markdown"] = markdown
            fields["capabilities"] = frozenset(
                set(fields["capabilities"]) | {"markdown"}
            )
        if markdown and "text" in request.acceptable:
            fields["text"] = _visible_text(markdown)
            fields["capabilities"] = frozenset(set(fields["capabilities"]) | {"text"})
        return DocumentArtifact(**fields)


TrafilaturaProviderProfile = TrafilaturaProvider
Trafilatura = TrafilaturaProvider


def _visible_text(value: str) -> str:
    try:
        soup = BeautifulSoup(value, "html.parser")
        return " ".join(soup.get_text(" ", strip=True).split())
    except Exception:
        return " ".join(value.split())


def _trafilatura_markdown(raw_html: str, options: Mapping[str, Any]) -> str | object:
    try:
        module = importlib.import_module("trafilatura")
    except Exception:
        return NO_MATCH
    try:
        result = module.extract(
            raw_html,
            output_format="markdown",
            include_tables=bool(options.get("include_tables", True)),
            include_comments=bool(options.get("include_comments", False)),
            favor_precision=bool(options.get("favor_precision", False)),
            favor_recall=bool(options.get("favor_recall", False)),
        )
    except Exception:
        return NO_MATCH
    if isinstance(result, str) and result.strip():
        return result.strip()
    return NO_MATCH


def _thaw_json(value: Any) -> Any:
    """Convert the immutable binding view to ordinary JSON-compatible values."""

    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_thaw_json(item) for item in value]
    return value


def firecrawl_actions(actions: tuple[Any, ...] | list[Any]) -> list[dict[str, Any]]:
    """Translate the validated Firecrawl action subset to vendor JSON."""

    translated: list[dict[str, Any]] = []
    for action in actions:
        if not isinstance(action, Mapping) or len(action) != 1:
            # Configuration loading validates this shape.  Keep direct
            # construction safe as well and avoid forwarding arbitrary JSON.
            raise DefinitionError("invalid Firecrawl action")
        kind, payload = next(iter(action.items()))
        if kind not in {"wait", "click", "write", "press", "scroll"}:
            raise DefinitionError(f"unsupported firecrawl action {kind!r}")
        if not isinstance(payload, Mapping):
            raise DefinitionError("invalid Firecrawl action payload")
        translated.append({"type": str(kind), **_thaw_json(payload)})
    return translated


def firecrawl_payload(
    binding: ProviderBinding, request: FetchRequest
) -> dict[str, Any]:
    """Build one Firecrawl v2 scrape request.

    The formats are deliberately selected from the request mode instead of
    asking the service for multiple operations.  ``rawHtml`` is the only
    DOM-preserving format accepted for list and raw-feed requests.
    """

    options = binding.options
    only_main_content = options.get("only_main_content", binding.only_main_content)
    wait_for = options.get("wait_for", binding.wait_for)
    wait_for_selector = options.get("wait_for_selector", binding.wait_for_selector)
    mobile = options.get("mobile", binding.mobile)
    actions = options.get("actions", binding.actions)
    formats = ["markdown", "rawHtml"] if request.mode == "article" else ["rawHtml"]
    if wait_for is None:
        wait_for = 0
    if (
        isinstance(wait_for, bool)
        or not isinstance(wait_for, (int, float))
        or wait_for < 0
    ):
        raise DefinitionError("Firecrawl wait_for must be a non-negative duration")
    wait_for_milliseconds = wait_for * 1000
    if not float(wait_for_milliseconds).is_integer():
        raise DefinitionError("Firecrawl wait_for must resolve to whole milliseconds")
    actions_payload = firecrawl_actions(actions)
    if wait_for_selector is not None:
        if not isinstance(wait_for_selector, str) or not wait_for_selector.strip():
            raise DefinitionError("Firecrawl wait_for_selector must be non-empty")
        actions_payload.insert(0, {"type": "wait", "selector": wait_for_selector})
    target_headers = merge_target_headers(request, binding)
    payload: dict[str, Any] = {
        "url": request.url,
        "formats": formats,
        "onlyMainContent": bool(only_main_content),
        "waitFor": int(wait_for_milliseconds),
        "mobile": bool(mobile),
        "actions": actions_payload,
        "maxAge": 0,
        "storeInCache": False,
        "skipTlsVerification": False,
    }
    if target_headers:
        payload["headers"] = target_headers
    return payload


def _service_transport(runtime: object | None, binding: ProviderBinding) -> Any:
    """Find the startup-injected service boundary for a remote provider."""

    candidates: list[Any] = []
    if runtime is not None:
        # Hosted providers must use the catalog's binding-specific transport
        # first.  In particular, the app-wide ``http_client`` is the trusted
        # compatibility boundary for ``startup_http`` only; it may be a
        # post-capable client but cannot enforce the hosted service's pinned
        # streaming policy.
        resolved = _transport_for(runtime, binding, binding_specific_only=True)
        if resolved is not None:
            candidates.append(resolved)
        candidates.append(runtime)
        for name in (
            "service",
            "service_client",
            "httpx_client",
            "transport",
        ):
            value = _runtime_value(runtime, name)
            if value is not None:
                candidates.append(value)
    for candidate in candidates:
        if candidate is None:
            continue
        if callable(getattr(candidate, "post_pinned", None)):
            return candidate
        if callable(getattr(candidate, "post", None)):
            return candidate
        if callable(getattr(candidate, "request", None)):
            return candidate
        if callable(candidate):
            return candidate
        if isinstance(candidate, Mapping) and (
            callable(candidate.get("post")) or callable(candidate.get("request"))
        ):
            return candidate
    return None


def _service_endpoint_url(binding: ProviderBinding, path: str) -> str:
    defaults = {
        "firecrawl": "https://api.firecrawl.dev",
        "crawl4ai": "http://localhost:11235",
        "jina_reader": "https://r.jina.ai",
        "zyte": "https://api.zyte.com",
        "bright_data": "https://api.brightdata.com",
    }
    base = binding.base_url or defaults.get(binding.type)
    if not base:
        raise DefinitionError(
            f"{binding.type} provider {binding.name!r} requires a service endpoint"
        )
    return f"{str(base).rstrip('/')}{path}"


def _service_headers(
    binding: ProviderBinding,
    *,
    auth_scheme: str = "bearer",
    adapter_headers: Mapping[str, str] | None = None,
) -> dict[str, str]:
    headers = {str(name): str(value) for name, value in binding.service_headers.items()}
    api_key = binding.options.get("api_key")
    if api_key is not None:
        if any(name.casefold() == "authorization" for name in headers):
            raise DefinitionError(
                f"provider {binding.name!r} cannot combine api_key with Authorization"
            )
        # ``api_key`` is either resolved by WebRuntimeCatalog or an explicit
        # test value when an adapter is constructed directly.
        if auth_scheme == "basic":
            encoded = base64.b64encode(f"{api_key}:".encode()).decode("ascii")
            headers["Authorization"] = f"Basic {encoded}"
        elif auth_scheme == "bearer":
            headers["Authorization"] = f"Bearer {api_key}"
        else:
            raise DefinitionError("unsupported service authentication scheme")
    for name, value in (adapter_headers or {}).items():
        normalized = str(name).casefold()
        for existing in tuple(headers):
            if existing.casefold() == normalized:
                del headers[existing]
        headers[str(name)] = str(value)
    try:
        return validate_target_headers(headers)
    except WebSafetyError:
        raise
    except Exception:
        raise WebSafetyError("provider_error") from None


def _retry_after_seconds(headers: Mapping[str, str]) -> float | None:
    """Parse a service Retry-After value without retaining header text."""

    value = _header(headers, "retry-after")
    if value is None:
        return None
    stripped = value.strip()
    if stripped.isdigit():
        try:
            seconds = float(stripped)
        except (ValueError, OverflowError):
            return None
        return seconds if math.isfinite(seconds) else None
    try:
        parsed = parsedate_to_datetime(stripped)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return max(
            0.0,
            (parsed.astimezone(UTC) - datetime.now(UTC)).total_seconds(),
        )
    except (TypeError, ValueError, OverflowError):
        return None


def _service_failure(status: int, headers: Mapping[str, str]) -> WebSafetyError:
    reason = "rate_limited" if status == 429 else "http_status"
    error = WebSafetyError(reason)
    # These are the only values the coordinator may retain.  Raw headers and
    # vendor bodies never cross this boundary.
    error.reason = reason
    error.status_code = status
    error.status_kind = "service"
    if status == 429:
        error.retry_after_seconds = _retry_after_seconds(headers)
    return error


def _json_from_response(
    response: Any,
) -> tuple[int, Mapping[str, str], Any, str | None]:
    """Detach and decode a service response without leaking vendor errors."""

    # A tiny fake boundary often returns the decoded object directly.  Mapping
    # response envelopes are handled here too, including ``json=...`` values,
    # so tests need not construct an HTTPX response object.
    if isinstance(response, Mapping):
        status_value = response.get("status_code", response.get("status", 200))
        status = int(status_value) if isinstance(status_value, int) else 200
        headers_value = response.get("headers", {})
        headers = (
            {str(key): str(item) for key, item in headers_value.items()}
            if isinstance(headers_value, Mapping)
            else {}
        )
        final_value = response.get("final_url", response.get("url"))
        final = None if final_value is None else str(final_value)
        if "json" in response:
            return status, headers, response["json"], final
        if "body" in response or "content" in response:
            raw = response.get("body", response.get("content", b""))
            if isinstance(raw, (Mapping, list, tuple)):
                return status, headers, raw, final
            if isinstance(raw, str):
                raw = raw.encode("utf-8")
            if isinstance(raw, (bytes, bytearray, memoryview)):
                try:
                    return (
                        status,
                        headers,
                        json.loads(bytes(raw).decode("utf-8")),
                        final,
                    )
                except (UnicodeError, ValueError, TypeError):
                    raise WebSafetyError("provider_error") from None
        if "data" in response or "results" in response or "success" in response:
            return status, headers, response, final
        if "markdown" in response or "html" in response or "rawHtml" in response:
            return status, headers, response, final
    if isinstance(response, tuple) and len(response) >= 3:
        status_value = response[0]
        status = int(status_value) if isinstance(status_value, int) else 200
        headers_value = response[1]
        headers = (
            {str(key): str(item) for key, item in headers_value.items()}
            if isinstance(headers_value, Mapping)
            else {}
        )
        raw = response[2]
        if isinstance(raw, Mapping):
            return status, headers, raw, None
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        if isinstance(raw, (bytes, bytearray, memoryview)):
            try:
                return status, headers, json.loads(bytes(raw).decode("utf-8")), None
            except (UnicodeError, ValueError, TypeError):
                raise WebSafetyError("provider_error") from None
    json_method = getattr(response, "json", None)
    if callable(json_method):
        try:
            value = json_method()
            status = int(
                getattr(response, "status_code", getattr(response, "status", 200)) or 0
            )
            headers = {
                str(key): str(item)
                for key, item in dict(getattr(response, "headers", {}) or {}).items()
            }
            final = getattr(response, "url", None) or getattr(
                response, "final_url", None
            )
            return status, headers, value, None if final is None else str(final)
        except Exception:
            raise WebSafetyError("provider_error") from None
    if isinstance(json_method, (Mapping, list, tuple)):
        status = int(
            getattr(response, "status_code", getattr(response, "status", 200)) or 0
        )
        headers = {
            str(key): str(item)
            for key, item in dict(getattr(response, "headers", {}) or {}).items()
        }
        final = getattr(response, "url", None) or getattr(response, "final_url", None)
        return status, headers, json_method, None if final is None else str(final)
    status, headers, body, final = _response_body(response)
    if not body:
        raise WebSafetyError("provider_error")
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeError, ValueError, TypeError):
        raise WebSafetyError("provider_error") from None
    return status, headers, value, final


def _post_service_json(
    binding: ProviderBinding,
    runtime: object | None,
    path: str,
    payload: Mapping[str, Any],
    *,
    service_headers: Mapping[str, str] | None = None,
    deadline: float | None = None,
) -> tuple[Mapping[str, Any], int, Mapping[str, str], str | None]:
    transport = _service_transport(runtime, binding)
    target = _service_endpoint_url(binding, path)
    try:
        response = bounded_service_post(
            target,
            binding,
            payload,
            headers=(
                _service_headers(binding)
                if service_headers is None
                else service_headers
            ),
            transport=transport,
            deadline=deadline,
        )
        try:
            status, headers, value, response_url = _json_from_response(response)
        except WebSafetyError:
            status, headers, _body, _response_url = _response_body(response)
            if status < 200 or status >= 300:
                raise _service_failure(status, headers) from None
            raise
    except (WebAttemptCancelled, WebStageDeadlineExpired):
        raise
    except WebSafetyError:
        raise
    except BaseException:
        raise WebSafetyError("provider_error") from None
    if status < 200 or status >= 300:
        raise _service_failure(status, headers)
    if not isinstance(value, Mapping):
        raise WebSafetyError("provider_error")
    return value, status, headers, response_url


def _remote_data(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    for key in ("data", "result"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            return value
    values = payload.get("results")
    if isinstance(values, (list, tuple)) and values and isinstance(values[0], Mapping):
        return values[0]
    return payload


def _first_string(mapping: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _remote_final_url(
    binding: ProviderBinding, request: FetchRequest, data: Mapping[str, Any]
) -> str:
    metadata = data.get("metadata")
    metadata_map = metadata if isinstance(metadata, Mapping) else {}

    def reported_url(mapping: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
        selected: str | None = None
        for key in keys:
            if key not in mapping:
                continue
            value = mapping[key]
            # Vendor models commonly serialize an optional redirect URL as
            # null.  A non-null authored value, however, must be a usable URL
            # rather than silently falling back to the submitted target.
            if value is None:
                continue
            if not isinstance(value, str) or not value.strip():
                raise WebSafetyError("provider_error")
            if selected is None:
                selected = value
        return selected

    data_reported = reported_url(
        data,
        (
            "redirected_url",
            "redirectedUrl",
            "final_url",
            "finalUrl",
            "url",
            "sourceURL",
            "source_url",
        ),
    )
    metadata_reported = reported_url(
        metadata_map,
        ("sourceURL", "sourceUrl", "finalUrl", "final_url", "url"),
    )
    reported = data_reported or metadata_reported
    candidate = reported or request.url
    try:
        return str(
            normalize_target_url(
                candidate, TargetPolicy.from_value(binding.target_policy)
            )
        )
    except WebSafetyError:
        raise
    except Exception:
        raise WebSafetyError("provider_error") from None


def _validate_remote_request_url(
    binding: ProviderBinding, request: FetchRequest
) -> FetchRequest:
    """Apply the local target URL boundary before submitting to a service."""

    try:
        normalized = normalize_target_url(
            request.url, TargetPolicy.from_value(binding.target_policy)
        )
    except WebSafetyError:
        raise
    except Exception:
        raise WebSafetyError("provider_error") from None
    if str(normalized) == request.url:
        return request
    # ``FetchRequest`` is frozen; reconstructing it retains the same quality
    # and mode contract while ensuring the service receives the canonical URL.
    return FetchRequest(
        url=str(normalized),
        mode=request.mode,
        acceptable=request.acceptable,
        preferred=request.preferred,
        list_selector=request.list_selector,
        target_headers=request.target_headers,
        quality=request.quality,
        stage_deadline=request.stage_deadline,
    )


def _remote_status(data: Mapping[str, Any], response_status: int) -> int | None:
    metadata = data.get("metadata")
    metadata_map = metadata if isinstance(metadata, Mapping) else {}
    for key in ("status_code", "statusCode", "status"):
        for mapping in (data, metadata_map):
            value = mapping.get(key)
            if isinstance(value, int) and not isinstance(value, bool):
                return value
    # A service success response is not proof of the target status.  Keep
    # ``null`` unless the remote adapter explicitly reports one.
    del response_status
    return None


def _remote_title(data: Mapping[str, Any]) -> str | None:
    metadata = data.get("metadata")
    metadata_map = metadata if isinstance(metadata, Mapping) else {}
    return _first_string(data, "title") or _first_string(metadata_map, "title")


def _remote_value(data: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.get(key)
        if value is not None:
            if isinstance(value, Mapping):
                nested = _first_string(
                    value,
                    "raw_markdown",
                    "rawMarkdown",
                    "fit_markdown",
                    "fitMarkdown",
                    "value",
                )
                if nested is not None:
                    return nested
            return value
    return None


def _remote_artifact(
    binding: ProviderBinding,
    request: FetchRequest,
    data: Mapping[str, Any],
    *,
    response_status: int,
    response_headers: Mapping[str, str],
    raw_html_keys: tuple[str, ...] = (
        "rawHtml",
        "raw_html",
        "html",
    ),
    raw_source_keys: tuple[str, ...] = ("body", "rawHtml"),
) -> DocumentArtifact:
    final_url = _remote_final_url(binding, request, data)
    status_code = _remote_status(data, response_status)
    metadata = data.get("metadata")
    metadata_map = dict(metadata) if isinstance(metadata, Mapping) else {}
    title = _remote_title(data)
    raw_html_value = _remote_value(data, *raw_html_keys)
    markdown_value = _remote_value(data, "markdown", "fit_markdown", "fitMarkdown")
    raw_html = raw_html_value if isinstance(raw_html_value, str) else None
    markdown = markdown_value if isinstance(markdown_value, str) else None
    # The HTTP response is a service JSON envelope; its Content-Type is not
    # the target document MIME.  Prefer a vendor-reported target MIME and
    # otherwise derive it from the representation we retained.
    content_type = _first_string(data, "content_type", "contentType")
    metadata_content_type = _first_string(
        metadata_map, "content_type", "contentType", "mimeType"
    )
    content_type = content_type or metadata_content_type
    if content_type:
        content_type = content_type.split(";", 1)[0].strip().lower()
    if request.mode == "raw_source":
        raw_source_value: Any = None
        for key in raw_source_keys:
            if key in data and data[key] is not None:
                # Raw feed eligibility is deliberately narrower than the
                # ordinary representation helper: nested/normalized values
                # are not the exact body returned by the crawl operation.
                raw_source_value = data[key]
                break
        raw_source: bytes | None
        if isinstance(raw_source_value, bytes):
            raw_source = raw_source_value
        elif isinstance(raw_source_value, str):
            raw_source = raw_source_value.encode("utf-8")
        else:
            raw_source = None
        if raw_source is not None and validate_raw_feed(raw_source) is not None:
            return DocumentArtifact(
                source_url=request.url,
                final_url=final_url,
                status_code=status_code,
                # The service envelope is JSON, so its Content-Type does not
                # describe the raw feed bytes.  Sniff the returned body.
                content_type=_sniff_content_type(raw_source, None),
                raw_source=raw_source,
                provider_instance=binding.name,
                provider_type=binding.type,
                capabilities=frozenset({"raw_source"}),
                metadata=metadata_map,
            )
        return DocumentArtifact(
            source_url=request.url,
            final_url=final_url,
            status_code=status_code,
            content_type=content_type or "text/html",
            provider_instance=binding.name,
            provider_type=binding.type,
            capabilities=frozenset(),
            metadata=metadata_map,
        )

    capabilities: set[str] = set()
    if "rendered_js" in binding.capabilities:
        capabilities.add("rendered_js")
    if raw_html is not None:
        capabilities.add("raw_html")
    if markdown is not None and request.mode == "article":
        capabilities.add("markdown")
    if content_type is None or content_type == "application/json":
        content_type = "text/html" if raw_html is not None else "text/markdown"
    return DocumentArtifact(
        source_url=request.url,
        final_url=final_url,
        status_code=status_code,
        content_type=content_type,
        raw_html=raw_html,
        markdown=markdown if request.mode == "article" else None,
        title=title,
        metadata=metadata_map,
        provider_instance=binding.name,
        provider_type=binding.type,
        capabilities=frozenset(capabilities),
    )


@dataclass(frozen=True, slots=True)
class FirecrawlProvider:
    """Synchronous Firecrawl v2 ``/scrape`` adapter."""

    binding: ProviderBinding
    runtime: object | None = None

    def __post_init__(self) -> None:
        if self.binding.options.get("api_version", "v2") != "v2":
            raise DefinitionError(
                f"firecrawl provider {self.binding.name!r} supports api_version v2 only"
            )

    def fetch(self, request: FetchRequest) -> DocumentArtifact:
        request = _validate_remote_request_url(self.binding, request)
        payload = firecrawl_payload(self.binding, request)
        data, status, headers, _response_url = _post_service_json(
            self.binding,
            self.runtime,
            "/v2/scrape",
            payload,
            deadline=request.stage_deadline,
        )
        if data.get("success") is False:
            raise WebSafetyError("provider_error")
        return _remote_artifact(
            self.binding,
            request,
            _remote_data(data),
            response_status=status,
            response_headers=headers,
            raw_html_keys=("rawHtml", "raw_html"),
        )


@dataclass(frozen=True, slots=True)
class Crawl4AIProvider:
    """Synchronous Crawl4AI ``/crawl`` adapter (no job polling)."""

    binding: ProviderBinding
    runtime: object | None = None

    def fetch(self, request: FetchRequest) -> DocumentArtifact:
        request = _validate_remote_request_url(self.binding, request)
        options = self.binding.options
        target_headers = merge_target_headers(request, self.binding)
        browser_headers = {
            name: value
            for name, value in target_headers.items()
            if name.casefold() != "cookie"
        }
        cookies = [
            {
                "type": "dict",
                "value": {
                    "name": str(name),
                    "value": str(value),
                    "url": request.url,
                },
            }
            for name, value in self.binding.cookies.items()
        ]
        browser_params: dict[str, Any] = {
            "browser_type": options.get("browser_type", "chromium"),
            "headless": bool(options.get("headless", True)),
        }
        if browser_headers:
            browser_params["headers"] = {
                "type": "dict",
                "value": browser_headers,
            }
        if cookies:
            browser_params["cookies"] = cookies
        payload: dict[str, Any] = {
            "urls": [request.url],
            "browser_config": {
                "type": "BrowserConfig",
                "params": browser_params,
            },
            "crawler_config": {
                "type": "CrawlerRunConfig",
                "params": {
                    "cache_mode": "bypass",
                    "stream": False,
                    "wait_until": options.get("wait_until", "domcontentloaded"),
                    "wait_for": options.get(
                        "wait_for_selector", self.binding.wait_for_selector
                    ),
                    "delay_before_return_html": options.get(
                        "delay_before_return_html", 0
                    ),
                },
            },
        }
        data, status, headers, _response_url = _post_service_json(
            self.binding,
            self.runtime,
            "/crawl",
            payload,
            deadline=request.stage_deadline,
        )
        if data.get("success") is False:
            raise WebSafetyError("provider_error")
        return _remote_artifact(
            self.binding,
            request,
            _remote_data(data),
            response_status=status,
            response_headers=headers,
            raw_html_keys=("rawHtml", "raw_html", "html"),
            raw_source_keys=("body", "rawHtml"),
        )


def _bounded_scalar(value: Any) -> str | int | float | bool | None:
    if isinstance(value, str):
        return value[:8192]
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    return None


@dataclass(frozen=True, slots=True)
class JinaReaderProvider:
    """JSON-mode Jina Reader adapter returning Markdown only."""

    binding: ProviderBinding
    runtime: object | None = None

    def fetch(self, request: FetchRequest) -> DocumentArtifact:
        if request.mode != "article" or "markdown" not in request.acceptable:
            raise WebSafetyError("missing_capability")
        request = _validate_remote_request_url(self.binding, request)
        options = self.binding.options
        adapter_headers: dict[str, str] = {"Accept": "application/json"}
        target_selector = options.get("target_selector")
        if isinstance(target_selector, str) and target_selector:
            adapter_headers["X-Target-Selector"] = target_selector
        remove_selectors = options.get("remove_selectors", ())
        if isinstance(remove_selectors, (tuple, list)) and remove_selectors:
            if not all(isinstance(item, str) and item for item in remove_selectors):
                raise DefinitionError("invalid Jina remove_selectors")
            adapter_headers["X-Remove-Selector"] = ",".join(remove_selectors)
        if options.get("with_generated_alt", False) is True:
            adapter_headers["X-With-Generated-Alt"] = "true"
        headers = _service_headers(
            self.binding,
            adapter_headers=adapter_headers,
        )
        payload, status, _headers, _response_url = _post_service_json(
            self.binding,
            self.runtime,
            "/",
            {"url": request.url},
            service_headers=headers,
            deadline=request.stage_deadline,
        )
        if payload.get("code") not in (None, 200):
            raise WebSafetyError("provider_error")
        data = _remote_data(payload)
        markdown = _first_string(data, "content", "markdown")
        title_value = _first_string(data, "title")
        title = None if title_value is None else title_value[:8192]
        final_url = _remote_final_url(self.binding, request, data)
        metadata: dict[str, Any] = {}
        for key in ("description", "publishedTime"):
            bounded = _bounded_scalar(data.get(key))
            if bounded is not None:
                metadata[key] = bounded
        return DocumentArtifact(
            source_url=request.url,
            final_url=final_url,
            status_code=_remote_status(data, status),
            content_type="text/markdown",
            markdown=markdown,
            title=title,
            metadata=metadata,
            provider_instance=self.binding.name,
            provider_type=self.binding.type,
            capabilities=(
                frozenset({"markdown"}) if markdown is not None else frozenset()
            ),
        )


JinaProvider = JinaReaderProvider


def zyte_actions(actions: tuple[Any, ...] | list[Any]) -> list[dict[str, Any]]:
    """Translate the closed Sluice action subset into Zyte actions."""

    translated: list[dict[str, Any]] = []
    for action in actions:
        if not isinstance(action, Mapping) or len(action) != 1:
            raise DefinitionError("invalid Zyte action")
        kind, raw_payload = next(iter(action.items()))
        if not isinstance(raw_payload, Mapping):
            raise DefinitionError("invalid Zyte action payload")
        payload = _thaw_json(raw_payload)
        if kind == "wait":
            milliseconds = payload.get("milliseconds")
            if (
                isinstance(milliseconds, bool)
                or not isinstance(milliseconds, int)
                or milliseconds < 0
            ):
                raise DefinitionError("invalid Zyte wait action")
            seconds = milliseconds / 1000
            translated.append(
                {
                    "action": "waitForTimeout",
                    "timeout": int(seconds) if seconds.is_integer() else seconds,
                }
            )
        elif kind in {"click", "write"}:
            selector = payload.get("selector")
            if not isinstance(selector, str) or not selector:
                raise DefinitionError(f"invalid Zyte {kind} action")
            item: dict[str, Any] = {
                "action": "click" if kind == "click" else "type",
                "selector": {"type": "css", "value": selector},
            }
            if kind == "write":
                text = payload.get("text")
                if not isinstance(text, str) or not text:
                    raise DefinitionError("invalid Zyte write action")
                item["text"] = text
            translated.append(item)
        elif kind == "press":
            key = payload.get("key")
            if not isinstance(key, str) or not key:
                raise DefinitionError("invalid Zyte press action")
            translated.append({"action": "keyPress", "key": key})
        elif kind == "scroll":
            x = payload.get("x")
            y = payload.get("y")
            if any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in (x, y)
            ):
                raise DefinitionError("invalid Zyte scroll action")
            translated.append({"action": "scrollTo", "x": x, "y": y})
        else:
            raise DefinitionError(f"unsupported Zyte action {kind!r}")
    return translated


def zyte_payload(binding: ProviderBinding, request: FetchRequest) -> dict[str, Any]:
    options = binding.options
    mode = options.get("mode", "auto")
    if mode not in {"auto", "http", "browser"}:
        raise DefinitionError("invalid Zyte mode")
    payload: dict[str, Any] = {"url": request.url}
    if mode == "http":
        payload["httpResponseBody"] = True
    else:
        payload["browserHtml"] = True
    automatic_content = options.get("automatic_content", True)
    if not isinstance(automatic_content, bool):
        raise DefinitionError("invalid Zyte automatic_content")
    if automatic_content:
        payload["pageContent"] = True
        if mode in {"http", "browser"}:
            payload["pageContentOptions"] = {
                "extractFrom": ("httpResponseBody" if mode == "http" else "browserHtml")
            }
    geolocation = options.get("geolocation")
    if geolocation is not None:
        payload["geolocation"] = geolocation
    actions = options.get("actions", binding.actions)
    translated_actions = zyte_actions(actions)
    if translated_actions:
        if mode == "http":
            raise DefinitionError("Zyte HTTP mode does not support actions")
        payload["actions"] = translated_actions

    target_headers = merge_target_headers(request, binding)
    target_headers = {
        name: value
        for name, value in target_headers.items()
        if name.casefold() != "cookie"
    }
    if target_headers:
        if mode == "http":
            payload["customHttpRequestHeaders"] = [
                {"name": name, "value": value} for name, value in target_headers.items()
            ]
        else:
            referers = {
                name: value
                for name, value in target_headers.items()
                if name.casefold() == "referer"
            }
            if len(referers) != len(target_headers):
                raise WebSafetyError("bad_url")
            if referers:
                payload["requestHeaders"] = {"referer": next(iter(referers.values()))}
    if binding.cookies:
        host = urlsplit(request.url).hostname
        if not host:
            raise WebSafetyError("bad_url")
        payload["requestCookies"] = [
            {"name": str(name), "value": str(value), "domain": host}
            for name, value in binding.cookies.items()
        ]
    return payload


def _zyte_target_headers(data: Mapping[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}
    raw = data.get("httpResponseHeaders")
    if not isinstance(raw, (tuple, list)):
        return headers
    for item in raw[:200]:
        if not isinstance(item, Mapping):
            continue
        name = item.get("name")
        value = item.get("value")
        if isinstance(name, str) and isinstance(value, str):
            headers[name] = value
    return headers


@dataclass(frozen=True, slots=True)
class ZyteProvider:
    """One-shot Zyte extraction adapter using Basic authentication."""

    binding: ProviderBinding
    runtime: object | None = None

    def fetch(self, request: FetchRequest) -> DocumentArtifact:
        request = _validate_remote_request_url(self.binding, request)
        payload, status, _response_headers, _response_url = _post_service_json(
            self.binding,
            self.runtime,
            "/v1/extract",
            zyte_payload(self.binding, request),
            service_headers=_service_headers(
                self.binding,
                auth_scheme="basic",
            ),
            deadline=request.stage_deadline,
        )
        final_url = _remote_final_url(self.binding, request, payload)
        target_status = _remote_status(payload, status)
        raw_html = _first_string(payload, "browserHtml")
        text: str | None = None
        target_headers = _zyte_target_headers(payload)
        content_type = _header(target_headers, "content-type")
        if content_type:
            content_type = content_type.split(";", 1)[0].strip().lower()
        encoded_body = payload.get("httpResponseBody")
        if encoded_body is not None:
            if not isinstance(encoded_body, str):
                raise WebSafetyError("provider_error")
            try:
                decoded = base64.b64decode(encoded_body, validate=True)
            except (binascii.Error, ValueError):
                raise WebSafetyError("provider_error") from None
            sniffed = _sniff_content_type(decoded, content_type)
            if sniffed in {"text/html", "application/xhtml+xml"}:
                raw_html = _decode(decoded, target_headers)
                content_type = sniffed
            else:
                text = _decode(decoded, target_headers)
                content_type = sniffed
        page_content = payload.get("pageContent")
        title: str | None = None
        metadata: dict[str, Any] = {}
        if isinstance(page_content, Mapping):
            text = _first_string(page_content, "itemMain") or text
            title_value = _first_string(page_content, "title", "headline")
            title = None if title_value is None else title_value[:8192]
            bounded: dict[str, Any] = {}
            for key in ("headline", "canonicalUrl", "itemMainXPath"):
                value = _bounded_scalar(page_content.get(key))
                if value is not None:
                    bounded[key] = value
            if bounded:
                metadata["page_content"] = bounded
        if content_type is None:
            content_type = "text/html" if raw_html is not None else "text/plain"
        capabilities: set[str] = set()
        if raw_html is not None:
            capabilities.add("raw_html")
        if text is not None:
            capabilities.add("text")
        return DocumentArtifact(
            source_url=request.url,
            final_url=final_url,
            status_code=target_status,
            content_type=content_type,
            raw_html=raw_html,
            text=text,
            title=title,
            metadata=metadata,
            provider_instance=self.binding.name,
            provider_type=self.binding.type,
            capabilities=frozenset(capabilities),
        )


@dataclass(frozen=True, slots=True)
class BrightDataProvider:
    """Synchronous Bright Data Web Unlocker adapter."""

    binding: ProviderBinding
    runtime: object | None = None

    def fetch(self, request: FetchRequest) -> DocumentArtifact:
        request = _validate_remote_request_url(self.binding, request)
        options = self.binding.options
        if options.get("async", False) is not False:
            raise DefinitionError("Bright Data async mode is unsupported")
        output = options.get("output", "markdown")
        if output not in {"html", "markdown"}:
            raise DefinitionError("invalid Bright Data output")
        zone = options.get("zone")
        if not isinstance(zone, str) or not zone:
            raise DefinitionError("Bright Data zone is required")
        payload: dict[str, Any] = {
            "url": request.url,
            "zone": zone,
            "format": "raw",
        }
        if output == "markdown":
            payload["data_format"] = "markdown"
        country = options.get("country")
        if country is not None:
            payload["country"] = country
        envelope, status, _headers, _response_url = _post_service_json(
            self.binding,
            self.runtime,
            "/request",
            payload,
            deadline=request.stage_deadline,
        )
        body = envelope.get("body")
        if not isinstance(body, str):
            raise WebSafetyError("provider_error")
        target_headers = envelope.get("headers")
        target_headers = (
            {str(name): str(value) for name, value in target_headers.items()}
            if isinstance(target_headers, Mapping)
            else {}
        )
        supplied_type = _header(target_headers, "content-type")
        content_type = (
            "text/markdown"
            if output == "markdown"
            else (
                supplied_type.split(";", 1)[0].strip().lower()
                if supplied_type
                else "text/html"
            )
        )
        final_url = _remote_final_url(self.binding, request, envelope)
        target_status = _remote_status(envelope, status)
        fields: dict[str, Any] = {
            "markdown": body if output == "markdown" else None,
            "raw_html": body if output == "html" else None,
        }
        return DocumentArtifact(
            source_url=request.url,
            final_url=final_url,
            status_code=target_status,
            content_type=content_type,
            provider_instance=self.binding.name,
            provider_type=self.binding.type,
            capabilities=frozenset(
                {"markdown" if output == "markdown" else "raw_html"}
            ),
            **fields,
        )


def build_httpx_provider(
    binding: ProviderBinding, runtime: object | None = None
) -> WebProvider:
    return HTTPXProvider(binding, runtime)


def build_startup_http_provider(
    binding: ProviderBinding, runtime: object | None = None
) -> WebProvider:
    return StartupHTTPProvider(binding, runtime)


def build_curl_cffi_provider(
    binding: ProviderBinding, runtime: object | None = None
) -> WebProvider:
    return CurlCffiProvider(binding, runtime)


def build_trafilatura_provider(
    binding: ProviderBinding, runtime: object | None = None
) -> WebProvider:
    return TrafilaturaProvider(binding, runtime)


def build_firecrawl_provider(
    binding: ProviderBinding, runtime: object | None = None
) -> WebProvider:
    return FirecrawlProvider(binding, runtime)


def build_crawl4ai_provider(
    binding: ProviderBinding, runtime: object | None = None
) -> WebProvider:
    return Crawl4AIProvider(binding, runtime)


def build_jina_reader_provider(
    binding: ProviderBinding, runtime: object | None = None
) -> WebProvider:
    return JinaReaderProvider(binding, runtime)


def build_zyte_provider(
    binding: ProviderBinding, runtime: object | None = None
) -> WebProvider:
    return ZyteProvider(binding, runtime)


def build_bright_data_provider(
    binding: ProviderBinding, runtime: object | None = None
) -> WebProvider:
    return BrightDataProvider(binding, runtime)


CORE_PROVIDER_BUILDERS = {
    "httpx": build_httpx_provider,
    "startup_http": build_startup_http_provider,
    "curl_cffi": build_curl_cffi_provider,
    "trafilatura": build_trafilatura_provider,
    "firecrawl": build_firecrawl_provider,
    "crawl4ai": build_crawl4ai_provider,
    "jina_reader": build_jina_reader_provider,
    "zyte": build_zyte_provider,
    "bright_data": build_bright_data_provider,
}
PROVIDER_BUILDERS = CORE_PROVIDER_BUILDERS


def build_provider(
    binding: ProviderBinding, runtime: object | None = None
) -> WebProvider:
    if not isinstance(binding, ProviderBinding):
        raise DefinitionError("provider binding must be a ProviderBinding")
    builder = CORE_PROVIDER_BUILDERS.get(binding.type)
    if builder is None:
        raise DefinitionError(
            f"unsupported web provider type {binding.type!r} for {binding.name!r}"
        )
    if binding.proxy is not None:
        if binding.type not in {"httpx", "curl_cffi", "trafilatura"}:
            raise DefinitionError(
                f"provider {binding.name!r} configures proxy, but direct provider "
                "proxy transport is unavailable"
            )
        _direct_remote_dns_proxy(binding)
    try:
        return builder(binding, runtime)
    except DefinitionError:
        raise
    except (TypeError, ValueError) as error:
        raise DefinitionError(f"invalid provider {binding.name!r}: {error}") from error


def build_extractor(
    binding: ExtractorBinding | str, options: Mapping[str, Any] | None = None
):
    try:
        return _build_extractor(binding, options)
    except (ExecutionError, TypeError, ValueError) as error:
        raise DefinitionError(f"unsupported web extractor {binding!r}") from error


__all__ = [
    "CORE_PROVIDER_BUILDERS",
    "PROVIDER_BUILDERS",
    "BrightDataProvider",
    "Crawl4AIProvider",
    "CurlCffiProvider",
    "FirecrawlProvider",
    "HTTPXProvider",
    "HttpxProvider",
    "JinaProvider",
    "JinaReaderProvider",
    "StartupHTTPProvider",
    "StartupHttp",
    "StartupHttpProvider",
    "Trafilatura",
    "TrafilaturaProvider",
    "TrafilaturaProviderProfile",
    "WebProvider",
    "ZyteProvider",
    "build_bright_data_provider",
    "build_crawl4ai_provider",
    "build_curl_cffi_provider",
    "build_extractor",
    "build_firecrawl_provider",
    "build_httpx_provider",
    "build_jina_reader_provider",
    "build_provider",
    "build_startup_http_provider",
    "build_trafilatura_provider",
    "build_zyte_provider",
    "firecrawl_actions",
    "firecrawl_payload",
    "zyte_actions",
    "zyte_payload",
]
