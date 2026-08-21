"""Fixed startup-bound web provider adapters."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import importlib
import re
from typing import Any, Protocol
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

from ..errors import DefinitionError, ExecutionError, WebSafetyError
from .contracts import (
    DocumentArtifact,
    ExtractorBinding,
    FetchRequest,
    NO_MATCH,
    ProviderBinding,
)
from .extractors import build_extractor as _build_extractor
from .http import DirectResponse, bounded_manual_get
from .url import TargetPolicy, normalize_target_url


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


def _transport_for(runtime: object, binding: ProviderBinding) -> Any:
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
        return response.status_code, response.headers, response.body, response.final_url or response.url
    if isinstance(response, Mapping):
        raw = response.get("body", response.get("content", b""))
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        return (
            int(response.get("status_code", response.get("status", 200)) or 0),
            {str(k): str(v) for k, v in dict(response.get("headers", {})).items()},
            bytes(raw or b""),
            None if response.get("final_url", response.get("url")) is None else str(response.get("final_url", response.get("url"))),
        )
    status = int(getattr(response, "status_code", getattr(response, "status", 200)) or 0)
    headers = {str(k): str(v) for k, v in dict(getattr(response, "headers", {}) or {}).items()}
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
    match = re.search(r"charset\s*=\s*[\"']?([^;\"'\s]+)", _header(headers, "content-type") or "", flags=re.I)
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
    if prefix.startswith(b"<") and re.match(rb"<\s*(?:!doctype\s+html|html|article|main|body|div|p)\b", prefix):
        return "text/html"
    return "text/plain"


def _allowed_representation(request: FetchRequest, content_type: str) -> str | None:
    if request.mode == "raw_source":
        return "raw_source" if "raw_source" in request.acceptable else None
    if content_type in {"application/xml", "text/xml", "application/rss+xml", "application/atom+xml"} and "raw_source" in request.acceptable:
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
        response = _safe_fetch(lambda: bounded_manual_get(request, self.binding, transport=_transport_for(self.runtime, self.binding)))
        return _safe_fetch(lambda: _artifact_from_response(self.binding, request, response))


HttpxProvider = HTTPXProvider


@dataclass(frozen=True, slots=True)
class StartupHTTPProvider:
    binding: ProviderBinding
    runtime: object | None = None

    def __post_init__(self) -> None:
        if self.binding.options.get("trusted") is not True:
            raise DefinitionError(f"startup_http provider {self.binding.name!r} requires trusted: true")
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
            raise ExecutionError(f"startup_http provider {self.binding.name!r} has no injected HTTP client")
        try:
            response = client(request.url) if callable(client) and not callable(getter) else getter(request.url)
        except BaseException:
            raise WebSafetyError("provider_error") from None
        force_final_url: str | None = None
        if isinstance(response, str):
            response = DirectResponse(status_code=200, body=response.encode("utf-8"), url=request.url, final_url=request.url)
            force_final_url = request.url
        return _safe_fetch(
            lambda: _artifact_from_response(
                self.binding, request, response, default_status=200, force_final_url=force_final_url
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
        return _safe_fetch(lambda: _artifact_from_response(self.binding, request, response))


def _curl_remote_dns_proxy(binding: ProviderBinding) -> bool:
    """Validate curl's proxy mode and report whether target DNS is remote."""

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
            f"curl_cffi provider {binding.name!r} requires "
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
            internal = FetchRequest(url=request.url, mode=request.mode, acceptable=frozenset(set(request.acceptable) | {"raw_html"}), preferred=request.preferred, list_selector=request.list_selector, target_headers=request.target_headers, quality=request.quality, stage_deadline=request.stage_deadline)
        response = _safe_fetch(lambda: bounded_manual_get(internal, self.binding, transport=_transport_for(self.runtime, self.binding)))
        base = _safe_fetch(lambda: _artifact_from_response(self.binding, internal, response))
        if request.mode != "article":
            # List and raw-feed requests are representation-constrained.  The
            # combined profile still performs only the one bounded fetch, but
            # must not run an article extractor on a raw winner.
            return _safe_fetch(lambda: _artifact_from_response(self.binding, request, response))
        if base.raw_html is None:
            return _safe_fetch(lambda: _artifact_from_response(self.binding, request, response))
        if not {"markdown", "text"}.intersection(request.acceptable):
            return _safe_fetch(lambda: _artifact_from_response(self.binding, request, response))
        markdown = _trafilatura_markdown(base.raw_html, self.binding.options)
        if markdown is NO_MATCH:
            return NO_MATCH
        fields: dict[str, Any] = {"source_url": base.source_url, "final_url": base.final_url, "status_code": base.status_code, "content_type": base.content_type, "provider_instance": self.binding.name, "provider_type": self.binding.type, "capabilities": frozenset()}
        if "raw_html" in request.acceptable:
            fields["raw_html"] = base.raw_html
            fields["capabilities"] = frozenset({"raw_html"})
        if markdown and "markdown" in request.acceptable:
            fields["markdown"] = markdown
            fields["capabilities"] = frozenset(set(fields["capabilities"]) | {"markdown"})
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
        result = module.extract(raw_html, output_format="markdown", include_tables=bool(options.get("include_tables", True)), include_comments=bool(options.get("include_comments", False)), favor_precision=bool(options.get("favor_precision", False)), favor_recall=bool(options.get("favor_recall", False)))
    except Exception:
        return NO_MATCH
    if isinstance(result, str) and result.strip():
        return result.strip()
    return NO_MATCH


def build_httpx_provider(binding: ProviderBinding, runtime: object | None = None) -> WebProvider:
    return HTTPXProvider(binding, runtime)


def build_startup_http_provider(binding: ProviderBinding, runtime: object | None = None) -> WebProvider:
    return StartupHTTPProvider(binding, runtime)


def build_curl_cffi_provider(binding: ProviderBinding, runtime: object | None = None) -> WebProvider:
    return CurlCffiProvider(binding, runtime)


def build_trafilatura_provider(binding: ProviderBinding, runtime: object | None = None) -> WebProvider:
    return TrafilaturaProvider(binding, runtime)


CORE_PROVIDER_BUILDERS = {
    "httpx": build_httpx_provider,
    "startup_http": build_startup_http_provider,
    "curl_cffi": build_curl_cffi_provider,
    "trafilatura": build_trafilatura_provider,
}
PROVIDER_BUILDERS = CORE_PROVIDER_BUILDERS


def build_provider(binding: ProviderBinding, runtime: object | None = None) -> WebProvider:
    if not isinstance(binding, ProviderBinding):
        raise DefinitionError("provider binding must be a ProviderBinding")
    builder = CORE_PROVIDER_BUILDERS.get(binding.type)
    if builder is None:
        raise DefinitionError(f"unsupported web provider type {binding.type!r} for {binding.name!r}")
    if binding.proxy is not None and binding.type != "curl_cffi":
        raise DefinitionError(
            f"provider {binding.name!r} configures proxy, but direct provider "
            "proxy transport is unavailable"
        )
    if binding.type == "curl_cffi":
        _curl_remote_dns_proxy(binding)
    try:
        return builder(binding, runtime)
    except DefinitionError:
        raise
    except (TypeError, ValueError) as error:
        raise DefinitionError(f"invalid provider {binding.name!r}: {error}") from error


def build_extractor(binding: ExtractorBinding | str, options: Mapping[str, Any] | None = None):
    try:
        return _build_extractor(binding, options)
    except (ExecutionError, TypeError, ValueError) as error:
        raise DefinitionError(f"unsupported web extractor {binding!r}") from error


__all__ = ["CORE_PROVIDER_BUILDERS", "PROVIDER_BUILDERS", "CurlCffiProvider", "HTTPXProvider", "HttpxProvider", "StartupHTTPProvider", "StartupHttpProvider", "StartupHttp", "TrafilaturaProvider", "TrafilaturaProviderProfile", "Trafilatura", "WebProvider", "build_curl_cffi_provider", "build_extractor", "build_httpx_provider", "build_provider", "build_startup_http_provider", "build_trafilatura_provider"]
