"""Strict YAML authoring and immutable expansion for web bindings.

The loader in :mod:`sluice.config` owns the definition graph.  This module is
deliberately limited to the web authoring model and to pure expansion into the
typed contracts consumed by later runtime tasks.  Environment references are
validated but never resolved into any retained value.
"""

from __future__ import annotations

import ipaddress
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from types import MappingProxyType
from urllib.parse import urlsplit, urlunsplit

from ..errors import DefinitionError
from .contracts import ExtractorBinding, ProviderBinding, QualityPolicy, WebBinding

_ENV_REFERENCE = re.compile(r"^env:[A-Z_][A-Z0-9_]*$")
_DURATION = re.compile(r"^(0|[1-9][0-9]*)(s|m|h)$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_BCP47 = re.compile(r"^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$")

_PROVIDER_TYPES = frozenset(
    {
        "httpx",
        "curl_cffi",
        "trafilatura",
        "crawl4ai",
        "firecrawl",
        "jina_reader",
        "zyte",
        "bright_data",
        "startup_http",
    }
)
_EXTRACTOR_TYPES = frozenset(
    {"identity", "trafilatura", "newspaper4k", "resiliparse", "beautifulsoup"}
)
_COMMON_PROVIDER_FIELDS = frozenset(
    {
        "type",
        "base_url",
        "timeout",
        "connect_timeout",
        "max_body_bytes",
        "transport_retries",
        "concurrency",
        "cooldown",
        "service_headers",
        "target_headers",
        "target_cookies",
        "proxy",
        "allow_remote_dns_proxy",
        "target_allow_hosts",
        "target_allow_cidrs",
    }
)
_PROVIDER_FIELDS: dict[str, frozenset[str]] = {
    "httpx": frozenset(),
    "curl_cffi": frozenset({"impersonate", "http_version"}),
    "trafilatura": frozenset({"include_tables", "include_comments"}),
    "crawl4ai": frozenset(
        {
            "api_key",
            "browser_type",
            "headless",
            "wait_until",
            "wait_for_selector",
            "delay_before_return_html",
        }
    ),
    "firecrawl": frozenset(
        {
            "api_key",
            "api_version",
            "only_main_content",
            "wait_for_selector",
            "wait_for",
            "mobile",
            "actions",
        }
    ),
    "jina_reader": frozenset(
        {"api_key", "target_selector", "with_generated_alt", "remove_selectors"}
    ),
    "zyte": frozenset(
        {"api_key", "mode", "geolocation", "actions", "automatic_content"}
    ),
    "bright_data": frozenset({"api_key", "zone", "output", "country", "async"}),
    "startup_http": frozenset({"trusted"}),
}
_DEFAULTS: dict[str, tuple[str, str, int]] = {
    "httpx": ("30s", "10s", 1),
    "curl_cffi": ("30s", "10s", 1),
    "trafilatura": ("10s", "10s", 1),
    "crawl4ai": ("120s", "10s", 0),
    "firecrawl": ("60s", "10s", 0),
    "jina_reader": ("30s", "10s", 1),
    "zyte": ("60s", "10s", 0),
    "bright_data": ("60s", "10s", 0),
    "startup_http": ("30s", "10s", 0),
}
_CAPABILITIES: dict[str, frozenset[str]] = {
    "httpx": frozenset({"raw_html", "raw_source", "text"}),
    "curl_cffi": frozenset({"raw_html", "raw_source", "text"}),
    "trafilatura": frozenset({"raw_html", "text", "markdown"}),
    "crawl4ai": frozenset({"raw_html", "raw_source", "markdown", "rendered_js"}),
    "firecrawl": frozenset({"raw_html", "raw_source", "markdown", "rendered_js"}),
    "jina_reader": frozenset({"markdown"}),
    "zyte": frozenset({"raw_html", "markdown", "text"}),
    "bright_data": frozenset({"raw_html", "markdown", "text"}),
    "startup_http": frozenset({"raw_html", "raw_source", "text"}),
}

_WEB_TASKS = frozenset(
    {
        "sluice.tasks.fetch_webpage",
        "sluice.tasks.extract_content",
        "sluice.tasks.to_markdown",
        "sluice.tasks.to_text",
    }
)
_WEB_SOURCES = frozenset({"sluice.sources.webpage", "sluice.sources.rss"})


def _mapping(value: object, label: str) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise DefinitionError(f"{label} must be a mapping")
    if any(not isinstance(key, str) for key in value):
        raise DefinitionError(f"{label} keys must be strings")
    return dict(value)


def _check_keys(
    spec: Mapping[str, object], allowed: set[str] | frozenset[str], label: str
) -> None:
    unknown = set(spec) - set(allowed)
    if unknown:
        raise DefinitionError(f"unknown keys in {label}: {sorted(unknown)}")


def _name(value: object, label: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise DefinitionError(
            f"{label} must be an ASCII identifier of at most 128 characters"
        )
    return value


def _required_string(spec: Mapping[str, object], key: str, label: str) -> str:
    value = spec.get(key)
    if not isinstance(value, str) or not value.strip():
        raise DefinitionError(f"{label}.{key} must be a non-empty string")
    return value


def _selector(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DefinitionError(f"{label} must be a non-empty CSS selector")
    selector = value.strip()
    stack: list[str] = []
    quote: str | None = None
    escaped = False
    for character in selector:
        if escaped:
            escaped = False
            continue
        if character == "\\":
            escaped = True
            continue
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in {"'", '"'}:
            quote = character
        elif character in "[(":
            stack.append(character)
        elif character in ")]":
            expected = "(" if character == ")" else "["
            if not stack or stack.pop() != expected:
                raise DefinitionError(
                    f"{label} must be a structurally valid CSS selector"
                )
        elif character in "{};" or ord(character) < 0x20:
            raise DefinitionError(f"{label} must be a structurally valid CSS selector")
    if quote is not None or stack or escaped:
        raise DefinitionError(f"{label} must be a structurally valid CSS selector")
    return selector


def _boolean(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise DefinitionError(f"{label} must be a boolean")
    return value


def _integer(value: object, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise DefinitionError(f"{label} must be an integer at least {minimum}")
    return value


def _duration(value: object, label: str, *, allow_zero: bool = False) -> timedelta:
    if not isinstance(value, str):
        raise DefinitionError(f"{label} must be a duration string")
    match = _DURATION.fullmatch(value)
    if match is None or (not allow_zero and match.group(1) == "0"):
        qualifier = "non-negative" if allow_zero else "positive"
        raise DefinitionError(f"{label} must be a {qualifier} whole duration")
    amount = int(match.group(1))
    multiplier = {"s": 1, "m": 60, "h": 3600}[match.group(2)]
    try:
        return timedelta(seconds=amount * multiplier)
    except OverflowError as error:
        raise DefinitionError(f"{label} is too large") from error


def _duration_seconds(value: timedelta) -> int:
    return int(value.total_seconds())


def _env_reference(value: object, label: str, *, require_present: bool = False) -> str:
    if not isinstance(value, str) or _ENV_REFERENCE.fullmatch(value) is None:
        raise DefinitionError(f"{label} must be an exact env:NAME reference")
    # Presence is intentionally checked by the runtime catalog at startup,
    # not by definition loading.  Retained terminal snapshots may legitimately
    # refer to a secret that is no longer present until an explicit rebuild.
    return value


def _string_list(
    value: object, label: str, *, non_empty: bool = False, unique: bool = False
) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise DefinitionError(f"{label} must be a list of strings")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or (non_empty and not item.strip()):
            raise DefinitionError(f"{label} must contain non-empty strings")
        result.append(item)
    if unique and len(set(result)) != len(result):
        raise DefinitionError(f"{label} must not contain duplicates")
    return tuple(result)


def _freeze(value: object, active: set[int] | None = None) -> object:
    active = set() if active is None else active
    if isinstance(value, Mapping):
        marker = id(value)
        if marker in active:
            raise DefinitionError("recursive web configuration")
        active.add(marker)
        try:
            if any(not isinstance(k, str) for k in value):
                raise DefinitionError("web configuration object keys must be strings")
            return MappingProxyType({k: _freeze(v, active) for k, v in value.items()})
        finally:
            active.remove(marker)
    if isinstance(value, (list, tuple)):
        marker = id(value)
        if marker in active:
            raise DefinitionError("recursive web configuration")
        active.add(marker)
        try:
            return tuple(_freeze(item, active) for item in value)
        finally:
            active.remove(marker)
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise DefinitionError("web configuration numbers must be finite")
        return value
    raise DefinitionError(f"web configuration value is not JSON-compatible: {value!r}")


@dataclass(frozen=True, slots=True)
class WebRegistryConfig:
    """Frozen authored web registry retained on a loaded Definition."""

    providers: Mapping[str, ProviderBinding] = field(default_factory=dict)
    fetch_chains: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    extractors: Mapping[str, ExtractorBinding] = field(default_factory=dict)
    extract_chains: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    version: int = 1

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError("web configuration version must be one")
        providers = dict(self.providers)
        fetch_chains = dict(self.fetch_chains)
        extractors = dict(self.extractors)
        extract_chains = dict(self.extract_chains)
        if any(
            not isinstance(k, str) or not isinstance(v, ProviderBinding)
            for k, v in providers.items()
        ):
            raise TypeError("providers must contain ProviderBinding values")
        if any(
            not isinstance(k, str) or not isinstance(v, ExtractorBinding)
            for k, v in extractors.items()
        ):
            raise TypeError("extractors must contain ExtractorBinding values")
        object.__setattr__(self, "providers", MappingProxyType(providers))
        object.__setattr__(
            self,
            "fetch_chains",
            MappingProxyType({k: tuple(v) for k, v in fetch_chains.items()}),
        )
        object.__setattr__(self, "extractors", MappingProxyType(extractors))
        object.__setattr__(
            self,
            "extract_chains",
            MappingProxyType({k: tuple(v) for k, v in extract_chains.items()}),
        )


def _provider_base_url(spec: Mapping[str, object], typ: str, label: str) -> str | None:
    defaults = {
        "firecrawl": "https://api.firecrawl.dev",
        "jina_reader": "https://r.jina.ai",
        "zyte": "https://api.zyte.com",
        "bright_data": "https://api.brightdata.com",
    }
    authored = spec.get("base_url")
    if typ in {"httpx", "curl_cffi", "trafilatura", "startup_http"}:
        if authored is not None:
            raise DefinitionError(f"{label}.base_url is not accepted for {typ}")
        return None
    if typ == "crawl4ai" and not isinstance(authored, str):
        raise DefinitionError(f"{label}.base_url is required for crawl4ai")
    value = authored if authored is not None else defaults.get(typ)
    if not isinstance(value, str) or not value.strip():
        raise DefinitionError(f"{label}.base_url must be a URL")
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError as error:
        raise DefinitionError(f"{label}.base_url must be a URL") from error
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise DefinitionError(
            f"{label}.base_url must be an absolute HTTP(S) URL without credentials"
        )
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


# Shared by the load-time literal check and the cross-origin redirect strip.
SENSITIVE_HEADER_PARTS = (
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "api-key",
    "apikey",
    "token",
    "secret",
    "signature",
    "subscription-key",
    "access-key",
    "x-auth",
    "credential",
)


def is_sensitive_header_name(name: str) -> bool:
    lowered = name.casefold()
    return any(part in lowered for part in SENSITIVE_HEADER_PARTS)


def decode_headers(
    value: object, label: str, *, cookies: bool = False
) -> Mapping[str, object]:
    spec = _mapping(value, label)
    result: dict[str, object] = {}
    seen_names: set[str] = set()
    for key, item in spec.items():
        if not isinstance(key, str) or not key.strip():
            raise DefinitionError(f"{label} names must be non-empty strings")
        normalized_name = key.casefold()
        if normalized_name in seen_names:
            raise DefinitionError(
                f"{label} contains duplicate case-insensitive header names"
            )
        seen_names.add(normalized_name)
        sensitive = cookies or is_sensitive_header_name(key)
        if sensitive:
            result[key] = _env_reference(item, f"{label}.{key}")
        elif not isinstance(item, str):
            raise DefinitionError(f"{label}.{key} must be a string")
        else:
            result[key] = item
    return result


def _actions(value: object, typ: str, label: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, (list, tuple)):
        raise DefinitionError(f"{label} must be a list")
    allowed = {
        "firecrawl": {"wait", "click", "write", "press", "scroll"},
        "zyte": {"wait", "click", "write", "press", "scroll"},
    }[typ]
    result: list[Mapping[str, object]] = []
    for index, raw in enumerate(value):
        action = _mapping(raw, f"{label}[{index}]")
        if len(action) != 1:
            raise DefinitionError(
                f"{label}[{index}] must contain exactly one action kind"
            )
        kind, payload = next(iter(action.items()))
        if kind not in allowed:
            raise DefinitionError(f"unsupported {typ} action {kind!r}")
        body = _mapping(payload, f"{label}[{index}].{kind}")
        if typ == "firecrawl":
            fields: dict[str, set[str]] = {
                "wait": {"milliseconds"},
                "click": {"selector"},
                "write": {"text"},
                "press": {"key"},
                "scroll": {"direction"},
            }
            _check_keys(body, fields[kind], f"{label}[{index}].{kind}")
            if kind == "wait":
                _integer(
                    body.get("milliseconds"), f"{label}[{index}].wait.milliseconds"
                )
            elif kind in {"click", "press", "write"}:
                _required_string(
                    body,
                    "selector"
                    if kind == "click"
                    else "key"
                    if kind == "press"
                    else "text",
                    f"{label}[{index}].{kind}",
                )
            elif body.get("direction") not in {"up", "down"}:
                raise DefinitionError(
                    f"{label}[{index}].scroll.direction must be up or down"
                )
        else:
            fields = {
                "wait": {"milliseconds"},
                "click": {"selector"},
                "write": {"selector", "text"},
                "press": {"key"},
                "scroll": {"x", "y"},
            }
            _check_keys(body, fields[kind], f"{label}[{index}].{kind}")
            if kind == "wait":
                _integer(
                    body.get("milliseconds"), f"{label}[{index}].wait.milliseconds"
                )
            elif kind == "scroll":
                _integer(body.get("x"), f"{label}[{index}].scroll.x")
                _integer(body.get("y"), f"{label}[{index}].scroll.y")
            elif kind == "click":
                _required_string(body, "selector", f"{label}[{index}].click")
            elif kind == "write":
                _required_string(body, "selector", f"{label}[{index}].write")
                _required_string(body, "text", f"{label}[{index}].write")
            elif kind == "press":
                _required_string(body, "key", f"{label}[{index}].press")
        result.append(_freeze({kind: body}))  # type: ignore[arg-type]
    return tuple(result)


def _decode_provider(name: str, raw: object) -> ProviderBinding:
    label = f"web.providers.{name}"
    spec = _mapping(raw, label)
    typ = _required_string(spec, "type", label)
    if typ not in _PROVIDER_TYPES:
        raise DefinitionError(f"{label}.type is unsupported: {typ!r}")
    _check_keys(spec, _COMMON_PROVIDER_FIELDS | _PROVIDER_FIELDS[typ], label)

    timeout_default, connect_default, retries_default = _DEFAULTS[typ]
    timeout = _duration(spec.get("timeout", timeout_default), f"{label}.timeout")
    connect_timeout = _duration(
        spec.get("connect_timeout", connect_default), f"{label}.connect_timeout"
    )
    if connect_timeout > timeout:
        raise DefinitionError(f"{label}.connect_timeout cannot exceed timeout")
    max_body_bytes = _integer(
        spec.get("max_body_bytes", 10_485_760), f"{label}.max_body_bytes", 1
    )
    if max_body_bytes > 10_485_760:
        raise DefinitionError(f"{label}.max_body_bytes cannot exceed 10485760")
    concurrency_default = 1 if typ == "startup_http" else 8
    concurrency = _integer(
        spec.get("concurrency", concurrency_default), f"{label}.concurrency", 1
    )
    cooldown = _duration(
        spec.get("cooldown", "60s"), f"{label}.cooldown", allow_zero=True
    )
    retries = _integer(
        spec.get("transport_retries", retries_default), f"{label}.transport_retries"
    )
    service_headers = decode_headers(
        spec.get("service_headers"), f"{label}.service_headers"
    )
    target_headers = decode_headers(
        spec.get("target_headers"), f"{label}.target_headers"
    )
    cookies = decode_headers(
        spec.get("target_cookies"), f"{label}.target_cookies", cookies=True
    )
    proxy = spec.get("proxy")
    if proxy is not None:
        if isinstance(proxy, str) and _ENV_REFERENCE.fullmatch(proxy):
            proxy = _env_reference(proxy, f"{label}.proxy")
        elif isinstance(proxy, str):
            try:
                parsed_proxy = urlsplit(proxy)
                _ = parsed_proxy.port
            except ValueError as error:
                raise DefinitionError(
                    f"{label}.proxy must be a URL or env reference"
                ) from error
            if (
                parsed_proxy.username
                or parsed_proxy.password
                or parsed_proxy.query
                or parsed_proxy.fragment
                or "?" in proxy
                or "#" in proxy
                or parsed_proxy.scheme not in {"http", "https", "socks5", "socks5h"}
                or not parsed_proxy.netloc
            ):
                raise DefinitionError(
                    f"{label}.proxy must be credential-free or an env reference"
                )
        else:
            raise DefinitionError(f"{label}.proxy must be a URL or env reference")
    allow_remote_dns_proxy = _boolean(
        spec.get("allow_remote_dns_proxy", False), f"{label}.allow_remote_dns_proxy"
    )
    allow_hosts = _string_list(
        spec.get("target_allow_hosts", ()),
        f"{label}.target_allow_hosts",
        non_empty=True,
        unique=True,
    )
    allow_cidrs = _string_list(
        spec.get("target_allow_cidrs", ()),
        f"{label}.target_allow_cidrs",
        non_empty=True,
        unique=True,
    )
    for cidr in allow_cidrs:
        try:
            ipaddress.ip_network(cidr, strict=False)
        except ValueError as error:
            raise DefinitionError(
                f"{label}.target_allow_cidrs contains an invalid CIDR"
            ) from error
    base_url = _provider_base_url(spec, typ, label)

    options: dict[str, object] = {
        "timeout": _duration_seconds(timeout),
        "connect_timeout": _duration_seconds(connect_timeout),
        "max_body_bytes": max_body_bytes,
        "transport_retries": retries,
        "concurrency": concurrency,
        "cooldown": _duration_seconds(cooldown),
        "service_headers": service_headers,
        "target_headers": target_headers,
        "target_cookies": cookies,
        "proxy": proxy,
        "allow_remote_dns_proxy": allow_remote_dns_proxy,
        "target_allow_hosts": allow_hosts,
        "target_allow_cidrs": allow_cidrs,
    }
    if base_url is not None:
        options["base_url"] = base_url

    only_main_content = False
    wait_for_selector = None
    wait_for: object = None
    mobile = False
    actions: tuple[Mapping[str, object], ...] = ()
    capabilities = _CAPABILITIES[typ]
    if typ == "curl_cffi":
        impersonate = spec.get("impersonate", "chrome")
        if not isinstance(impersonate, str) or not impersonate.strip():
            raise DefinitionError(f"{label}.impersonate must be a non-empty string")
        http_version = spec.get("http_version", "auto")
        if http_version not in {"auto", "2", "3"}:
            raise DefinitionError(f"{label}.http_version must be auto, 2, or 3")
        options.update({"impersonate": impersonate, "http_version": http_version})
    elif typ == "trafilatura":
        include_tables = _boolean(
            spec.get("include_tables", True), f"{label}.include_tables"
        )
        include_comments = _boolean(
            spec.get("include_comments", False), f"{label}.include_comments"
        )
        options.update(
            {"include_tables": include_tables, "include_comments": include_comments}
        )
    elif typ == "crawl4ai":
        if "api_key" in spec:
            options["api_key"] = _env_reference(spec["api_key"], f"{label}.api_key")
        browser_type = spec.get("browser_type", "chromium")
        if browser_type not in {"chromium", "firefox", "webkit"}:
            raise DefinitionError(f"{label}.browser_type is unsupported")
        headless = _boolean(spec.get("headless", True), f"{label}.headless")
        wait_until = spec.get("wait_until", "domcontentloaded")
        if wait_until not in {"domcontentloaded", "load", "networkidle"}:
            raise DefinitionError(f"{label}.wait_until is unsupported")
        if "wait_for_selector" in spec:
            wait_for_selector = _required_string(spec, "wait_for_selector", label)
            if wait_for_selector.strip().startswith(("js:", "()", "function")):
                raise DefinitionError(
                    f"{label}.wait_for_selector must be CSS for Crawl4AI 0.9"
                )
        delay = _duration(
            spec.get("delay_before_return_html", "0s"),
            f"{label}.delay_before_return_html",
            allow_zero=True,
        )
        options.update(
            {
                "browser_type": browser_type,
                "headless": headless,
                "wait_until": wait_until,
                "wait_for_selector": wait_for_selector,
                "delay_before_return_html": _duration_seconds(delay),
            }
        )
    elif typ == "firecrawl":
        if "api_key" in spec:
            options["api_key"] = _env_reference(spec["api_key"], f"{label}.api_key")
        api_version = spec.get("api_version", "v2")
        if api_version != "v2":
            raise DefinitionError(f"{label}.api_version must be v2")
        only_main_content = _boolean(
            spec.get("only_main_content", False), f"{label}.only_main_content"
        )
        if "wait_for_selector" in spec:
            wait_for_selector = _required_string(spec, "wait_for_selector", label)
        wait_for_duration = _duration(
            spec.get("wait_for", "0s"), f"{label}.wait_for", allow_zero=True
        )
        wait_for = _duration_seconds(wait_for_duration)
        mobile = _boolean(spec.get("mobile", False), f"{label}.mobile")
        actions = _actions(spec.get("actions", ()), typ, f"{label}.actions")
        options.update(
            {
                "api_version": api_version,
                "only_main_content": only_main_content,
                "wait_for_selector": wait_for_selector,
                "wait_for": wait_for,
                "mobile": mobile,
                "actions": actions,
            }
        )
    elif typ == "jina_reader":
        if "api_key" not in spec:
            raise DefinitionError(f"{label}.api_key is required")
        options["api_key"] = _env_reference(spec["api_key"], f"{label}.api_key")
        target_selector = spec.get("target_selector")
        if target_selector is not None:
            target_selector = _required_string(spec, "target_selector", label)
        with_generated_alt = _boolean(
            spec.get("with_generated_alt", False), f"{label}.with_generated_alt"
        )
        remove_selectors = _string_list(
            spec.get("remove_selectors", ()),
            f"{label}.remove_selectors",
            non_empty=True,
            unique=True,
        )
        options.update(
            {
                "target_selector": target_selector,
                "with_generated_alt": with_generated_alt,
                "remove_selectors": remove_selectors,
            }
        )
    elif typ == "zyte":
        if "api_key" not in spec:
            raise DefinitionError(f"{label}.api_key is required")
        options["api_key"] = _env_reference(spec["api_key"], f"{label}.api_key")
        mode = spec.get("mode", "auto")
        if mode not in {"auto", "http", "browser"}:
            raise DefinitionError(f"{label}.mode is unsupported")
        geolocation = spec.get("geolocation")
        if geolocation is not None:
            geolocation = _required_string(spec, "geolocation", label)
            if (
                len(geolocation) != 2
                or not geolocation.isalpha()
                or not geolocation.isupper()
            ):
                raise DefinitionError(
                    f"{label}.geolocation must be an ISO country code"
                )
        automatic_content = _boolean(
            spec.get("automatic_content", True), f"{label}.automatic_content"
        )
        actions = _actions(spec.get("actions", ()), typ, f"{label}.actions")
        options.update(
            {
                "mode": mode,
                "geolocation": geolocation,
                "automatic_content": automatic_content,
                "actions": actions,
            }
        )
    elif typ == "bright_data":
        if "api_key" not in spec:
            raise DefinitionError(f"{label}.api_key is required")
        if "zone" not in spec:
            raise DefinitionError(f"{label}.zone is required")
        options["api_key"] = _env_reference(spec["api_key"], f"{label}.api_key")
        zone = spec["zone"]
        if isinstance(zone, str) and _ENV_REFERENCE.fullmatch(zone):
            zone = _env_reference(zone, f"{label}.zone")
        elif not isinstance(zone, str) or not zone.strip():
            raise DefinitionError(f"{label}.zone must be a string or env reference")
        output = spec.get("output", "markdown")
        if output not in {"html", "markdown"}:
            raise DefinitionError(f"{label}.output must be html or markdown")
        country = spec.get("country")
        if country is not None:
            country = _required_string(spec, "country", label)
            if len(country) != 2 or not country.isalpha() or not country.isupper():
                raise DefinitionError(f"{label}.country must be an ISO country code")
        if "async" in spec and spec["async"] is not False:
            raise DefinitionError(f"{label}.async must be false when present")
        options.update(
            {"zone": zone, "output": output, "country": country, "async": False}
        )
        capabilities = (
            frozenset({"raw_html", "text"})
            if output == "html"
            else frozenset({"markdown"})
        )
    elif typ == "startup_http":
        if spec.get("trusted") is not True:
            raise DefinitionError(f"{label}.trusted must be true")
        options["trusted"] = True

    if "api_key" in options and any(
        name.casefold() == "authorization" for name in service_headers
    ):
        raise DefinitionError(
            f"{label}.api_key cannot be combined with an Authorization service header"
        )

    target_policy = {
        "allow_remote_dns_proxy": allow_remote_dns_proxy,
        "allow_hosts": allow_hosts,
        "allow_cidrs": allow_cidrs,
        "target_allow_hosts": allow_hosts,
        "target_allow_cidrs": allow_cidrs,
    }
    try:
        return ProviderBinding(
            name=name,
            type=typ,
            options=options,
            base_url=base_url,
            timeout=timeout,
            connect_timeout=connect_timeout,
            transport_retries=retries,
            max_body_bytes=max_body_bytes,
            concurrency=concurrency,
            cooldown=cooldown,
            capabilities=capabilities,
            service_headers=service_headers,
            target_headers=target_headers,
            cookies=cookies,
            proxy=proxy,
            target_policy=target_policy,
            timeout_seconds=_duration_seconds(timeout),
            only_main_content=only_main_content,
            wait_for_selector=wait_for_selector,
            wait_for=wait_for,
            mobile=mobile,
            actions=actions,
        )
    except (TypeError, ValueError) as error:
        raise DefinitionError(f"invalid {label}: {error}") from error


def _decode_extractor(name: str, raw: object) -> ExtractorBinding:
    label = f"web.extractors.{name}"
    spec = _mapping(raw, label)
    typ = _required_string(spec, "type", label)
    if typ not in _EXTRACTOR_TYPES:
        raise DefinitionError(f"{label}.type is unsupported: {typ!r}")
    fields = {
        "identity": set(),
        "trafilatura": {
            "include_tables",
            "include_comments",
            "favor_precision",
            "favor_recall",
        },
        "newspaper4k": {"language"},
        "resiliparse": {"main_content", "preserve_formatting"},
        "beautifulsoup": {"mode", "content_selector", "remove_selectors"},
    }[typ]
    _check_keys(spec, fields | {"type"}, label)
    options: dict[str, object] = {}
    if typ == "trafilatura":
        options.update(
            {
                key: _boolean(spec.get(key, default), f"{label}.{key}")
                for key, default in (
                    ("include_tables", True),
                    ("include_comments", False),
                    ("favor_precision", False),
                    ("favor_recall", False),
                )
            }
        )
    elif typ == "newspaper4k":
        language = spec.get("language")
        if language is not None:
            language = _required_string(spec, "language", label)
            if _BCP47.fullmatch(language) is None:
                raise DefinitionError(f"{label}.language must be a BCP-47 language tag")
        options["language"] = language
    elif typ == "resiliparse":
        options.update(
            {
                "main_content": _boolean(
                    spec.get("main_content", True), f"{label}.main_content"
                ),
                "preserve_formatting": _boolean(
                    spec.get("preserve_formatting", False),
                    f"{label}.preserve_formatting",
                ),
            }
        )
    elif typ == "beautifulsoup":
        mode = spec.get("mode", "article")
        if mode not in {"article", "plain"}:
            raise DefinitionError(f"{label}.mode must be article or plain")
        selector = spec.get("content_selector")
        if selector is not None:
            selector = _required_string(spec, "content_selector", label)
        remove_selectors = _string_list(
            spec.get("remove_selectors", ()),
            f"{label}.remove_selectors",
            non_empty=True,
            unique=True,
        )
        options.update(
            {
                "mode": mode,
                "content_selector": selector,
                "remove_selectors": remove_selectors,
            }
        )
    try:
        return ExtractorBinding(name=name, type=typ, options=options)
    except (TypeError, ValueError) as error:
        raise DefinitionError(f"invalid {label}: {error}") from error


def decode_web_config(raw: object, base_dir: object | None = None) -> WebRegistryConfig:
    """Decode and freeze the top-level ``web`` authoring registry."""

    spec = _mapping(raw, "web")
    _check_keys(
        spec, {"providers", "fetch_chains", "extractors", "extract_chains"}, "web"
    )
    raw_providers = _mapping(spec.get("providers"), "web.providers")
    raw_extractors = _mapping(spec.get("extractors"), "web.extractors")
    providers = {
        _name(name, "web provider name"): _decode_provider(
            _name(name, "web provider name"), value
        )
        for name, value in raw_providers.items()
    }
    extractors = {
        _name(name, "web extractor name"): _decode_extractor(
            _name(name, "web extractor name"), value
        )
        for name, value in raw_extractors.items()
    }
    raw_fetch = _mapping(spec.get("fetch_chains"), "web.fetch_chains")
    fetch_chains: dict[str, tuple[str, ...]] = {}
    for name, raw_chain in raw_fetch.items():
        chain_name = _name(name, "web fetch chain name")
        chain_spec = _mapping(raw_chain, f"web.fetch_chains.{chain_name}")
        _check_keys(chain_spec, {"providers"}, f"web.fetch_chains.{chain_name}")
        members = _string_list(
            chain_spec.get("providers"),
            f"web.fetch_chains.{chain_name}.providers",
            non_empty=True,
            unique=True,
        )
        if not members:
            raise DefinitionError(
                f"web.fetch_chains.{chain_name}.providers must not be empty"
            )
        for member in members:
            if member not in providers:
                raise DefinitionError(
                    f"web.fetch_chains.{chain_name} references unknown provider {member!r}"
                )
        fetch_chains[chain_name] = members
    raw_extract = _mapping(spec.get("extract_chains"), "web.extract_chains")
    extract_chains: dict[str, tuple[str, ...]] = {}
    for name, raw_chain in raw_extract.items():
        chain_name = _name(name, "web extract chain name")
        chain_spec = _mapping(raw_chain, f"web.extract_chains.{chain_name}")
        _check_keys(chain_spec, {"extractors"}, f"web.extract_chains.{chain_name}")
        members = _string_list(
            chain_spec.get("extractors"),
            f"web.extract_chains.{chain_name}.extractors",
            non_empty=True,
            unique=True,
        )
        if not members:
            raise DefinitionError(
                f"web.extract_chains.{chain_name}.extractors must not be empty"
            )
        for member in members:
            if member not in extractors:
                raise DefinitionError(
                    f"web.extract_chains.{chain_name} references unknown extractor {member!r}"
                )
        member_types = [extractors[member].type for member in members]
        if member_types[0] != "identity" or member_types.count("identity") != 1:
            raise DefinitionError(
                f"web.extract_chains.{chain_name} must start with identity exactly once"
            )
        if len(member_types) != len(set(member_types)):
            raise DefinitionError(
                f"web.extract_chains.{chain_name} may contain each extractor type at most once"
            )
        extract_chains[chain_name] = members
    return WebRegistryConfig(
        providers=providers,
        fetch_chains=fetch_chains,
        extractors=extractors,
        extract_chains=extract_chains,
    )


def _quality(config: Mapping[str, object], label: str) -> QualityPolicy:
    min_chars = _integer(config.get("min_chars", 500), f"{label}.min_chars", 0)
    min_words_value = config.get("min_words")
    min_words = (
        None
        if min_words_value is None
        else _integer(min_words_value, f"{label}.min_words", 1)
    )
    ratio_value = config.get("max_link_ratio")
    if ratio_value is not None:
        if (
            isinstance(ratio_value, bool)
            or not isinstance(ratio_value, (int, float))
            or not 0 <= ratio_value <= 1
        ):
            raise DefinitionError(f"{label}.max_link_ratio must be between 0 and 1")
        ratio = float(ratio_value)
    else:
        ratio = None
    reject = config.get("reject_challenges", True)
    if reject is not True:
        raise DefinitionError(f"{label}.reject_challenges cannot be disabled")
    try:
        return QualityPolicy(
            min_chars=min_chars, min_words=min_words, max_link_ratio=ratio
        )
    except ValueError as error:
        raise DefinitionError(f"invalid {label} quality policy: {error}") from error


def _fetch_chain(
    registry: WebRegistryConfig, name: str, label: str
) -> tuple[ProviderBinding, ...]:
    if not isinstance(name, str) or not name:
        raise DefinitionError(f"{label} must be a non-empty chain name")
    try:
        members = registry.fetch_chains[name]
    except KeyError as error:
        raise DefinitionError(
            f"{label} references unknown fetch chain {name!r}"
        ) from error
    return tuple(registry.providers[member] for member in members)


def _extract_chain(
    registry: WebRegistryConfig, name: str, label: str
) -> tuple[ExtractorBinding, ...]:
    if not isinstance(name, str) or not name:
        raise DefinitionError(f"{label} must be a non-empty chain name")
    try:
        members = registry.extract_chains[name]
    except KeyError as error:
        raise DefinitionError(
            f"{label} references unknown extract chain {name!r}"
        ) from error
    return tuple(registry.extractors[member] for member in members)


def _synthetic_rss_provider() -> ProviderBinding:
    timeout = timedelta(seconds=30)
    connect_timeout = timedelta(seconds=10)
    try:
        return ProviderBinding(
            name="__rss_direct__",
            type="httpx",
            options={
                "timeout": 30,
                "connect_timeout": 10,
                "max_body_bytes": 10_485_760,
                "transport_retries": 1,
                "concurrency": 8,
                "cooldown": 60,
                "service_headers": {},
                "target_headers": {},
                "target_cookies": {},
                "proxy": None,
                "allow_remote_dns_proxy": False,
                "target_allow_hosts": (),
                "target_allow_cidrs": (),
            },
            timeout=timeout,
            connect_timeout=connect_timeout,
            transport_retries=1,
            max_body_bytes=10_485_760,
            concurrency=8,
            cooldown=timedelta(seconds=60),
            capabilities=frozenset({"raw_source", "text", "raw_html"}),
            service_headers={},
            target_headers={},
            cookies={},
            target_policy={
                "allow_remote_dns_proxy": False,
                "allow_hosts": (),
                "allow_cidrs": (),
                "target_allow_hosts": (),
                "target_allow_cidrs": (),
            },
        )
    except (TypeError, ValueError) as error:
        raise DefinitionError(f"invalid synthetic RSS provider: {error}") from error


def expand_web_binding(
    use: str, with_config: Mapping[str, object], registry: WebRegistryConfig
) -> WebBinding | None:
    """Expand one built-in web Task/Source into an immutable WebBinding."""

    if use not in _WEB_TASKS and use not in _WEB_SOURCES:
        return None
    if not isinstance(registry, WebRegistryConfig):
        raise DefinitionError("web binding expansion requires a WebRegistryConfig")
    if not isinstance(with_config, Mapping):
        raise DefinitionError("web binding configuration must be a mapping")
    config = dict(with_config)
    if use == "sluice.tasks.fetch_webpage":
        _check_keys(
            config,
            {
                "fetch_chain",
                "extract_chain",
                "url",
                "url_field",
                "fallback_url_field",
                "min_chars",
                "min_words",
                "max_link_ratio",
                "reject_challenges",
            },
            use,
        )
        fetch_name = config.get("fetch_chain")
        extract_name = config.get("extract_chain")
        if not isinstance(fetch_name, str) or not fetch_name:
            raise DefinitionError("fetch_webpage.fetch_chain is required")
        if not isinstance(extract_name, str) or not extract_name:
            raise DefinitionError("fetch_webpage.extract_chain is required")
        quality = _quality(config, use)
        url = config.get("url")
        if url is not None and (not isinstance(url, str) or not url.strip()):
            raise DefinitionError("fetch_webpage.url must be a non-empty string")
        url_field = config.get("url_field", "url")
        fallback = config.get("fallback_url_field", "link")
        if not isinstance(url_field, str) or not url_field:
            raise DefinitionError("fetch_webpage.url_field must be a non-empty string")
        if not isinstance(fallback, str) or not fallback:
            raise DefinitionError(
                "fetch_webpage.fallback_url_field must be a non-empty string"
            )
        return WebBinding(
            fetch_chain=_fetch_chain(registry, fetch_name, "fetch_webpage.fetch_chain"),
            extract_chain=_extract_chain(
                registry, extract_name, "fetch_webpage.extract_chain"
            ),
            fetch_chain_name=fetch_name,
            extract_chain_name=extract_name,
            quality=quality,
            output="artifact",
        )
    if use == "sluice.tasks.extract_content":
        _check_keys(
            config,
            {
                "extract_chain",
                "input_format",
                "min_chars",
                "min_words",
                "max_link_ratio",
                "reject_challenges",
            },
            use,
        )
        extract_name = config.get("extract_chain")
        if not isinstance(extract_name, str) or not extract_name:
            raise DefinitionError("extract_content.extract_chain is required")
        input_format = config.get("input_format", "auto")
        if input_format not in {"auto", "html", "text", "markdown"}:
            raise DefinitionError("extract_content.input_format is unsupported")
        return WebBinding(
            extract_chain=_extract_chain(
                registry, extract_name, "extract_content.extract_chain"
            ),
            extract_chain_name=extract_name,
            quality=_quality(config, use),
            output="artifact",
        )
    if use == "sluice.tasks.to_markdown" or use == "sluice.tasks.to_text":
        _check_keys(config, {"input_format"}, use)
        input_format = config.get("input_format", "auto")
        if input_format not in {"auto", "html", "text", "markdown"}:
            raise DefinitionError(f"{use}.input_format is unsupported")
        return WebBinding(output="markdown" if use.endswith("to_markdown") else "text")
    if use == "sluice.sources.webpage":
        _check_keys(
            config,
            {
                "url",
                "list_selector",
                "fetch_chain",
                "extract_chain",
                "output",
                "min_chars",
                "min_words",
                "max_link_ratio",
                "reject_challenges",
                "next_checkpoint",
                "web_cache_version",
            },
            use,
        )
        url = config.get("url")
        if not isinstance(url, str) or not url.strip():
            raise DefinitionError("webpage.url must be a non-empty string")
        fetch_name = config.get("fetch_chain")
        if not isinstance(fetch_name, str) or not fetch_name:
            raise DefinitionError("webpage.fetch_chain is required")
        if "next_checkpoint" in config:
            _freeze(config["next_checkpoint"])
        cache_version = _integer(
            config.get("web_cache_version", 1), "webpage.web_cache_version", 1
        )
        selector = config.get("list_selector")
        if selector is not None:
            selector = _selector(selector, "webpage.list_selector")
            forbidden = {
                "extract_chain",
                "output",
                "min_chars",
                "min_words",
                "max_link_ratio",
                "reject_challenges",
            }
            present = forbidden & set(config)
            if present:
                raise DefinitionError(
                    "webpage list_selector cannot be combined with output/extract/quality fields"
                )
            return WebBinding(
                fetch_chain=_fetch_chain(registry, fetch_name, "webpage.fetch_chain"),
                fetch_chain_name=fetch_name,
                quality=QualityPolicy(),
                output="raw_html",
                web_cache_version=cache_version,
            )
        extract_name = config.get("extract_chain")
        if not isinstance(extract_name, str) or not extract_name:
            raise DefinitionError("webpage.extract_chain is required")
        output = config.get("output")
        if output not in {"markdown", "text"}:
            raise DefinitionError("webpage.output must be markdown or text")
        return WebBinding(
            fetch_chain=_fetch_chain(registry, fetch_name, "webpage.fetch_chain"),
            extract_chain=_extract_chain(
                registry, extract_name, "webpage.extract_chain"
            ),
            fetch_chain_name=fetch_name,
            extract_chain_name=extract_name,
            quality=_quality(config, use),
            output=output,
            web_cache_version=cache_version,
        )
    # RSS Source
    _check_keys(
        config,
        {
            "url",
            "raw_fallback_chain",
            "entry_output",
            "cursor_param",
            "checkpoint_param",
            "next_checkpoint",
            "web_cache_version",
        },
        use,
    )
    url = config.get("url")
    if not isinstance(url, str) or not url.strip():
        raise DefinitionError("rss.url must be a non-empty string")
    if "next_checkpoint" in config:
        _freeze(config["next_checkpoint"])
    cursor = config.get("cursor_param")
    checkpoint = config.get("checkpoint_param")
    if cursor is not None and checkpoint is not None:
        raise DefinitionError("rss cannot set both cursor_param and checkpoint_param")
    if cursor is not None and (not isinstance(cursor, str) or not cursor.strip()):
        raise DefinitionError("rss.cursor_param must be a non-empty string")
    if checkpoint is not None and (
        not isinstance(checkpoint, str) or not checkpoint.strip()
    ):
        raise DefinitionError("rss.checkpoint_param must be a non-empty string")
    if cursor is not None and checkpoint is not None and cursor != checkpoint:
        raise DefinitionError("rss checkpoint_param alias must match cursor_param")
    fallback = config.get("raw_fallback_chain")
    if fallback is not None and (not isinstance(fallback, str) or not fallback):
        raise DefinitionError("rss.raw_fallback_chain must be a non-empty string")
    entry_output = config.get("entry_output", "text")
    if entry_output not in {"text", "markdown"}:
        raise DefinitionError("rss.entry_output must be text or markdown")
    cache_version = _integer(
        config.get("web_cache_version", 1), "rss.web_cache_version", 1
    )
    return WebBinding(
        direct_provider=_synthetic_rss_provider(),
        fetch_chain=(
            None
            if fallback is None
            else _fetch_chain(registry, fallback, "rss.raw_fallback_chain")
        ),
        fetch_chain_name=fallback,
        output="raw_source",
        web_cache_version=cache_version,
    )


__all__ = ["WebRegistryConfig", "decode_web_config", "expand_web_binding"]
