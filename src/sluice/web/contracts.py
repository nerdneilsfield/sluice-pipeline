"""Frozen value contracts for the web content pipeline.

These objects cross task/provider boundaries and are deliberately immutable.
The runtime may therefore retain a binding snapshot while the YAML definition
changes, and an in-flight provider cannot mutate an artifact that another
consumer is projecting.  This module does not import :mod:`sluice.contracts`
at module import time; the core contracts module imports :class:`WebBinding`
late to make both import directions safe.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import timedelta
import math
import re
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Final,
    Literal,
    Mapping,
    TypeAlias,
)
from urllib.parse import urlsplit

from ..errors import ExecutionError


JSONValue: TypeAlias = (
    None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]
)
FrozenJSON: TypeAlias = (
    None
    | bool
    | int
    | float
    | str
    | tuple["FrozenJSON", ...]
    | Mapping[str, "FrozenJSON"]
)


ARTIFACT_CAPABILITIES: Final[frozenset[str]] = frozenset(
    {"raw_html", "raw_source", "text", "markdown", "rendered_js"}
)
REQUEST_CAPABILITIES: Final[frozenset[str]] = frozenset(
    {"raw_html", "raw_source", "text", "markdown"}
)
SELECTED_FORMATS: Final[frozenset[str]] = frozenset(
    {"raw_html", "text", "markdown"}
)
WEB_OUTPUTS: Final[frozenset[str]] = frozenset(
    {"artifact", "markdown", "text", "raw_html", "raw_source"}
)
WEB_MODES: Final[frozenset[str]] = frozenset(
    {"article", "list_html", "raw_source"}
)
STABLE_FAILURE_REASONS: Final[frozenset[str]] = frozenset(
    {
        "bad_url",
        "ssrf_blocked",
        "timeout",
        "network",
        "rate_limited",
        "provider_busy",
        "http_status",
        "bad_content_type",
        "body_too_large",
        "unsafe_xml",
        "challenge",
        "empty_shell",
        "empty",
        "too_short",
        "extract_failed",
        "missing_capability",
        "provider_error",
    }
)

ArtifactCapability: TypeAlias = Literal[
    "raw_html", "raw_source", "text", "markdown", "rendered_js"
]
RequestCapability: TypeAlias = Literal["raw_html", "raw_source", "text", "markdown"]
WebMode: TypeAlias = Literal["article", "list_html", "raw_source"]
SelectedFormat: TypeAlias = Literal["raw_html", "text", "markdown"]
WebOutput: TypeAlias = Literal[
    "artifact", "markdown", "text", "raw_html", "raw_source"
]


def _freeze_json(value: Any) -> Any:
    """Use the core JSON freezer without introducing an import cycle."""

    # ``sluice.contracts`` imports WebBinding after its definitions.  Calls to
    # this helper occur after construction, at which point that late import is
    # complete regardless of which module the caller imported first.
    from ..contracts import freeze_item_json

    return freeze_item_json(value)


def _freeze_mapping(value: Mapping[str, Any] | None) -> Mapping[str, FrozenJSON]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise TypeError("expected a mapping")
    frozen = _freeze_json(dict(value))
    # ``freeze_item_json`` returns a read-only mapping for dictionaries.  The
    # fallback keeps this helper robust if its implementation changes.
    return frozen if isinstance(frozen, Mapping) else MappingProxyType(dict(value))


def _validate_non_empty(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_ENV_REFERENCE = re.compile(r"^env:[A-Z_][A-Z0-9_]*$")


def _validate_safe_identifier(value: str, field_name: str) -> str:
    # The RSS fallback uses one reserved synthetic provider name whose leading
    # underscores intentionally distinguish it from authored instances.
    if value == "__rss_direct__":
        return value
    if not isinstance(value, str) or not _SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(
            f"{field_name} must be an ASCII identifier of at most 128 characters"
        )
    return value


def _is_secret_field(key: str) -> bool:
    normalized = key.casefold().replace("_", "-")
    return (
        normalized in {"api-key", "authorization", "proxy-authorization", "cookie", "set-cookie"}
        or any(
            token in normalized
            for token in (
                "authorization",
                "proxy-authorization",
                "cookie",
                "set-cookie",
                "api-key",
                "token",
                "secret",
                "signature",
            )
        )
    )


def _duration_seconds(value: timedelta | None) -> int | None:
    if value is None:
        return None
    seconds = value.total_seconds()
    return int(seconds) if seconds.is_integer() else seconds


def _is_exact_env_reference(value: Any) -> bool:
    return isinstance(value, str) and _ENV_REFERENCE.fullmatch(value) is not None


def _safe_proxy(value: Any) -> Any:
    if value is None:
        return None
    if _is_exact_env_reference(value):
        return value
    if not isinstance(value, str):
        return "<redacted>"
    if value.startswith("env:"):
        return "<redacted>"
    try:
        parsed = urlsplit(value)
    except ValueError:
        return "<redacted>"
    if (
        parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or "?" in value
        or "#" in value
    ):
        return "<redacted>"
    return value


@dataclass(frozen=True, slots=True)
class QualityPolicy:
    """Quality thresholds applied to a candidate representation.

    ``None`` disables the optional word and link-ratio checks.  Challenge,
    MIME, and body-size safety checks remain mandatory in the web runtime.
    """

    min_chars: int = 500
    min_words: int | None = None
    max_link_ratio: float | None = None
    reject_challenges: bool = True
    max_body_bytes: int = 10_485_760

    def __post_init__(self) -> None:
        if (
            isinstance(self.min_chars, bool)
            or not isinstance(self.min_chars, int)
            or self.min_chars < 0
        ):
            raise ValueError("min_chars must be a non-negative integer")
        if self.min_words is not None and (
            isinstance(self.min_words, bool)
            or not isinstance(self.min_words, int)
            or self.min_words <= 0
        ):
            raise ValueError("min_words must be a positive integer or null")
        if self.max_link_ratio is not None and not (
            not isinstance(self.max_link_ratio, bool)
            and isinstance(self.max_link_ratio, (int, float))
            and 0 <= self.max_link_ratio <= 1
        ):
            raise ValueError("max_link_ratio must be between 0 and 1")
        if not isinstance(self.reject_challenges, bool):
            raise ValueError("reject_challenges must be a boolean")
        if (
            isinstance(self.max_body_bytes, bool)
            or not isinstance(self.max_body_bytes, int)
            or self.max_body_bytes <= 0
            or self.max_body_bytes > 10_485_760
        ):
            raise ValueError("max_body_bytes must be between 1 and 10485760")


@dataclass(frozen=True, slots=True)
class ProviderBinding:
    """One fully expanded provider instance in a fetch chain.

    ``type`` is the canonical short field used by YAML.  ``provider_type`` is
    retained as a read-compatible alias because provenance and runtime code
    naturally use that spelling.  Both values are normalised to the same
    immutable value at construction.
    """

    name: str
    type: str | None = None
    options: Mapping[str, FrozenJSON] = field(default_factory=dict)
    provider_type: str | None = None
    base_url: str | None = None
    timeout: timedelta | None = None
    connect_timeout: timedelta | None = None
    transport_retries: int = 0
    max_body_bytes: int = 10_485_760
    concurrency: int = 8
    cooldown: timedelta = timedelta(seconds=60)
    capabilities: frozenset[ArtifactCapability | str] = frozenset()
    service_headers: Mapping[str, FrozenJSON] = field(default_factory=dict)
    target_headers: Mapping[str, FrozenJSON] = field(default_factory=dict)
    cookies: Mapping[str, FrozenJSON] = field(default_factory=dict)
    proxy: str | None = None
    target_policy: Any = None
    timeout_seconds: int | None = None
    only_main_content: bool = False
    wait_for_selector: str | None = None
    wait_for: Any = None
    mobile: bool = False
    actions: tuple[Any, ...] = ()

    def __post_init__(self) -> None:
        _validate_safe_identifier(self.name, "provider name")
        provider_type = self.provider_type or self.type
        if provider_type is None:
            raise ValueError("provider type must be provided")
        _validate_safe_identifier(provider_type, "provider type")
        object.__setattr__(self, "type", provider_type)
        object.__setattr__(self, "provider_type", provider_type)
        object.__setattr__(self, "options", _freeze_mapping(self.options))
        object.__setattr__(self, "service_headers", _freeze_mapping(self.service_headers))
        object.__setattr__(self, "target_headers", _freeze_mapping(self.target_headers))
        object.__setattr__(self, "cookies", _freeze_mapping(self.cookies))
        if isinstance(self.target_policy, Mapping):
            object.__setattr__(self, "target_policy", _freeze_mapping(self.target_policy))

        if (
            isinstance(self.transport_retries, bool)
            or not isinstance(self.transport_retries, int)
            or self.transport_retries < 0
        ):
            raise ValueError("transport_retries must be a non-negative integer")
        if (
            isinstance(self.max_body_bytes, bool)
            or not isinstance(self.max_body_bytes, int)
            or self.max_body_bytes <= 0
            or self.max_body_bytes > 10_485_760
        ):
            raise ValueError("max_body_bytes must be between 1 and 10485760")
        if (
            isinstance(self.concurrency, bool)
            or not isinstance(self.concurrency, int)
            or self.concurrency <= 0
        ):
            raise ValueError("concurrency must be a positive integer")
        if self.timeout is not None and self.timeout <= timedelta(0):
            raise ValueError("timeout must be positive")
        if self.connect_timeout is not None and self.connect_timeout <= timedelta(0):
            raise ValueError("connect_timeout must be positive")
        if (
            self.timeout is not None
            and self.connect_timeout is not None
            and self.connect_timeout > self.timeout
        ):
            raise ValueError("connect_timeout cannot exceed timeout")
        if self.cooldown < timedelta(0):
            raise ValueError("cooldown must be non-negative")
        timeout_seconds = self.timeout_seconds
        if timeout_seconds is not None:
            if isinstance(timeout_seconds, bool) or not isinstance(
                timeout_seconds, (int, float)
            ):
                raise ValueError("timeout_seconds must be a positive whole number")
            try:
                finite_timeout = math.isfinite(timeout_seconds)
            except OverflowError:
                finite_timeout = False
            whole_timeout = (
                isinstance(timeout_seconds, int)
                or (isinstance(timeout_seconds, float) and timeout_seconds.is_integer())
            )
            if (
                not finite_timeout
                or timeout_seconds <= 0
                or not whole_timeout
            ):
                raise ValueError("timeout_seconds must be a positive whole number")
            timeout_seconds = int(timeout_seconds)
        if self.timeout is not None:
            timeout_total = self.timeout.total_seconds()
            if not math.isfinite(timeout_total) or not timeout_total.is_integer():
                raise ValueError("timeout must be a whole number of seconds")
            expected_timeout_seconds = int(timeout_total)
            if timeout_seconds is None:
                timeout_seconds = expected_timeout_seconds
            elif timeout_seconds != expected_timeout_seconds:
                raise ValueError("timeout_seconds must match timeout")
        if self.connect_timeout is not None:
            connect_total = self.connect_timeout.total_seconds()
            if not math.isfinite(connect_total) or not connect_total.is_integer():
                raise ValueError("connect_timeout must be a whole number of seconds")
        object.__setattr__(self, "timeout_seconds", timeout_seconds)
        if not isinstance(self.only_main_content, bool):
            raise ValueError("only_main_content must be a boolean")
        if not isinstance(self.mobile, bool):
            raise ValueError("mobile must be a boolean")
        object.__setattr__(
            self,
            "actions",
            tuple(_freeze_json(action) for action in self.actions),
        )

        caps = frozenset(self.capabilities)
        if not caps <= ARTIFACT_CAPABILITIES:
            unknown = sorted(caps - ARTIFACT_CAPABILITIES)
            raise ValueError(f"unknown provider capability: {unknown[0]}")
        object.__setattr__(self, "capabilities", caps)

    @property
    def config(self) -> Mapping[str, FrozenJSON]:
        """Alias for the expanded, non-secret provider options."""

        return self.options


@dataclass(frozen=True, slots=True)
class ExtractorBinding:
    """One fully expanded deterministic local extractor instance."""

    name: str
    type: str | None = None
    options: Mapping[str, FrozenJSON] = field(default_factory=dict)
    extractor_type: str | None = None

    def __post_init__(self) -> None:
        _validate_safe_identifier(self.name, "extractor name")
        extractor_type = self.extractor_type or self.type
        if extractor_type is None:
            raise ValueError("extractor type must be provided")
        _validate_safe_identifier(extractor_type, "extractor type")
        object.__setattr__(self, "type", extractor_type)
        object.__setattr__(self, "extractor_type", extractor_type)
        object.__setattr__(self, "options", _freeze_mapping(self.options))

    @property
    def config(self) -> Mapping[str, FrozenJSON]:
        return self.options


@dataclass(frozen=True, slots=True)
class WebBinding:
    """The immutable, fully expanded web configuration for one definition."""

    version: int = 1
    direct_provider: ProviderBinding | None = None
    fetch_chain: tuple[ProviderBinding, ...] | None = None
    extract_chain: tuple[ExtractorBinding, ...] | None = None
    quality: QualityPolicy = field(default_factory=QualityPolicy)
    output: WebOutput = "artifact"
    fetch_chain_name: str | None = None
    extract_chain_name: str | None = None
    web_cache_version: int | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.version, bool)
            or not isinstance(self.version, int)
            or self.version <= 0
        ):
            raise ValueError("web binding version must be a positive integer")
        if self.direct_provider is not None and not isinstance(
            self.direct_provider, ProviderBinding
        ):
            raise TypeError("direct_provider must be a ProviderBinding or null")
        if self.output not in WEB_OUTPUTS:
            raise ValueError(f"unsupported web output: {self.output!r}")
        if not isinstance(self.quality, QualityPolicy):
            raise TypeError("quality must be a QualityPolicy")
        if self.fetch_chain_name is not None:
            _validate_safe_identifier(self.fetch_chain_name, "fetch chain name")
        if self.extract_chain_name is not None:
            _validate_safe_identifier(self.extract_chain_name, "extract chain name")
        if self.web_cache_version is not None and (
            isinstance(self.web_cache_version, bool)
            or not isinstance(self.web_cache_version, int)
            or self.web_cache_version <= 0
        ):
            raise ValueError("web_cache_version must be a positive integer or null")

        fetch_chain, fetch_name = _normalise_chain(
            self.fetch_chain, ProviderBinding, self.fetch_chain_name, "provider"
        )
        extract_chain, extract_name = _normalise_chain(
            self.extract_chain, ExtractorBinding, self.extract_chain_name, "extractor"
        )
        object.__setattr__(self, "fetch_chain", fetch_chain)
        object.__setattr__(self, "extract_chain", extract_chain)
        object.__setattr__(self, "fetch_chain_name", fetch_name)
        object.__setattr__(self, "extract_chain_name", extract_name)

    @property
    def providers(self) -> tuple[ProviderBinding, ...]:
        """Return the ordered providers, or an empty tuple for extract-only work."""

        chain = self.fetch_chain
        return () if chain is None else tuple(chain)

    @property
    def extractors(self) -> tuple[ExtractorBinding, ...]:
        chain = self.extract_chain
        return () if chain is None else tuple(chain)

    def semantic_projection(self) -> Mapping[str, FrozenJSON]:
        """Return the deterministic, secret-safe identity of this binding.

        Provider and extractor objects are runtime contracts, while this
        projection is the persisted/cache-facing representation.  It contains
        only JSON-safe values and retains secret *references* (for example
        ``env:API_KEY``), never values resolved from the environment.
        """

        def safe_value(value: Any, key: str | None = None) -> Any:
            if key is not None and key.casefold().replace("_", "-") == "proxy":
                return _safe_proxy(value)
            if key is not None and _is_secret_field(key):
                if isinstance(value, Mapping):
                    return {
                        str(name): safe_value(item, "__secret_value__")
                        for name, item in value.items()
                    }
                if isinstance(value, (tuple, list)):
                    return [safe_value(item, "__secret_value__") for item in value]
                if _is_exact_env_reference(value):
                    return value
                return "<redacted>"
            if isinstance(value, Mapping):
                return {
                    str(name): safe_value(item, str(name))
                    for name, item in value.items()
                }
            if isinstance(value, (tuple, list)):
                return [safe_value(item, key) for item in value]
            if isinstance(value, timedelta):
                return _duration_seconds(value)
            return value

        def provider_projection(provider: ProviderBinding) -> Mapping[str, Any]:
            return {
                "name": provider.name,
                "type": provider.type,
                "options": safe_value(provider.options),
                "base_url": provider.base_url,
                "timeout": _duration_seconds(provider.timeout),
                "connect_timeout": _duration_seconds(provider.connect_timeout),
                "transport_retries": provider.transport_retries,
                "max_body_bytes": provider.max_body_bytes,
                "concurrency": provider.concurrency,
                "cooldown": _duration_seconds(provider.cooldown),
                "capabilities": sorted(provider.capabilities),
                "service_headers": safe_value(provider.service_headers),
                "target_headers": safe_value(provider.target_headers),
                "cookies": safe_value(provider.cookies, "cookies"),
                "proxy": _safe_proxy(provider.proxy),
                "target_policy": safe_value(provider.target_policy),
                "timeout_seconds": provider.timeout_seconds,
                "only_main_content": provider.only_main_content,
                "wait_for_selector": provider.wait_for_selector,
                "wait_for": safe_value(provider.wait_for),
                "mobile": provider.mobile,
                "actions": safe_value(provider.actions),
            }

        def extractor_projection(extractor: ExtractorBinding) -> Mapping[str, Any]:
            return {
                "name": extractor.name,
                "type": extractor.type,
                "options": safe_value(extractor.options),
            }

        projection: dict[str, Any] = {
            "version": self.version,
            "direct_provider": (
                None
                if self.direct_provider is None
                else provider_projection(self.direct_provider)
            ),
            "fetch_chain": (
                None
                if self.fetch_chain is None
                else {
                    "name": self.fetch_chain_name,
                    "providers": [provider_projection(p) for p in self.fetch_chain],
                }
            ),
            "extract_chain": (
                None
                if self.extract_chain is None
                else {
                    "name": self.extract_chain_name,
                    "extractors": [
                        extractor_projection(e) for e in self.extract_chain
                    ],
                }
            ),
            "quality": {
                "min_chars": self.quality.min_chars,
                "min_words": self.quality.min_words,
                "max_link_ratio": self.quality.max_link_ratio,
                "reject_challenges": self.quality.reject_challenges,
                "max_body_bytes": self.quality.max_body_bytes,
            },
            "output": self.output,
        }
        if self.web_cache_version is not None:
            projection["web_cache_version"] = self.web_cache_version
        return _freeze_mapping(projection)

    def runtime_digest(self) -> Mapping[str, FrozenJSON]:
        """Return the runtime-safe identity used by provider catalogs.

        Resolved clients, limiters, and other process-local values are not
        representable in this immutable contract.  A runtime catalog may add
        those values in memory, but it must retain this secret-safe projection
        as its canonical base.
        """

        return self.semantic_projection()


def _normalise_chain(
    value: Any,
    member_type: type,
    name: str | None,
    member_label: str,
) -> tuple[Any, str | None]:
    if value is None:
        return None, name
    try:
        members_tuple = tuple(value)
    except TypeError as exc:
        raise TypeError(f"{member_label} chain must be an ordered collection") from exc
    if any(not isinstance(member, member_type) for member in members_tuple):
        raise TypeError(
            f"{member_label} chain members must be {member_type.__name__} values"
        )
    return members_tuple, name


@dataclass(frozen=True, slots=True)
class PrivateWebInvocation:
    """Private immutable pair passed to built-in web wrappers only."""

    with_config: Mapping[str, FrozenJSON]
    web_binding: WebBinding

    def __post_init__(self) -> None:
        object.__setattr__(self, "with_config", _freeze_mapping(self.with_config))
        if not isinstance(self.web_binding, WebBinding):
            raise TypeError("web_binding must be a WebBinding")


@dataclass(frozen=True, slots=True)
class FetchRequest:
    """One normalized, mode-specific request sent to a provider."""

    url: str
    mode: WebMode
    acceptable: frozenset[RequestCapability | str]
    preferred: tuple[RequestCapability | str, ...] = ()
    list_selector: str | None = None
    target_headers: Mapping[str, FrozenJSON] = field(default_factory=dict)
    quality: QualityPolicy = field(default_factory=QualityPolicy)
    stage_deadline: float | None = None

    def __post_init__(self) -> None:
        _validate_non_empty(self.url, "url")
        if self.mode not in WEB_MODES:
            raise ValueError(f"unsupported fetch mode: {self.mode!r}")
        acceptable = frozenset(self.acceptable)
        if not acceptable:
            raise ValueError("acceptable capabilities must not be empty")
        if not acceptable <= REQUEST_CAPABILITIES:
            unknown = sorted(acceptable - REQUEST_CAPABILITIES)
            raise ValueError(f"unsupported request capability: {unknown[0]}")
        preferred = tuple(self.preferred)
        if not set(preferred) <= REQUEST_CAPABILITIES:
            unknown = sorted(set(preferred) - REQUEST_CAPABILITIES)
            raise ValueError(f"unsupported preferred capability: {unknown[0]}")
        if self.mode == "list_html":
            if self.list_selector is None or not self.list_selector.strip():
                raise ValueError("list_selector is required for list_html requests")
            if acceptable != frozenset({"raw_html"}):
                raise ValueError("list_html requests require raw_html capability")
            if preferred and set(preferred) != {"raw_html"}:
                raise ValueError("list_html preferred capability must be raw_html")
        elif self.mode == "raw_source":
            if acceptable != frozenset({"raw_source"}):
                raise ValueError("raw_source requests require raw_source capability")
            if preferred and set(preferred) != {"raw_source"}:
                raise ValueError("raw_source preferred capability must be raw_source")
            if self.list_selector is not None:
                raise ValueError("list_selector is valid only for list_html requests")
        elif "raw_source" in acceptable:
            raise ValueError("article requests cannot accept raw_source capability")
        elif self.list_selector is not None:
            raise ValueError("list_selector is valid only for list_html requests")
        if not set(preferred) <= acceptable:
            raise ValueError("preferred capabilities must be acceptable")
        if not isinstance(self.quality, QualityPolicy):
            raise TypeError("quality must be a QualityPolicy")
        if self.stage_deadline is not None:
            if isinstance(self.stage_deadline, bool) or not isinstance(
                self.stage_deadline, (int, float)
            ):
                raise ValueError(
                    "stage_deadline must be a finite positive monotonic timestamp or null"
                )
            try:
                finite_deadline = math.isfinite(self.stage_deadline)
            except OverflowError:
                finite_deadline = False
            if (
                not finite_deadline
                or self.stage_deadline <= 0
            ):
                raise ValueError(
                    "stage_deadline must be a finite positive monotonic timestamp or null"
                )
        object.__setattr__(self, "acceptable", acceptable)
        object.__setattr__(self, "preferred", preferred)
        object.__setattr__(self, "target_headers", _freeze_mapping(self.target_headers))


@dataclass(frozen=True, slots=True)
class DocumentArtifact:
    """Transient provider/extractor result.

    Artifacts are never persisted directly as Item payloads.  Every field is
    either a bounded scalar, immutable JSON metadata, or an immutable attempt
    summary so an artifact can safely cross a task boundary.
    """

    source_url: str | None = None
    final_url: str | None = None
    status_code: int | None = None
    content_type: str | None = None
    raw_html: str | None = None
    raw_source: bytes | None = None
    text: str | None = None
    markdown: str | None = None
    selected_format: SelectedFormat | None = None
    title: str | None = None
    metadata: Mapping[str, FrozenJSON] = field(default_factory=dict)
    capabilities: frozenset[ArtifactCapability | str] = frozenset()
    provider_instance: str | None = None
    provider_type: str | None = None
    extraction: str | None = None
    extract_binding_digest: str | None = None
    quality: Any = None
    attempts: tuple[WebAttemptFailure, ...] = ()

    def __post_init__(self) -> None:
        if self.status_code is not None and (
            isinstance(self.status_code, bool)
            or not isinstance(self.status_code, int)
        ):
            raise ValueError("status_code must be an integer or null")
        if self.selected_format is not None and self.selected_format not in SELECTED_FORMATS:
            raise ValueError(f"unsupported selected format: {self.selected_format!r}")
        capabilities = frozenset(self.capabilities)
        if not capabilities <= ARTIFACT_CAPABILITIES:
            unknown = sorted(capabilities - ARTIFACT_CAPABILITIES)
            raise ValueError(f"unsupported artifact capability: {unknown[0]}")
        if self.raw_source is not None and not isinstance(self.raw_source, bytes):
            if isinstance(self.raw_source, (bytearray, memoryview)):
                object.__setattr__(self, "raw_source", bytes(self.raw_source))
            else:
                raise TypeError("raw_source must be bytes or null")
        attempts = tuple(self.attempts)
        if not all(isinstance(attempt, WebAttemptFailure) for attempt in attempts):
            raise TypeError("attempts must contain WebAttemptFailure values")
        object.__setattr__(self, "capabilities", capabilities)
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))
        if isinstance(self.quality, Mapping):
            object.__setattr__(self, "quality", _freeze_mapping(self.quality))
        object.__setattr__(self, "attempts", attempts)


@dataclass(frozen=True, slots=True)
class ValidatedFeedRoot:
    """A bounded, defused XML feed root retained only for projection.

    The parsed element tree is intentionally kept out of ``DocumentArtifact``:
    a raw feed is a source-only representation and must not cross a Stage
    projector boundary.  ``kind`` is the fixed root classification used by
    the RSS/Atom Source implementation.
    """

    root: Any
    kind: str

    def __post_init__(self) -> None:
        if self.kind not in {"rss2", "rss1", "atom"}:
            raise ValueError("unsupported validated feed kind")


@dataclass(frozen=True, slots=True)
class WebAttemptFailure:
    """Redacted provider failure summary safe for durable errors/logs."""

    provider_instance: str
    provider_type: str
    attempt_ordinal: int
    reason: str
    status_code: int | None = None
    status_kind: Literal["service", "target"] | None = None
    elapsed_ms: int | None = None
    detail_code: str | None = None

    def __post_init__(self) -> None:
        _validate_safe_identifier(self.provider_instance, "provider instance")
        _validate_safe_identifier(self.provider_type, "provider type")
        if (
            isinstance(self.attempt_ordinal, bool)
            or not isinstance(self.attempt_ordinal, int)
            or self.attempt_ordinal <= 0
        ):
            raise ValueError("attempt_ordinal must be a positive integer")
        if self.reason not in STABLE_FAILURE_REASONS:
            raise ValueError(f"unsupported web failure reason: {self.reason!r}")
        if self.status_code is not None and (
            isinstance(self.status_code, bool)
            or not isinstance(self.status_code, int)
        ):
            raise ValueError("status_code must be an integer or null")
        if self.status_kind not in (None, "service", "target"):
            raise ValueError("status_kind must be service, target, or null")
        if self.elapsed_ms is not None and (
            isinstance(self.elapsed_ms, bool)
            or not isinstance(self.elapsed_ms, int)
            or self.elapsed_ms < 0
        ):
            raise ValueError("elapsed_ms must be non-negative or null")
        if self.detail_code is not None:
            _validate_safe_identifier(self.detail_code, "detail_code")

    @property
    def attempt(self) -> int:
        """Compatibility alias for callers that use the shorter spelling."""

        return self.attempt_ordinal


@dataclass(frozen=True, slots=True, init=False)
class FetchChainExhausted(ExecutionError):
    """Raised when all eligible article providers fail safely."""

    attempts: tuple[WebAttemptFailure, ...]

    def __new__(
        cls,
        attempts: tuple[WebAttemptFailure, ...] | list[WebAttemptFailure],
    ) -> "FetchChainExhausted":
        # Bypass BaseException's default ``__new__`` argument capture.  The
        # only retained payload is the validated attempts tuple below.
        return BaseException.__new__(cls)

    def __init__(self, attempts: tuple[WebAttemptFailure, ...] | list[WebAttemptFailure]):
        safe_attempts = tuple(attempts)
        if not all(isinstance(attempt, WebAttemptFailure) for attempt in safe_attempts):
            raise TypeError("attempts must contain WebAttemptFailure values")
        object.__setattr__(self, "attempts", safe_attempts)
        # BaseException.__new__ receives the constructor arguments before our
        # dataclass initializer runs.  Reset that implicit tuple without ever
        # constructing or passing a human-readable cause/message.
        ExecutionError.__init__(self)


@dataclass(frozen=True, slots=True)
class WebAttemptCancelled(ExecutionError):
    """Internal cooperative cancellation signal for a stale Stage attempt."""


@dataclass(frozen=True, slots=True)
class WebStageDeadlineExpired(ExecutionError):
    """Internal signal that a Stage-level web deadline has elapsed."""


@dataclass(frozen=True, slots=True, init=False)
class WebExecutionProbe:
    """Immutable callback wrapper checked around provider/extractor work."""

    callback: Callable[[], None] | None

    def __init__(
        self,
        callback: Callable[[], None] | None = None,
        *,
        checker: Callable[[], None] | None = None,
        check_fn: Callable[[], None] | None = None,
    ) -> None:
        supplied = [candidate for candidate in (callback, checker, check_fn) if candidate is not None]
        if len(supplied) > 1:
            raise TypeError("provide only one WebExecutionProbe callback")
        selected = supplied[0] if supplied else None
        if selected is not None and not callable(selected):
            raise TypeError("WebExecutionProbe callback must be callable")
        object.__setattr__(self, "callback", selected)

    @property
    def checker(self) -> Callable[[], None] | None:
        return self.callback

    @property
    def check_fn(self) -> Callable[[], None] | None:
        return self.callback

    def check(self) -> None:
        if self.callback is not None:
            self.callback()


_WEB_EXECUTION_PROBE: ContextVar[WebExecutionProbe | None] = ContextVar(
    "sluice_web_execution_probe", default=None
)
# Public read-only-by-convention alias for integrations that need to install a
# token directly.  The accessor functions below are the preferred API.
WEB_EXECUTION_PROBE = _WEB_EXECUTION_PROBE


def get_web_execution_probe() -> WebExecutionProbe | None:
    """Return the probe installed for the current execution context, if any."""

    return _WEB_EXECUTION_PROBE.get()


def current_web_execution_probe() -> WebExecutionProbe | None:
    """Readable alias for :func:`get_web_execution_probe`."""

    return get_web_execution_probe()


def set_web_execution_probe(
    probe: WebExecutionProbe | None,
) -> Token[WebExecutionProbe | None]:
    if probe is not None and not isinstance(probe, WebExecutionProbe):
        raise TypeError("probe must be a WebExecutionProbe or null")
    return _WEB_EXECUTION_PROBE.set(probe)


def reset_web_execution_probe(token: Token[WebExecutionProbe | None]) -> None:
    _WEB_EXECUTION_PROBE.reset(token)


@dataclass(frozen=True, slots=True)
class _NoMatch:
    """Identity-only sentinel returned by deterministic extractors."""


NO_MATCH: Final[_NoMatch] = _NoMatch()


__all__ = [
    "ARTIFACT_CAPABILITIES",
    "REQUEST_CAPABILITIES",
    "STABLE_FAILURE_REASONS",
    "DocumentArtifact",
    "ExtractorBinding",
    "FetchChainExhausted",
    "FetchRequest",
    "NO_MATCH",
    "PrivateWebInvocation",
    "ProviderBinding",
    "QualityPolicy",
    "ValidatedFeedRoot",
    "WebAttemptCancelled",
    "WebAttemptFailure",
    "WebBinding",
    "WebExecutionProbe",
    "WebStageDeadlineExpired",
    "WEB_EXECUTION_PROBE",
    "current_web_execution_probe",
    "get_web_execution_probe",
    "reset_web_execution_probe",
    "set_web_execution_probe",
]
