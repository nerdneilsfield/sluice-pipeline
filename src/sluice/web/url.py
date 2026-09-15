"""URL normalization and target SSRF policy primitives.

The web pipeline treats a target URL as untrusted input.  This module keeps
the parsing/policy boundary small and deterministic so every direct provider
can share the same checks before opening a socket or following a redirect.
"""

from __future__ import annotations

import ipaddress
import re
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import (
    parse_qsl,
    quote,
    urljoin,
    urlsplit,
    urlunsplit,
)

from ..errors import WebSafetyError

MAX_REDIRECTS = 10
_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_CONTROL = re.compile(r"[\x00-\x20\x7f]")
_HEADER_CONTROL = re.compile(r"[\x00-\x1f\x7f]")
_NUMERIC_HOST = re.compile(r"^(?:0[xX][0-9A-Fa-f]+|[0-9]+|[0-9.]+)$")
_NUMERIC_IPV4_COMPONENT = re.compile(r"^(?:0[xX][0-9A-Fa-f]+|[0-9]+)$")
_SAFE_HEADER_NAME = re.compile(r"^[^\x00-\x20\x7f:]+$")
_FORBIDDEN_HEADERS = frozenset(
    {
        "host",
        ":authority",
        "connection",
        "content-length",
        "transfer-encoding",
        "proxy-authorization",
        "upgrade",
        "forwarded",
        "via",
        "cookie",
    }
)


def _error(reason: str) -> WebSafetyError:
    """Construct an exception with only a stable, non-sensitive reason."""

    exc = WebSafetyError(reason)
    # ``WebSafetyError`` intentionally remains a tiny core exception.  The
    # attribute is useful to adapters while ``str(exc)`` remains safe.
    try:
        exc.reason = reason
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        pass
    return exc


def _idna_host(host: str) -> str:
    if not isinstance(host, str) or not host:
        raise _error("bad_url")
    if _CONTROL.search(host) or _PERCENT_ESCAPE.search(host):
        raise _error("bad_url")
    # A trailing dot is DNS-equivalent and is removed for exact allowlist
    # comparisons.  Empty labels elsewhere remain invalid.
    host = host.rstrip(".")
    if not host or ".." in host:
        raise _error("bad_url")
    try:
        ascii_host = host.encode("idna").decode("ascii").lower()
    except (UnicodeError, ValueError):
        raise _error("bad_url") from None
    if not ascii_host or len(ascii_host) > 253:
        raise _error("bad_url")
    if any(not label or len(label) > 63 for label in ascii_host.split(".")):
        raise _error("bad_url")
    try:
        ipaddress.ip_address(ascii_host)
        is_literal = True
    except ValueError:
        is_literal = False
    if not is_literal:
        for label in ascii_host.split("."):
            if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label):
                raise _error("bad_url")
    # Reject alternate integer/hex/octal spellings used to disguise IPv4
    # literals.  Canonical dotted decimal IPv4 is handled by ipaddress below.
    if _NUMERIC_HOST.fullmatch(host) and "." not in host:
        raise _error("bad_url")
    if "." in host and all(
        _NUMERIC_IPV4_COMPONENT.fullmatch(label) for label in host.split(".")
    ):
        # Numeric dotted hosts must be canonical four-octet decimal IPv4.
        # Reject shortened (127.1), overflow (1.2.3.999), zero-padded, and
        # mixed-base spellings (0x7f.0.0.1) which libc resolvers may interpret
        # as an address even though ``ipaddress`` correctly refuses them.
        try:
            parsed_ip = ipaddress.ip_address(host)
        except ValueError:
            raise _error("bad_url") from None
        if not isinstance(parsed_ip, ipaddress.IPv4Address) or str(parsed_ip) != host:
            raise _error("bad_url")
    return ascii_host


def _normalise_ip_for_policy(value: str) -> ipaddress._BaseAddress | None:
    try:
        parsed = ipaddress.ip_address(value)
    except ValueError:
        return None
    if isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped is not None:
        return parsed.ipv4_mapped
    return parsed


def _is_disallowed_address(address: ipaddress._BaseAddress) -> bool:
    # ``is_private`` does not cover carrier-grade NAT on all Python releases.
    cgn = ipaddress.ip_network("100.64.0.0/10")
    metadata = ipaddress.ip_network("169.254.169.254/32")
    return bool(
        address.is_loopback
        or address.is_private
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
        or address in cgn
        or address in metadata
    )


def is_non_public_address(address: str) -> bool:
    """Return whether *address* belongs to a non-public network range.

    Service endpoints use this narrow classification after their exact host
    has been allowlisted.  It intentionally mirrors the target SSRF guard,
    while keeping the helper read-only and free of DNS resolution.
    """

    parsed = _normalise_ip_for_policy(str(address))
    return parsed is not None and _is_disallowed_address(parsed)


@dataclass(frozen=True, slots=True)
class TargetPolicy:
    """Allowlist additions to the conservative default target policy.

    Exact host names and CIDRs are the only supported exceptions.  The class
    accepts a few spelling aliases through :meth:`from_value` because retained
    provider snapshots use both authoring and runtime field names.
    """

    allow_hosts: tuple[str, ...] = ()
    allow_cidrs: tuple[str, ...] = ()
    allow_remote_dns_proxy: bool = False

    def __post_init__(self) -> None:
        hosts: list[str] = []
        for host in self.allow_hosts:
            hosts.append(_idna_host(str(host)))
        networks: list[str] = []
        for value in self.allow_cidrs:
            try:
                networks.append(str(ipaddress.ip_network(str(value), strict=False)))
            except ValueError:
                raise _error("bad_url") from None
        object.__setattr__(self, "allow_hosts", tuple(dict.fromkeys(hosts)))
        object.__setattr__(self, "allow_cidrs", tuple(dict.fromkeys(networks)))

    @property
    def target_allow_hosts(self) -> tuple[str, ...]:
        return self.allow_hosts

    @property
    def target_allow_cidrs(self) -> tuple[str, ...]:
        return self.allow_cidrs

    @classmethod
    def from_value(cls, value: Any = None) -> TargetPolicy:
        if value is None:
            return DEFAULT_TARGET_POLICY
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise _error("bad_url")
        hosts = value.get("allow_hosts", value.get("target_allow_hosts", ()))
        cidrs = value.get("allow_cidrs", value.get("target_allow_cidrs", ()))
        if isinstance(hosts, str):
            hosts = (hosts,)
        if isinstance(cidrs, str):
            cidrs = (cidrs,)
        if hosts is None:
            hosts = ()
        if cidrs is None:
            cidrs = ()
        if not isinstance(hosts, (list, tuple, set, frozenset)) or not isinstance(
            cidrs, (list, tuple, set, frozenset)
        ):
            raise _error("bad_url")
        allow_remote = value.get("allow_remote_dns_proxy", False)
        if not isinstance(allow_remote, bool):
            raise _error("bad_url")
        return cls(
            tuple(str(item) for item in hosts),
            tuple(str(item) for item in cidrs),
            allow_remote,
        )


DEFAULT_TARGET_POLICY = TargetPolicy()


@dataclass(frozen=True, slots=True, eq=False)
class NormalizedURL:
    """Canonical HTTP(S) target URL with convenient URL component access."""

    scheme: str
    host: str
    port: int
    path: str = "/"
    query: str = ""

    @property
    def hostname(self) -> str:
        return self.host

    @property
    def netloc(self) -> str:
        host = f"[{self.host}]" if ":" in self.host else self.host
        default = (self.scheme == "http" and self.port == 80) or (
            self.scheme == "https" and self.port == 443
        )
        return host if default else f"{host}:{self.port}"

    @property
    def url(self) -> str:
        return str(self)

    @property
    def value(self) -> str:
        return str(self)

    @property
    def parsed(self):
        return urlsplit(str(self))

    @property
    def fragment(self) -> str:
        return ""

    @property
    def username(self) -> None:
        return None

    @property
    def password(self) -> None:
        return None

    def __str__(self) -> str:
        return urlunsplit((self.scheme, self.netloc, self.path or "/", self.query, ""))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, NormalizedURL):
            return str(self) == str(other)
        if isinstance(other, str):
            return str(self) == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(str(self))

    def __fspath__(self) -> str:
        return str(self)


def normalize_target_url(
    url: str,
    target_policy: TargetPolicy | Mapping[str, Any] | None = None,
    *,
    policy: TargetPolicy | Mapping[str, Any] | None = None,
) -> NormalizedURL:
    """Parse and canonicalize an HTTP(S) target, rejecting unsafe syntax."""

    if (
        not isinstance(url, str)
        or not url
        or _CONTROL.search(url)
        or _PERCENT_ESCAPE.search(url)
    ):
        raise _error("bad_url")
    try:
        parsed = urlsplit(url)
        scheme = parsed.scheme.lower()
        if scheme not in {"http", "https"} or not parsed.netloc:
            raise _error("bad_url")
        # urlsplit exposes decoded userinfo separately.  Also check the raw
        # netloc because malformed percent escapes must never be accepted.
        if (
            parsed.username is not None
            or parsed.password is not None
            or "@" in parsed.netloc
        ):
            raise _error("bad_url")
        host = parsed.hostname
        if not host:
            raise _error("bad_url")
        host = _idna_host(host)
        try:
            port = parsed.port
        except ValueError:
            raise _error("bad_url") from None
        if port is None:
            port = 80 if scheme == "http" else 443
        if not 1 <= port <= 65535:
            raise _error("bad_url")
    except WebSafetyError:
        raise
    except (ValueError, UnicodeError):
        raise _error("bad_url") from None

    path = parsed.path or "/"
    if _CONTROL.search(path) or _CONTROL.search(parsed.query):
        raise _error("bad_url")
    # A fragment is never transmitted to a server.  Discarding it avoids
    # allowing a different diagnostic URL to masquerade as a target.
    result = NormalizedURL(scheme, host, port, path, parsed.query)
    if policy is not None:
        if target_policy is not None:
            raise _error("bad_url")
        target_policy = policy
    target_policy_obj = TargetPolicy.from_value(target_policy)
    if (
        host in {"localhost", "localhost.localdomain", "ip6-localhost"}
        and host not in target_policy_obj.allow_hosts
    ):
        raise _error("ssrf_blocked")
    literal = _normalise_ip_for_policy(host)
    if (
        literal is not None
        and _is_disallowed_address(literal)
        and not _address_allowed(host, literal, target_policy_obj)
    ):
        raise _error("ssrf_blocked")
    return result


def normalize_target_url_string(
    url: str, target_policy: TargetPolicy | Mapping[str, Any] | None = None
) -> str:
    """String-returning compatibility alias for callers that need a URL key."""

    return str(normalize_target_url(url, target_policy))


def resolve_host(host: str) -> list[str]:
    """Resolve *host* to unique textual addresses without retaining metadata."""

    literal = _normalise_ip_for_policy(host)
    if literal is not None:
        return [str(literal)]
    try:
        answers = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except (OSError, UnicodeError):
        raise _error("ssrf_blocked") from None
    result: list[str] = []
    for answer in answers:
        sockaddr = answer[4]
        if not sockaddr:
            continue
        candidate = _normalise_ip_for_policy(str(sockaddr[0]))
        if candidate is not None and str(candidate) not in result:
            result.append(str(candidate))
    if not result:
        raise _error("ssrf_blocked")
    return result


def _address_allowed(
    host: str, address: ipaddress._BaseAddress, policy: TargetPolicy
) -> bool:
    normalized_host = host.lower().rstrip(".")
    # Host allowlists name DNS identities.  A literal non-public address must
    # be authorized by an explicit CIDR so the network scope is visible in
    # configuration rather than smuggled through the hostname exception.
    if (
        _normalise_ip_for_policy(normalized_host) is None
        and normalized_host in policy.allow_hosts
    ):
        return True
    for cidr in policy.allow_cidrs:
        network = ipaddress.ip_network(cidr)
        if address.version == network.version and address in network:
            return True
    return not _is_disallowed_address(address)


def guard_resolved_addresses(
    host: str,
    target_policy: TargetPolicy | Mapping[str, Any] | None = None,
    *,
    policy: TargetPolicy | Mapping[str, Any] | None = None,
) -> tuple[str, ...]:
    """Resolve *host* and reject any address outside the target policy.

    A mixed public/private DNS answer is rejected as soon as any returned
    address is disallowed; callers must not select a seemingly safe subset.
    """

    if not isinstance(host, str) or not host:
        raise _error("ssrf_blocked")
    if policy is not None:
        if target_policy is not None:
            raise _error("bad_url")
        target_policy = policy
    policy = TargetPolicy.from_value(target_policy)
    try:
        normalized_host = _idna_host(host)
        addresses = tuple(resolve_host(normalized_host))
    except WebSafetyError:
        raise
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        raise _error("ssrf_blocked") from None
    if not addresses:
        raise _error("ssrf_blocked")
    normalized: list[str] = []
    has_public = False
    has_non_public = False
    for raw in addresses:
        candidate = _normalise_ip_for_policy(str(raw))
        if candidate is None:
            raise _error("ssrf_blocked")
        text = str(candidate)
        if text not in normalized:
            normalized.append(text)
        if _is_disallowed_address(candidate):
            has_non_public = True
        else:
            has_public = True
        if not _address_allowed(normalized_host, candidate, policy):
            raise _error("ssrf_blocked")
    if has_public and has_non_public:
        raise _error("ssrf_blocked")
    return tuple(normalized)


def validate_target_headers(headers: Mapping[str, Any] | None) -> dict[str, str]:
    """Validate user-supplied target headers and return a detached copy."""

    if headers is None:
        return {}
    if not isinstance(headers, Mapping):
        raise _error("forbidden_header")
    result: dict[str, str] = {}
    seen: set[str] = set()
    for name, value in headers.items():
        if not isinstance(name, str) or not _SAFE_HEADER_NAME.fullmatch(name):
            raise _error("forbidden_header")
        normalized = name.casefold()
        if normalized in seen:
            raise _error("forbidden_header")
        seen.add(normalized)
        if normalized in _FORBIDDEN_HEADERS or normalized.startswith("x-forwarded-"):
            raise _error("forbidden_header")
        if not isinstance(value, str) or _HEADER_CONTROL.search(value):
            raise _error("forbidden_header")
        result[name] = value
    return result


def redact_url(url: str) -> str:
    """Return a diagnostic URL with userinfo/fragments removed and values redacted."""

    if not isinstance(url, str):
        return "<redacted-url>"
    try:
        parsed = urlsplit(url)
    except ValueError:
        return "<redacted-url>"
    scheme = parsed.scheme.lower()
    host = parsed.hostname or ""
    try:
        host = _idna_host(host)
    except WebSafetyError:
        host = "<invalid-host>"
    try:
        port = parsed.port
    except ValueError:
        port = None
    if ":" in host:
        netloc = f"[{host}]"
    else:
        netloc = host
    if port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        netloc += f":{port}"
    query_parts: list[str] = []
    if parsed.query:
        # Retain keys for useful correlation but never retain any decoded
        # value, including blank values.
        for key, _value in parse_qsl(parsed.query, keep_blank_values=True):
            query_parts.append(f"{quote(key, safe="[]!$&'()*+,;:@/?")}=<redacted>")
        if not query_parts:
            for raw_key in parsed.query.split("&"):
                key = raw_key.split("=", 1)[0]
                query_parts.append(f"{key}=<redacted>")
    return urlunsplit((scheme, netloc, parsed.path or "/", "&".join(query_parts), ""))


def join_target_url(
    base: str | NormalizedURL,
    location: str,
    target_policy: TargetPolicy | Mapping[str, Any] | None = None,
) -> NormalizedURL:
    """Resolve a redirect/link location against a normalized HTTP(S) base."""

    if isinstance(base, NormalizedURL):
        base_value = str(base)
    else:
        base_value = str(base)
    if not isinstance(location, str) or _CONTROL.search(location):
        raise _error("bad_url")
    try:
        joined = urljoin(base_value, location)
    except ValueError:
        raise _error("bad_url") from None
    return normalize_target_url(joined, target_policy)


__all__ = [
    "DEFAULT_TARGET_POLICY",
    "MAX_REDIRECTS",
    "NormalizedURL",
    "TargetPolicy",
    "guard_resolved_addresses",
    "is_non_public_address",
    "join_target_url",
    "normalize_target_url",
    "normalize_target_url_string",
    "redact_url",
    "resolve_host",
    "validate_target_headers",
]
