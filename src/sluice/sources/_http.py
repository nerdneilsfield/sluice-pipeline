"""Bounded HTTP GET and option parsing shared by the built-in network Sources."""

from __future__ import annotations

import ipaddress
import os
import time
from collections.abc import Mapping

from sluice.errors import ExecutionError
from sluice.web.contracts import FetchRequest, ProviderBinding
from sluice.web.http import send_pinned_httpx_request
from sluice.web.url import (
    TargetPolicy,
    guard_resolved_addresses,
    normalize_target_url,
    validate_target_headers,
)


def fetch(
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    timeout: float,
    max_bytes: int,
    label: str,
    policy: TargetPolicy | None = None,
) -> bytes:
    """One DNS-pinned, redirect-free GET; anything but HTTP 200 is an error.

    Errors carry a fixed reason or the status only, never URL, headers or body.
    """

    checked_headers = validate_target_headers(headers)
    target = normalize_target_url(url, policy)
    addresses = guard_resolved_addresses(target.host, policy)
    request = FetchRequest(
        url=str(target),
        mode="article",
        acceptable=frozenset({"text"}),
        stage_deadline=time.monotonic() + timeout,
    )
    binding = ProviderBinding("direct", "httpx", max_body_bytes=max_bytes)
    response = send_pinned_httpx_request(
        target, addresses, request, binding, headers=checked_headers
    )
    if response.status_code != 200:
        raise ExecutionError(f"{label} HTTP {response.status_code}")
    return response.body


def resolve_env(value: str, label: str) -> str:
    """Resolve ``env:NAME`` at call time; any other value is returned as is."""

    if not value.startswith("env:"):
        return value
    resolved = os.environ.get(value[4:])
    if not resolved:
        raise ExecutionError(f"{label} environment variable {value[4:]} is not set")
    return resolved


def check_keys(config: Mapping[str, object], allowed: set[str], label: str) -> None:
    for key in config:
        if key not in allowed:
            raise ExecutionError(f"{label} does not support option {key}")


def int_option(
    config: Mapping[str, object],
    name: str,
    default: int,
    low: int,
    high: int,
    label: str,
) -> int:
    value = config.get(name, default)
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not low <= value <= high
    ):
        raise ExecutionError(f"{label} {name} must be an integer in [{low}, {high}]")
    return value


def timeout_seconds(config: Mapping[str, object], label: str) -> float:
    value = config.get("timeout_seconds", 30)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not 0 < value <= 300
    ):
        raise ExecutionError(f"{label} timeout_seconds must be in (0, 300]")
    return float(value)


def target_policy(config: Mapping[str, object], label: str) -> TargetPolicy:
    """Map ``target_allow_hosts``/``target_allow_cidrs`` onto a TargetPolicy.

    Same key names and rules as web providers (web/config.py); absent means
    the default public-only policy.
    """

    lists: dict[str, tuple[str, ...]] = {}
    for name in ("target_allow_hosts", "target_allow_cidrs"):
        value = config.get(name, ())
        if (
            not isinstance(value, (list, tuple))
            or any(not isinstance(item, str) or not item.strip() for item in value)
            or len(set(value)) != len(value)
        ):
            raise ExecutionError(
                f"{label} {name} must be a list of unique non-empty strings"
            )
        lists[name] = tuple(value)
    for cidr in lists["target_allow_cidrs"]:
        try:
            ipaddress.ip_network(cidr, strict=False)
        except ValueError:
            raise ExecutionError(
                f"{label} target_allow_cidrs contains an invalid CIDR"
            ) from None
    return TargetPolicy(lists["target_allow_hosts"], lists["target_allow_cidrs"])
