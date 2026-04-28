import ipaddress
import os
import socket
from urllib.parse import urljoin, urlsplit

import httpx

from sluice.logging_setup import get_logger

_ALLOWED_SCHEMES = {"http", "https"}
_TUN_FAKE_IP_NET = ipaddress.ip_network("198.18.0.0/15")
_BLOCKED_HOSTS = {
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "::1",
}


class SSRFError(ValueError):
    pass


log = get_logger(__name__)


def _allow_tun_fake_ip() -> bool:
    return os.environ.get("SLUICE_SSRF_ALLOW_TUN_FAKE_IP", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _is_tun_fake_ip(addr: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return isinstance(addr, ipaddress.IPv4Address) and addr in _TUN_FAKE_IP_NET


def _is_blocked_ip(host: str, *, allow_tun_fake_ip: bool = False) -> bool:
    try:
        addr = ipaddress.ip_address(host)
        if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
            addr = addr.ipv4_mapped
        if allow_tun_fake_ip and _is_tun_fake_ip(addr):
            return False
        return not addr.is_global
    except ValueError:
        return False


def _check_host(host: str) -> None:
    if host is None:
        raise SSRFError("invalid URL: missing hostname")
    host_lower = host.rstrip(".").lower()
    if host_lower in _BLOCKED_HOSTS:
        raise SSRFError(f"blocked host: {host}")
    if _is_blocked_ip(host_lower):
        raise SSRFError(f"blocked private IP: {host}")
    # Resolve hostname and check all A/AAAA records
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror as e:
        raise SSRFError(f"cannot resolve host {host}: {e}") from e
    for info in infos:
        resolved_ip = str(info[4][0])
        allow_tun_fake_ip = _allow_tun_fake_ip()
        if allow_tun_fake_ip and _is_tun_fake_ip(ipaddress.ip_address(resolved_ip)):
            log.bind(host=host, resolved_ip=resolved_ip).debug("ssrf.tun_fake_ip_allowed")
            continue
        if _is_blocked_ip(resolved_ip, allow_tun_fake_ip=allow_tun_fake_ip):
            raise SSRFError(f"blocked private IP resolved from {host}: {resolved_ip}")


def guard(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme not in _ALLOWED_SCHEMES:
        raise SSRFError(f"invalid URL scheme: {parts.scheme!r}")
    host = parts.hostname
    if host is None:
        raise SSRFError("invalid URL: missing hostname")
    _check_host(host)


_MAX_REDIRECTS = 10


def guarded_redirect_url(current_url: str, location: str) -> str:
    next_url = urljoin(current_url, location)
    guard(next_url)
    return next_url


def guard_response(response: httpx.Response) -> None:
    guard(str(response.url))
    stream = response.extensions.get("network_stream")
    if stream is None:
        return
    remote = stream.get_extra_info("remote_address")
    if not remote:
        return
    remote_host = remote[0] if isinstance(remote, tuple) else remote
    if _is_blocked_ip(str(remote_host), allow_tun_fake_ip=_allow_tun_fake_ip()):
        raise SSRFError(f"blocked private peer IP: {remote_host}")


def _resolve_redirects(response: httpx.Response) -> str:
    """Return the final URL after following redirects, guarding each hop."""
    history = list(response.history) + [response]
    for redirect_response in history:
        guard_response(redirect_response)
    return str(response.url)


def get_final_url(response: httpx.Response) -> str:
    """Guard the initial URL and all redirect hops."""
    guard_response(response)
    if response.history:
        return _resolve_redirects(response)
    return str(response.url)
