import ipaddress
import socket
from urllib.parse import urlsplit

import httpx

_BLOCKED_HOSTS = {
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "::1",
}

_BLOCKED_NETWORKS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]


class SSRFError(ValueError):
    pass


def _is_blocked_ip(host: str) -> bool:
    try:
        addr = ipaddress.ip_address(host)
        for net in _BLOCKED_NETWORKS:
            if addr in net:
                return True
        return False
    except ValueError:
        return False


def _check_host(host: str) -> None:
    if host is None:
        raise SSRFError("invalid URL: missing hostname")
    host_lower = host.lower()
    if host_lower in _BLOCKED_HOSTS:
        raise SSRFError(f"blocked host: {host}")
    if _is_blocked_ip(host_lower):
        raise SSRFError(f"blocked private IP: {host}")
    # Resolve hostname and check all A/AAAA records
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror as e:
        raise SSRFError(f"cannot resolve host {host}: {e}") from e
    for info in infos:
        resolved_ip = str(info[4][0])
        if _is_blocked_ip(resolved_ip):
            raise SSRFError(f"blocked private IP resolved from {host}: {resolved_ip}")


def guard(url: str) -> None:
    parts = urlsplit(url)
    host = parts.hostname
    if host is None:
        raise SSRFError("invalid URL: missing hostname")
    _check_host(host)


_MAX_REDIRECTS = 10


def _resolve_redirects(response: httpx.Response) -> str:
    """Return the final URL after following redirects, guarding each hop."""
    history = list(response.history) + [response]
    for redirect_response in history[1:]:
        location = redirect_response.headers.get("location")
        if location:
            guard(location)
    return str(response.url)


def get_final_url(response: httpx.Response) -> str:
    """Guard the initial URL and all redirect hops."""
    guard(str(response.url))
    if response.history:
        return _resolve_redirects(response)
    return str(response.url)
