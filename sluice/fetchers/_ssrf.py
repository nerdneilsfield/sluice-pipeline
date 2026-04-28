import ipaddress
from urllib.parse import urlsplit

_BLOCKED_HOSTS = {
    "localhost", "127.0.0.1", "0.0.0.0", "::1",
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

class SSRFError(ValueError): pass

def guard(url: str) -> None:
    parts = urlsplit(url)
    host = parts.hostname
    if host is None:
        raise SSRFError(f"invalid URL: {url}")
    if host.lower() in _BLOCKED_HOSTS:
        raise SSRFError(f"blocked host: {host}")
    try:
        addr = ipaddress.ip_address(host)
        for net in _BLOCKED_NETWORKS:
            if addr in net:
                raise SSRFError(f"blocked private IP: {host}")
    except ValueError as e:
        if isinstance(e, SSRFError):
            raise
        pass  # hostname, not IP
