from collections.abc import Mapping
from typing import Any


def has_auth_header(headers: Mapping[str, Any]) -> bool:
    return any(key.lower() == "authorization" for key in headers)
