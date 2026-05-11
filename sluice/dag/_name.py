from __future__ import annotations

import re

_NAME_RE = re.compile(r"^[a-zA-Z0-9_]+$")


def safe_name(name: str) -> str:
    if _NAME_RE.fullmatch(name) is None:
        raise ValueError(f"unsafe identifier: {name!r}")
    return name
