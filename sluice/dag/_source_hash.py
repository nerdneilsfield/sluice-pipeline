from __future__ import annotations

import hashlib
from typing import Any


def source_hash_from_row(row: Any) -> str:
    base = f"{row['source_name']}::{row['guid'] or row['url'] or ''}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()
