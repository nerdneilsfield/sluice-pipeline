from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

_SENTINEL = object()


@dataclass
class Attachment:
    url: str
    mime_type: str | None = None
    length: int | None = None


@dataclass
class Item:
    source_id: str
    pipeline_id: str
    guid: str | None
    url: str
    title: str
    published_at: datetime | None
    raw_summary: str | None
    fulltext: str | None = None
    attachments: list[Attachment] = field(default_factory=list)
    summary: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)

    def get(self, path: str, default: Any = None) -> Any:
        cur: Any = self
        for part in path.split("."):
            if isinstance(cur, dict):
                cur = cur.get(part, _SENTINEL)
            elif isinstance(cur, list):
                try:
                    cur = cur[int(part)]
                except (ValueError, IndexError):
                    return default
            else:
                cur = getattr(cur, part, _SENTINEL)
            if cur is _SENTINEL:
                return default
        return cur


def compute_item_key(item: "Item") -> str:
    import hashlib

    if item.guid and item.guid.strip():
        return item.guid.strip()
    if item.url:
        return hashlib.sha256(item.url.encode()).hexdigest()
    pub = item.published_at.isoformat() if item.published_at else ""
    return hashlib.sha256((item.title + pub).encode()).hexdigest()
