from __future__ import annotations

import json
from typing import Any

ITEM_COLUMNS: frozenset[str] = frozenset(
    {
        "item_id",
        "source_name",
        "guid",
        "url",
        "title",
        "author",
        "published_at",
        "ingested_at",
        "ingested_by",
        "updated_at",
        "tag",
        "raw_summary",
        "raw_content",
        "raw_json",
        "fulltext",
        "attachments_json",
        "summary",
        "score",
        "tags_json",
        "extras_json",
        "dropped",
        "dropped_reason",
        "dropped_at",
    }
)


class Item:
    __slots__ = ("_data", "_dirty", "_volatile", "_dropped", "_drop_reason")

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._dirty: set[str] = set()
        self._volatile: dict[str, Any] = {}
        self._dropped = False
        self._drop_reason: str | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Item":
        bad = set(row) - ITEM_COLUMNS
        if bad:
            raise KeyError(f"unknown item columns: {sorted(bad)!r}")
        item = cls()
        item._data = dict(row)
        item._dropped = bool(item._data.get("dropped", 0))
        item._drop_reason = item._data.get("dropped_reason")
        return item

    def __getitem__(self, key: str) -> Any:
        if key not in ITEM_COLUMNS:
            raise KeyError(f"unknown item column: {key!r}")
        return self._data.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        if key not in ITEM_COLUMNS:
            raise KeyError(f"unknown item column: {key!r}")
        if self._data.get(key) != value:
            self._data[key] = value
            self._dirty.add(key)

    def dirty_columns(self) -> frozenset[str]:
        return frozenset(self._dirty)

    def to_dirty_dict(self) -> dict[str, Any]:
        return {key: self._data[key] for key in self._dirty}

    def mark_dropped(self, reason: str) -> None:
        self._dropped = True
        self._drop_reason = reason

    def is_dropped(self) -> bool:
        return self._dropped

    def drop_reason(self) -> str | None:
        return self._drop_reason

    def set_extra(self, key: str, value: Any) -> None:
        raw = self._data.get("extras_json")
        existing: dict[str, Any] = json.loads(raw) if raw else {}
        existing[key] = value
        self._data["extras_json"] = json.dumps(existing, ensure_ascii=False)
        self._dirty.add("extras_json")

    def get_extra(self, key: str, default: Any = None) -> Any:
        raw = self._data.get("extras_json")
        if not raw:
            return default
        return json.loads(raw).get(key, default)

    def set_volatile(self, key: str, value: Any) -> None:
        self._volatile[key] = value

    def get_volatile(self, key: str, default: Any = None) -> Any:
        return self._volatile.get(key, default)
