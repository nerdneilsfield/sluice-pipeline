import hashlib
from datetime import datetime, timezone

from sluice.core.item import Attachment, Item, compute_item_key


def make_item(**kw):
    base = dict(
        source_id="src1",
        pipeline_id="p1",
        guid="g1",
        url="https://example.com/a",
        title="hi",
        published_at=datetime(2026, 4, 28, tzinfo=timezone.utc),
        raw_summary=None,
    )
    base.update(kw)
    return Item(**base)


def test_get_dataclass_field():
    it = make_item(summary="hello")
    assert it.get("summary") == "hello"


def test_get_extras_field():
    it = make_item()
    it.extras["relevance"] = 7.5
    assert it.get("extras.relevance") == 7.5


def test_get_nested_attachment_index():
    it = make_item()
    it.attachments.append(Attachment(url="https://x/y.mp3", mime_type="audio/mpeg"))
    assert it.get("attachments.0.url") == "https://x/y.mp3"


def test_get_missing_returns_none():
    it = make_item()
    assert it.get("extras.nope") is None
    assert it.get("nonexistent") is None


def test_get_default_value():
    it = make_item()
    assert it.get("extras.score", default=0) == 0


def test_key_uses_guid():
    assert compute_item_key(make_item(guid="g-1")) == "g-1"


def test_key_falls_back_to_url():
    it = make_item()
    it.guid = None
    expected = hashlib.sha256(it.url.encode()).hexdigest()
    assert compute_item_key(it) == expected


def test_key_falls_back_to_title_plus_date():
    it = make_item()
    it.guid = None
    it.url = ""
    expected = hashlib.sha256((it.title + it.published_at.isoformat()).encode()).hexdigest()
    assert compute_item_key(it) == expected
