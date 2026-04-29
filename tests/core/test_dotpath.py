import pytest

from sluice.core.dotpath import set_dotpath
from sluice.core.errors import ConfigError
from sluice.core.item import Item


def _item(**kw):
    base = dict(
        source_id="s",
        pipeline_id="p",
        guid="g",
        url="u",
        title="t",
        published_at=None,
        raw_summary=None,
    )
    base.update(kw)
    return Item(**base)


def test_set_dotpath_extras():
    it = _item()
    set_dotpath(it, "extras.foo", "bar")
    assert it.extras["foo"] == "bar"


def test_set_dotpath_top_level():
    it = _item()
    set_dotpath(it, "title", "new title")
    assert it.title == "new title"


def test_set_dotpath_unsupported_path_raises():
    it = _item()
    with pytest.raises(ConfigError):
        set_dotpath(it, "a.b.c", "val")


def test_set_dotpath_invalid_attribute_raises():
    it = _item()
    with pytest.raises(ConfigError, match="no attribute"):
        set_dotpath(it, "nonexistent_field", "val")
