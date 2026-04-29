import pytest

from sluice.core.errors import ConfigError, PluginExtraMissingError
from sluice.registry import get_sink, register_sink_lazy


def test_lazy_stub_loads_on_first_get():
    register_sink_lazy("missing_extra_sink", "sluice.sinks._does_not_exist:Foo")
    with pytest.raises(PluginExtraMissingError) as exc:
        get_sink("missing_extra_sink")
    assert "missing_extra_sink" in str(exc.value)


def test_lazy_stub_loads_real_module():
    import types
    from unittest.mock import patch

    mod = types.ModuleType("_test_lazy_mod")

    class _Dummy:
        pass

    mod.Dummy = _Dummy
    register_sink_lazy("test_lazy_ok", "_test_lazy_mod:Dummy")
    with patch("importlib.import_module", return_value=mod):
        cls = get_sink("test_lazy_ok")
    assert cls is _Dummy


def test_unknown_sink_still_raises_config_error():
    with pytest.raises(ConfigError):
        get_sink("totally_unknown_sink_name")
