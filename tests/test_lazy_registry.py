import pytest

from sluice.core.errors import ConfigError, PluginExtraMissingError
from sluice.registry import get_sink, register_sink_lazy


def test_lazy_stub_loads_on_first_get():
    register_sink_lazy("missing_extra_sink", "sluice.sinks._does_not_exist:Foo")
    with pytest.raises(PluginExtraMissingError) as exc:
        get_sink("missing_extra_sink")
    assert "missing_extra_sink" in str(exc.value)


def test_lazy_stub_loads_real_module():
    register_sink_lazy("file_md_lazy_alias", "sluice.sinks.file_md:FileMdSink")
    cls = get_sink("file_md_lazy_alias")
    from sluice.sinks.file_md import FileMdSink

    assert cls is FileMdSink


def test_unknown_sink_still_raises_config_error():
    with pytest.raises(ConfigError):
        get_sink("totally_unknown_sink_name")
