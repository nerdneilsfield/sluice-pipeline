import importlib

from sluice.core.errors import ConfigError, PluginExtraMissingError

_SOURCES: dict[str, type] = {}
_FETCHERS: dict[str, type] = {}
_PROCESSORS: dict[str, type] = {}
_SINKS: dict[str, type] = {}
_ENRICHERS: dict[str, type] = {}

_LAZY_SOURCES: dict[str, str] = {}
_LAZY_FETCHERS: dict[str, str] = {}
_LAZY_PROCESSORS: dict[str, str] = {}
_LAZY_SINKS: dict[str, str] = {}
_LAZY_ENRICHERS: dict[str, str] = {}


def _make_register(reg: dict[str, type], kind: str):
    def deco(name: str):
        def inner(cls):
            if name in reg:
                raise ConfigError(f"{kind} {name!r} already registered")
            reg[name] = cls
            return cls

        return inner

    return deco


def _make_lazy_register(lazy_reg: dict[str, str], reg: dict[str, type], kind: str):
    def register(name: str, dotted_path: str):
        if name in reg or name in lazy_reg:
            raise ConfigError(f"{kind} {name!r} already registered")
        lazy_reg[name] = dotted_path

    return register


def _resolve_lazy(name: str, dotted_path: str, kind: str) -> type:
    module_path, _, attr = dotted_path.partition(":")
    try:
        mod = importlib.import_module(module_path)
    except (ImportError, ModuleNotFoundError) as exc:
        raise PluginExtraMissingError(f"{kind} {name!r} requires missing extra: {exc}") from exc
    cls = getattr(mod, attr, None)
    if cls is None:
        raise PluginExtraMissingError(f"{kind} {name!r}: {dotted_path!r} has no attribute {attr!r}")
    return cls


def _make_get_with_lazy(reg: dict[str, type], lazy_reg: dict[str, str], kind: str):
    def get(name: str):
        if name in reg:
            return reg[name]
        if name in lazy_reg:
            cls = _resolve_lazy(name, lazy_reg[name], kind)
            reg[name] = cls
            del lazy_reg[name]
            return cls
        raise ConfigError(f"unknown {kind}: {name!r}")

    return get


register_source = _make_register(_SOURCES, "source")
register_fetcher = _make_register(_FETCHERS, "fetcher")
register_processor = _make_register(_PROCESSORS, "processor")
register_sink = _make_register(_SINKS, "sink")
register_enricher = _make_register(_ENRICHERS, "enricher")

register_source_lazy = _make_lazy_register(_LAZY_SOURCES, _SOURCES, "source")
register_fetcher_lazy = _make_lazy_register(_LAZY_FETCHERS, _FETCHERS, "fetcher")
register_processor_lazy = _make_lazy_register(_LAZY_PROCESSORS, _PROCESSORS, "processor")
register_sink_lazy = _make_lazy_register(_LAZY_SINKS, _SINKS, "sink")
register_enricher_lazy = _make_lazy_register(_LAZY_ENRICHERS, _ENRICHERS, "enricher")

get_source = _make_get_with_lazy(_SOURCES, _LAZY_SOURCES, "source")
get_fetcher = _make_get_with_lazy(_FETCHERS, _LAZY_FETCHERS, "fetcher")
get_processor = _make_get_with_lazy(_PROCESSORS, _LAZY_PROCESSORS, "processor")
get_sink = _make_get_with_lazy(_SINKS, _LAZY_SINKS, "sink")
get_enricher = _make_get_with_lazy(_ENRICHERS, _LAZY_ENRICHERS, "enricher")


def all_processors() -> dict[str, type]:
    _flush_lazy(_LAZY_PROCESSORS, _PROCESSORS, "processor")
    return dict(_PROCESSORS)


def all_fetchers() -> dict[str, type]:
    _flush_lazy(_LAZY_FETCHERS, _FETCHERS, "fetcher")
    return dict(_FETCHERS)


def all_sources() -> dict[str, type]:
    _flush_lazy(_LAZY_SOURCES, _SOURCES, "source")
    return dict(_SOURCES)


def all_sinks() -> dict[str, type]:
    _flush_lazy(_LAZY_SINKS, _SINKS, "sink")
    return dict(_SINKS)


def all_enrichers() -> dict[str, type]:
    _flush_lazy(_LAZY_ENRICHERS, _ENRICHERS, "enricher")
    return dict(_ENRICHERS)


def _flush_lazy(lazy_reg: dict[str, str], reg: dict[str, type], kind: str):
    for name, dotted_path in list(lazy_reg.items()):
        cls = _resolve_lazy(name, dotted_path, kind)
        reg[name] = cls
        del lazy_reg[name]
