from sluice.core.errors import ConfigError

_SOURCES: dict[str, type] = {}
_FETCHERS: dict[str, type] = {}
_PROCESSORS: dict[str, type] = {}
_SINKS: dict[str, type] = {}

def _make_register(reg: dict[str, type], kind: str):
    def deco(name: str):
        def inner(cls):
            if name in reg:
                raise ConfigError(f"{kind} {name!r} already registered")
            reg[name] = cls
            return cls
        return inner
    return deco

def _make_get(reg: dict[str, type], kind: str):
    def get(name: str):
        if name not in reg:
            raise ConfigError(f"unknown {kind}: {name!r}")
        return reg[name]
    return get

register_source    = _make_register(_SOURCES,    "source")
register_fetcher   = _make_register(_FETCHERS,   "fetcher")
register_processor = _make_register(_PROCESSORS, "processor")
register_sink      = _make_register(_SINKS,      "sink")

get_source    = _make_get(_SOURCES,    "source")
get_fetcher   = _make_get(_FETCHERS,   "fetcher")
get_processor = _make_get(_PROCESSORS, "processor")
get_sink      = _make_get(_SINKS,      "sink")

def all_processors() -> dict[str, type]: return dict(_PROCESSORS)
def all_fetchers()   -> dict[str, type]: return dict(_FETCHERS)
def all_sources()    -> dict[str, type]: return dict(_SOURCES)
def all_sinks()      -> dict[str, type]: return dict(_SINKS)
