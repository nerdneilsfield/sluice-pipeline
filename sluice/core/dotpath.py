from sluice.core.errors import ConfigError
from sluice.core.item import Item


def set_dotpath(item: Item, path: str, value) -> None:
    parts = path.split(".")
    if parts[0] == "extras" and len(parts) == 2:
        item.extras[parts[1]] = value
        return
    if len(parts) == 1:
        if not hasattr(item, parts[0]):
            raise ConfigError(f"Item has no attribute {parts[0]!r}")
        setattr(item, parts[0], value)
        return
    raise ConfigError(f"cannot set dot-path {path!r} on Item")
