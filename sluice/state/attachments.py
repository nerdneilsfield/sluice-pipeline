from sluice.core.errors import ConfigError


def format_url(local_path: str, prefix: str) -> str:
    if prefix == "":
        return local_path
    if prefix.startswith(("http://", "https://", "file://")):
        return f"{prefix.rstrip('/')}/{local_path.lstrip('/')}"
    raise ConfigError(
        f"attachment_url_prefix must be empty, http(s)://..., or file://...; got {prefix!r}"
    )
