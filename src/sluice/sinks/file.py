"""One local file per Delivery, confined to the definition's ``file_root``."""

from __future__ import annotations

import os
import secrets
from collections.abc import Callable, Mapping
from functools import partial
from pathlib import Path

from ..contracts import JSONValue
from ..errors import ExecutionError
from ..sources._http import check_keys
from ._common import message_text

SINK_PATH = "sluice.sinks.file"
_KEYS = {"directory", "extension"}
_MAX_BYTES = 10 * 1024 * 1024


def resolve_file_root(file_root: str | None, base_dir: Path) -> Path:
    """The runtime root: ``file_root`` relative to the definition dir, default it."""

    return (base_dir / (file_root or ".")).resolve()


def resolve_inside(root: Path, directory: object, error: type[Exception]) -> Path:
    """Resolve ``root/directory`` (symlinks followed); it must stay inside root."""

    if not isinstance(directory, str) or not directory:
        raise error("File Sink directory must be a non-empty string")
    try:
        resolved = (root / directory).resolve()
    except (OSError, RuntimeError, ValueError):
        raise error("File Sink directory cannot be resolved") from None
    if not resolved.is_relative_to(root):
        raise error("File Sink directory must be inside file_root")
    return resolved


def send(
    config: Mapping[str, JSONValue],
    payload: JSONValue,
    delivery_key: str,
    *,
    file_root: Path | None,
) -> str:
    check_keys(config, _KEYS, "File Sink")
    extension = config.get("extension", "txt")
    if extension not in {"txt", "md"}:
        raise ExecutionError("File Sink extension must be txt or md")
    if file_root is None:
        raise ExecutionError("File Sink has no file_root")
    data = message_text(payload).encode()
    if len(data) > _MAX_BYTES:
        raise ExecutionError("File Sink content exceeds 10 MiB")
    name = f"{delivery_key}.{extension}"
    temp = f".{name}.{secrets.token_hex(8)}.tmp"
    directory = resolve_inside(file_root, config.get("directory"), ExecutionError)
    try:
        # shortcut: a symlink swapped in between this check and the open is not
        # excluded; the dir fd only pins the directory once it is open.
        directory.mkdir(parents=True, exist_ok=True)
        dir_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            try:
                fd = os.open(
                    temp,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=dir_fd,
                )
                with os.fdopen(fd, "wb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
            except BaseException:
                try:
                    os.unlink(temp, dir_fd=dir_fd)
                except OSError:
                    pass
                raise
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        raise ExecutionError("File Sink write failed") from None
    return name


class _FileSink:
    def __init__(self, client):
        self.client = client

    def send(self, config, rendered_payload, delivery_key):
        return self.client(config, rendered_payload, delivery_key)


def bind(
    client: Callable[[Mapping[str, JSONValue], JSONValue, str], str | None]
    | None = None,
    *,
    file_root: Path | None = None,
) -> _FileSink:
    return _FileSink(partial(send, file_root=file_root) if client is None else client)
