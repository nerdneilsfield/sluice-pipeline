from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from sluice.core.errors import ConfigError


@dataclass(frozen=True)
class RotationConfig:
    drain_timeout_seconds: float = 30 * 60.0
    archive_dir: str = "data/archive"
    default_cron: str | None = None


@dataclass(frozen=True)
class RuntimeConfig:
    """Runtime settings used by v2 Prefect deployment.

    ``image`` must reference an image that has already been built and pushed.
    The deploy command passes it to Prefect with ``build=False`` and
    ``push=False``.

    v3 core rollout flags:

    - ``runtime_core`` selects the runner core (``"v2"`` default; ``"v3"`` is
      experimental until v3.1 security hardening lands).
    - ``enable_outbox_dispatch`` gates the durable sink outbox dispatcher.
    - ``enable_lease_recovery`` gates lease/heartbeat/fencing recovery.
    - ``enable_rotation_manifest`` gates the sidecar rotation manifest.
    """

    prefect_api_url: str = ""
    prefect_api_auth_string: str = ""
    work_pool: str = "default"
    default_timezone: str = "UTC"
    image: str | None = None
    concurrency_strategy: str = "queue"
    runtime_core: str = "v2"
    enable_outbox_dispatch: bool = False
    enable_lease_recovery: bool = False
    enable_rotation_manifest: bool = False


@dataclass(frozen=True)
class SluiceConfig:
    rotation: RotationConfig = field(default_factory=RotationConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)


def _parse_duration(text: str | None, default: float) -> float:
    if text is None:
        return default
    value = text.strip().lower()
    if not value:
        return default
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd]?)", value)
    if match is None:
        raise ValueError(f"invalid duration: {text!r}")
    number = float(match.group(1))
    unit = match.group(2) or "s"
    multiplier = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}[unit]
    return number * multiplier


def load_sluice_config(config_path: Path | None = None) -> SluiceConfig:
    path = config_path or Path("sluice.toml")
    if not path.exists():
        return SluiceConfig()
    try:
        data = tomllib.loads(path.read_text())
    except UnicodeDecodeError as exc:
        raise ConfigError(f"failed to read sluice config {path}: invalid encoding") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"failed to parse sluice config {path}: {exc}") from exc
    rotation = data.get("gc", {}).get("rotation", {})
    runtime = data.get("runtime", {})
    concurrency_strategy = runtime.get("concurrency_strategy", "queue")
    if concurrency_strategy not in {"queue", "enqueue", "cancel_new"}:
        raise ConfigError(
            "runtime.concurrency_strategy must be one of queue/enqueue/cancel_new, "
            f"got {concurrency_strategy!r}"
        )
    runtime_core = runtime.get("runtime_core", "v2")
    if runtime_core not in {"v2", "v3"}:
        raise ConfigError(
            "runtime.runtime_core must be 'v2' or 'v3', "
            f"got {runtime_core!r}"
        )
    enable_outbox_dispatch = _require_bool(
        runtime, "enable_outbox_dispatch", default=False
    )
    enable_lease_recovery = _require_bool(
        runtime, "enable_lease_recovery", default=False
    )
    enable_rotation_manifest = _require_bool(
        runtime, "enable_rotation_manifest", default=False
    )
    return SluiceConfig(
        rotation=RotationConfig(
            drain_timeout_seconds=_parse_duration(rotation.get("drain_timeout", "30m"), 30 * 60.0),
            archive_dir=rotation.get("archive_dir", "data/archive"),
            default_cron=rotation.get("default_cron"),
        ),
        runtime=RuntimeConfig(
            prefect_api_url=runtime.get("prefect_api_url", ""),
            prefect_api_auth_string=runtime.get("prefect_api_auth_string", ""),
            work_pool=runtime.get("work_pool", "default"),
            default_timezone=runtime.get("default_timezone", "UTC"),
            image=runtime.get("image"),
            concurrency_strategy=concurrency_strategy,
            runtime_core=runtime_core,
            enable_outbox_dispatch=enable_outbox_dispatch,
            enable_lease_recovery=enable_lease_recovery,
            enable_rotation_manifest=enable_rotation_manifest,
        ),
    )


def _require_bool(data: dict, key: str, *, default: bool) -> bool:
    value = data.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(
            f"runtime.{key} must be a boolean, got {type(value).__name__}: {value!r}"
        )
    return value
