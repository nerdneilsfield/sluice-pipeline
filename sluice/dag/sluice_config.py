from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class RotationConfig:
    drain_timeout_seconds: float = 30 * 60.0
    archive_dir: str = "data/archive"
    default_cron: str | None = None


@dataclass(frozen=True)
class RuntimeConfig:
    prefect_api_url: str = ""
    prefect_api_auth_string: str = ""
    work_pool: str = "default"
    default_timezone: str = "UTC"
    image: str | None = None


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
    data = tomllib.loads(path.read_text())
    rotation = data.get("gc", {}).get("rotation", {})
    runtime = data.get("runtime", {})
    return SluiceConfig(
        rotation=RotationConfig(
            drain_timeout_seconds=_parse_duration(
                rotation.get("drain_timeout", "30m"), 30 * 60.0
            ),
            archive_dir=rotation.get("archive_dir", "data/archive"),
            default_cron=rotation.get("default_cron"),
        ),
        runtime=RuntimeConfig(
            prefect_api_url=runtime.get("prefect_api_url", ""),
            prefect_api_auth_string=runtime.get("prefect_api_auth_string", ""),
            work_pool=runtime.get("work_pool", "default"),
            default_timezone=runtime.get("default_timezone", "UTC"),
            image=runtime.get("image"),
        ),
    )
