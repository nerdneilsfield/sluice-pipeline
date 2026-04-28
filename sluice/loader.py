import tomllib
from dataclasses import dataclass
from pathlib import Path

from sluice.config import GlobalConfig, PipelineConfig, ProvidersConfig
from sluice.core.errors import ConfigError


@dataclass
class ConfigBundle:
    global_cfg: GlobalConfig
    providers: ProvidersConfig
    pipelines: dict[str, PipelineConfig]
    root: Path


def _load(path: Path) -> dict:
    if not path.exists():
        raise ConfigError(f"missing config: {path}")
    return tomllib.loads(path.read_text())


def load_all(root: Path) -> ConfigBundle:
    root = Path(root)
    global_cfg = GlobalConfig.model_validate(_load(root / "sluice.toml"))
    providers = ProvidersConfig.model_validate(_load(root / "providers.toml"))
    pipes_dir = root / "pipelines"
    pipelines: dict[str, PipelineConfig] = {}
    if pipes_dir.exists():
        for f in sorted(pipes_dir.glob("*.toml")):
            cfg = PipelineConfig.model_validate(_load(f))
            if cfg.id in pipelines:
                raise ConfigError(f"duplicate pipeline id {cfg.id} in {f}")
            pipelines[cfg.id] = cfg
    return ConfigBundle(global_cfg, providers, pipelines, root)


def resolve_env(value: str) -> str:
    import os

    if value.startswith("env:"):
        v = os.environ.get(value[4:])
        if v is None:
            raise ConfigError(f"env var {value[4:]} not set")
        return v
    return value
