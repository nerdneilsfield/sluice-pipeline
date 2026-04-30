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
    for pipe in pipelines.values():
        _validate_run_key_template(global_cfg, pipe)
    for pipe in pipelines.values():
        att_prefix = (
            pipe.state.attachment_url_prefix or global_cfg.state.attachment_url_prefix or ""
        )
        validate_pipeline_attachment_email_compat(
            [s.model_dump() for s in pipe.sinks],
            [st.model_dump() for st in pipe.stages],
            att_prefix,
        )
    return ConfigBundle(global_cfg, providers, pipelines, root)


def resolve_env(value: str) -> str:
    import os

    if value.startswith("env:"):
        v = os.environ.get(value[4:])
        if v is None:
            raise ConfigError(f"env var {value[4:]} not set")
        return v
    return value


def _validate_run_key_template(global_cfg: GlobalConfig, pipe: PipelineConfig) -> None:
    from sluice.run_key import validate_template

    cron = pipe.cron or global_cfg.runtime.default_cron
    tz = pipe.timezone or global_cfg.runtime.timezone
    validate_template(pipe.run_key_template, cron, tz)


def validate_pipeline_attachment_email_compat(
    sinks,
    stages,
    attachment_url_prefix: str,
) -> None:
    has_email = any(s.get("type") == "email" for s in sinks)
    has_mirror = any(st.get("type") == "mirror_attachments" for st in stages)
    if has_email and has_mirror and not attachment_url_prefix.startswith(("http://", "https://")):
        raise ConfigError(
            f"email sink with mirror_attachments requires "
            f"attachment_url_prefix to be http(s):// (current: "
            f"{attachment_url_prefix!r}); local/file:// paths cannot be "
            f"loaded by email clients."
        )
