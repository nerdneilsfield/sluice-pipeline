from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True)
class SourceDef:
    name: str
    type: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SinkDef:
    name: str
    type: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StageDef:
    name: str
    type: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WindowDef:
    anchor: str
    length: str


@dataclass(frozen=True)
class DeploymentsDef:
    pipelines: list[str] | Literal["all"] = "all"

    def __post_init__(self) -> None:
        if self.pipelines != "all" and not isinstance(self.pipelines, list):
            raise ValueError("deployments.pipelines must be 'all' or a list")


@dataclass(frozen=True)
class PipelineDef:
    name: str
    stages: list[str]
    sources: list[str] | None
    upstream_all: list[str] | None
    upstream_any: list[str] | None
    sinks: list[str] | None
    post_run_cleanup: str | None
    cron: str | None = None
    input_mode: str | None = "union"
    max_attempts: int = 3

    def __post_init__(self) -> None:
        if self.input_mode not in (None, "union", "intersection"):
            raise ValueError(
                "input_mode must be 'union' or 'intersection', "
                f"got {self.input_mode!r}"
            )
        if self.input_mode is None:
            object.__setattr__(self, "input_mode", "union")


@dataclass(frozen=True)
class TaskConfig:
    name: str
    db_path: str
    seen_db: str
    enabled: bool
    timezone: str | None
    window: WindowDef
    deployments: DeploymentsDef
    sources: list[SourceDef]
    sinks: list[SinkDef]
    stages: list[StageDef]
    pipelines: list[PipelineDef]
    task_dir: str | None = None
    rotation_cron: str | None = None
