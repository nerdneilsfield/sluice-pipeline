from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol

if TYPE_CHECKING:
    from .web.config import WebRegistryConfig
    from .web.contracts import WebBinding


type JSONValue = (
    None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]
)
type FrozenJSON = (
    None
    | bool
    | int
    | float
    | str
    | tuple["FrozenJSON", ...]
    | Mapping[str, "FrozenJSON"]
)


def freeze_item_json(
    value: JSONValue | FrozenJSON,
    _active: set[int] | None = None,
) -> FrozenJSON:
    """Recursively convert JSON containers into immutable Item containers."""

    active = set() if _active is None else _active
    if isinstance(value, Mapping):
        marker = id(value)
        if marker in active:
            raise ValueError("recursive JSON value")
        active.add(marker)
        try:
            frozen = {
                key: freeze_item_json(item, active) for key, item in value.items()
            }
        finally:
            active.remove(marker)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        marker = id(value)
        if marker in active:
            raise ValueError("recursive JSON value")
        active.add(marker)
        try:
            frozen = tuple(freeze_item_json(item, active) for item in value)
        finally:
            active.remove(marker)
        return frozen
    return value


@dataclass(frozen=True)
class Item:
    id: str
    producer_scope: tuple[Literal["source", "stage"], str]
    logical_id: str
    payload: FrozenJSON
    metadata: Mapping[str, FrozenJSON]
    content_hash: str
    observed_at: str
    produced_by: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", freeze_item_json(self.payload))
        frozen_metadata = freeze_item_json(dict(self.metadata))
        object.__setattr__(self, "metadata", frozen_metadata)


@dataclass(frozen=True)
class SourceRecord:
    logical_id: str
    payload: JSONValue
    metadata: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", freeze_item_json(self.payload))
        object.__setattr__(self, "metadata", freeze_item_json(self.metadata))


@dataclass(frozen=True)
class SourceResult:
    records: tuple[SourceRecord, ...]
    next_checkpoint: JSONValue | KeepCheckpoint


@dataclass(frozen=True)
class KeepCheckpoint:
    """Identity-only marker that preserves the previous Source checkpoint."""

    __slots__ = ()


KEEP_CHECKPOINT: Final = KeepCheckpoint()


@dataclass(frozen=True)
class Drop:
    """Explicit Task output that produces no value."""

    __slots__ = ()


DROP: Final = Drop()


@dataclass(frozen=True)
class FORWARD:
    item: Item


@dataclass(frozen=True)
class NEW:
    value: Any
    metadata: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", freeze_item_json(self.metadata))


type TaskMarker = Drop | FORWARD | NEW
type EachTaskResult = TaskMarker
type BatchTaskResult = tuple[TaskMarker, ...]


@dataclass(frozen=True)
class BatchInput:
    """Explicit input passed to the first Task in a batch Stage.

    ``history`` is a reserved, read-only side input for an initial ``dedup``
    Task.  It is kept separate from ``items`` so a Stage cannot accidentally
    publish or count a prior Item as a current candidate.
    """

    items: tuple[Item, ...]
    history: tuple[Item, ...] = ()


@dataclass(frozen=True)
class StageEnvelope:
    """The exact ordered Item references used as a Stage input."""

    items: tuple[str, ...]
    history: tuple[str, ...] = ()


@dataclass(frozen=True)
class StageOutcome:
    """Pure Task-chain result before durable Stage materialization."""

    outputs: tuple[FORWARD | NEW, ...]
    status: Literal["succeeded", "dropped"]


class SourceContract(Protocol):
    def collect(
        self, config: Mapping[str, JSONValue], checkpoint: JSONValue
    ) -> SourceResult:
        raise NotImplementedError


class TaskContract(Protocol):
    input_mode: Literal["each", "batch"]

    def run(
        self, config: Mapping[str, JSONValue], input: Any
    ) -> EachTaskResult | BatchTaskResult:
        raise NotImplementedError


class SinkContract(Protocol):
    def send(
        self,
        config: Mapping[str, JSONValue],
        rendered_payload: JSONValue,
        delivery_key: str,
    ) -> str | None:
        raise NotImplementedError


class HTTPClient(Protocol):
    def get(self, url: str) -> str:
        raise NotImplementedError


class LLMClient(Protocol):
    def complete(self, model: str, prompt: str, input_payload: JSONValue) -> JSONValue:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    timeout: timedelta | None = None
    retries: int = 0
    concurrency: int = 1


@dataclass(frozen=True, slots=True)
class BatchPolicy:
    count: int | None = None
    max_wait: timedelta | None = None
    on_schedule: bool = False
    include_updates: bool = False


@dataclass(frozen=True, slots=True)
class SourceDef:
    name: str
    use: str
    schedule: timedelta | None
    with_config: Mapping[str, FrozenJSON]
    policy: ExecutionPolicy
    web_binding: WebBinding | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "with_config", freeze_item_json(self.with_config))
        if self.web_binding is not None and not isinstance(
            self.web_binding, WebBinding
        ):
            raise TypeError("web_binding must be a WebBinding or null")


@dataclass(frozen=True, slots=True)
class TaskDef:
    use: str
    with_config: Mapping[str, FrozenJSON]
    web_binding: WebBinding | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "with_config", freeze_item_json(self.with_config))
        if self.web_binding is not None and not isinstance(
            self.web_binding, WebBinding
        ):
            raise TypeError("web_binding must be a WebBinding or null")


@dataclass(frozen=True, slots=True)
class StageDef:
    name: str
    input_mode: Literal["each", "batch"]
    tasks: tuple[TaskDef, ...]
    output_schema: Mapping[str, FrozenJSON] | None
    cache_version: int
    canonical_config_json: str


@dataclass(frozen=True, slots=True)
class DeliveryDef:
    use: str
    with_config: Mapping[str, FrozenJSON]


@dataclass(frozen=True, slots=True)
class NodeDef:
    name: str
    stage: str | None
    from_refs: tuple[str, ...]
    schedule: timedelta | None
    batch: BatchPolicy | None
    policy: ExecutionPolicy
    deliver: DeliveryDef | None


@dataclass(frozen=True, slots=True)
class PipelineDef:
    name: str
    nodes: Mapping[str, NodeDef]


@dataclass(frozen=True, slots=True)
class LLMConfig:
    """One OpenAI-compatible endpoint; ``api_key`` is an unresolved ``env:NAME``."""

    base_url: str
    api_key: str | None = None
    timeout_seconds: float = 60.0
    max_attempts: int = 3


@dataclass(frozen=True, slots=True)
class Definition:
    version: int
    database: str
    sources: Mapping[str, SourceDef]
    stages: Mapping[str, StageDef]
    pipelines: Mapping[str, PipelineDef]
    web: WebRegistryConfig | None = None
    llm: LLMConfig | None = None
    # Runtime-only like ``database``: never hashed into Delivery identity.
    file_root: str | None = None


# ``web.contracts`` deliberately has no eager dependency on this module so it
# can be imported on its own.  Importing the type here after the core contract
# definitions are complete keeps ``typing.get_type_hints`` useful without
# introducing an import cycle during module initialisation.
from .web.config import WebRegistryConfig
from .web.contracts import WebBinding
