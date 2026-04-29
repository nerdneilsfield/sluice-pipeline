from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


class KeyConfig(BaseModel):
    value: str
    weight: int = 1
    quota_duration: int | None = None
    reset_time: str | None = None
    quota_error_tokens: list[str] = Field(default_factory=list)


class BaseEndpoint(BaseModel):
    url: str
    weight: int = 1
    key: list[KeyConfig]
    extra_headers: dict[str, str] = Field(default_factory=dict)
    active_windows: list[str] = Field(default_factory=list)
    active_timezone: str | None = None


class ModelEntry(BaseModel):
    model_name: str
    is_stream: bool = True
    is_support_json_schema: bool = False
    is_support_json_object: bool = False
    input_price_per_1k: float = 0.0
    output_price_per_1k: float = 0.0


class Provider(BaseModel):
    name: str
    type: Literal["openai_compatible"]
    base: list[BaseEndpoint]
    models: list[ModelEntry]
    extra_headers: dict[str, str] = Field(default_factory=dict)


class ProvidersConfig(BaseModel):
    providers: list[Provider]


# ---------- Sources ----------
class RssSourceConfig(BaseModel):
    type: Literal["rss"]
    url: str
    tag: str | None = None
    name: str | None = None
    timeout: float = 120.0


SourceConfig = Annotated[
    Union[RssSourceConfig],
    Field(discriminator="type"),
]

# ---------- Filter rules ----------
FilterOp = Literal[
    "gt",
    "gte",
    "lt",
    "lte",
    "eq",
    "matches",
    "not_matches",
    "contains",
    "not_contains",
    "in",
    "not_in",
    "min_length",
    "max_length",
    "newer_than",
    "older_than",
    "exists",
    "not_exists",
]


class FilterRule(BaseModel):
    field: str
    op: FilterOp
    value: Any = None


# ---------- Stages ----------
class DedupeConfig(BaseModel):
    type: Literal["dedupe"]
    name: str
    key_strategy: Literal["auto", "guid_only", "url_only"] = "auto"


class FetcherApplyConfig(BaseModel):
    type: Literal["fetcher_apply"]
    name: str
    write_field: str = "fulltext"
    skip_if_field_longer_than: int | None = None


class FilterConfig(BaseModel):
    type: Literal["filter"]
    name: str
    mode: Literal["keep_if_all", "keep_if_any", "drop_if_all", "drop_if_any"]
    rules: list[FilterRule]


class FieldOp(BaseModel):
    op: Literal["truncate", "drop"]
    field: str
    n: int | None = None


class FieldFilterConfig(BaseModel):
    type: Literal["field_filter"]
    name: str
    ops: list[FieldOp]


class LLMStageConfig(BaseModel):
    type: Literal["llm_stage"]
    name: str
    mode: Literal["per_item", "aggregate"]
    input_field: str
    output_field: str | None = None
    output_target: str | None = None
    prompt_file: str
    model: str
    retry_model: str | None = None
    fallback_model: str | None = None
    fallback_model_2: str | None = None
    workers: int = 4
    concurrency: int = 4
    retry_workers: int | None = None
    retry_concurrency: int | None = None
    fallback_workers: int | None = None
    fallback_concurrency: int | None = None
    fallback_2_workers: int | None = None
    fallback_2_concurrency: int | None = None
    timeout: float = 120.0
    output_parser: Literal["text", "json"] = "text"
    output_schema: str | None = None
    on_parse_error: Literal["fail", "skip", "default"] = "fail"
    on_parse_error_default: dict[str, Any] = Field(default_factory=dict)
    max_input_chars: int = 20000
    truncate_strategy: Literal["head_tail", "head", "error"] = "head_tail"

    @model_validator(mode="after")
    def _check_output_target(self):
        if self.mode == "per_item" and not self.output_field:
            raise ValueError("per_item mode requires output_field")
        if self.mode == "aggregate" and not self.output_target:
            raise ValueError("aggregate mode requires output_target")
        return self


class RenderConfig(BaseModel):
    type: Literal["render"]
    name: str
    template: str
    output_field: str


StageConfig = Annotated[
    Union[
        DedupeConfig,
        FetcherApplyConfig,
        FilterConfig,
        FieldFilterConfig,
        LLMStageConfig,
        RenderConfig,
    ],
    Field(discriminator="type"),
]


# ---------- Sinks ----------
class CommonSinkFields(BaseModel):
    id: str
    emit_on_empty: bool = False


class FileMdSinkConfig(CommonSinkFields):
    type: Literal["file_md"]
    input: str
    path: str


class NotionSinkConfig(CommonSinkFields):
    type: Literal["notion"]
    input: str
    parent_id: str
    parent_type: Literal["database", "page"]
    title_template: str
    token: str = "env:NOTION_TOKEN"
    properties: dict[str, Any] = Field(default_factory=dict)
    mode: Literal["upsert", "create_once", "create_new"] = "upsert"
    max_block_chars: int = 1900


SinkConfig = Annotated[
    Union[FileMdSinkConfig, NotionSinkConfig],
    Field(discriminator="type"),
]


# ---------- Pipeline ----------
class FetcherChainOverride(BaseModel):
    chain: list[str] | None = None
    min_chars: int | None = None
    on_all_failed: Literal["skip", "continue_empty", "use_raw_summary"] | None = None


class CacheOverride(BaseModel):
    enabled: bool = True
    ttl: str = "7d"


class PipelineLimits(BaseModel):
    max_items_per_run: int = 50
    item_overflow_policy: Literal["drop_oldest", "drop_newest"] = "drop_oldest"
    max_llm_calls_per_run: int = 500
    max_estimated_cost_usd: float = 5.0


class PipelineFailures(BaseModel):
    retry_failed: bool = True
    max_retries: int = 3
    retry_backoff: Literal["next_run"] = "next_run"


class StateConfig(BaseModel):
    db_path: str | None = None


StateOverride = StateConfig


class RuntimeConfig(BaseModel):
    timezone: str = "Asia/Shanghai"
    default_cron: str = "0 8 * * *"


class GlobalFetcherConfig(BaseModel):
    chain: list[str] | None = None
    min_chars: int = 500
    on_all_failed: Literal["skip", "continue_empty", "use_raw_summary"] = "skip"
    cache: CacheOverride = Field(default_factory=CacheOverride)


class FetcherImplConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str
    extra: dict[str, Any] = Field(default_factory=dict)


class GlobalConfig(BaseModel):
    state: StateConfig = Field(default_factory=StateConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    fetcher: GlobalFetcherConfig = Field(default_factory=GlobalFetcherConfig)
    fetchers: dict[str, FetcherImplConfig] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    id: str
    description: str = ""
    enabled: bool = True
    cron: str | None = None
    timezone: str | None = None
    window: str = "24h"
    lookback_overlap: str | None = None
    sources: list[SourceConfig]
    stages: list[StageConfig]
    sinks: list[SinkConfig]
    fetcher: FetcherChainOverride = Field(default_factory=FetcherChainOverride)
    cache: CacheOverride = Field(default_factory=CacheOverride)
    limits: PipelineLimits = Field(default_factory=PipelineLimits)
    failures: PipelineFailures = Field(default_factory=PipelineFailures)
    state: StateConfig = Field(default_factory=StateConfig)

    @model_validator(mode="after")
    def _unique_sink_ids(self):
        ids = [s.id for s in self.sinks]
        if len(ids) != len(set(ids)):
            raise ValueError(f"sink ids must be unique within pipeline: {ids}")
        return self
