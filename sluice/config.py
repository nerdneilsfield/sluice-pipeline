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


class FieldOpTruncate(BaseModel):
    op: Literal["truncate"]
    field: str
    n: int | None = None


class FieldOpDrop(BaseModel):
    op: Literal["drop"]
    field: str


class FieldOpLower(BaseModel):
    op: Literal["lower"]
    field: str


class FieldOpStrip(BaseModel):
    op: Literal["strip"]
    field: str
    chars: str | None = None


class FieldOpRegexReplace(BaseModel):
    op: Literal["regex_replace"]
    field: str
    pattern: str
    replacement: str
    count: int = 0


FieldOp = Annotated[
    Union[FieldOpTruncate, FieldOpDrop, FieldOpLower, FieldOpStrip, FieldOpRegexReplace],
    Field(discriminator="op"),
]


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


class LimitStage(BaseModel):
    type: Literal["limit"]
    name: str
    top_n: int
    sort_by: str
    sort_order: Literal["desc", "asc"] = "desc"
    sort_missing: Literal["first", "last", "drop"] = "last"
    group_by: str | None = None
    per_group_max: int | None = None


class MirrorAttachmentsStage(BaseModel):
    type: Literal["mirror_attachments"]
    name: str
    mime_prefixes: list[str] = ["image/"]
    max_bytes: int = 10_000_000
    ttl: str = "180d"
    on_failure: Literal["skip", "fail", "drop_attachment"] = "skip"
    rewrite_fields: list[str] = []


class EnrichStage(BaseModel):
    type: Literal["enrich"]
    name: str
    enricher: str
    output_field: str
    on_failure: Literal["skip", "fail"] = "skip"
    cache: bool = True
    max_chars: int = 8000
    concurrency: int = 4
    config: dict = Field(default_factory=dict)


StageConfig = Annotated[
    Union[
        DedupeConfig,
        EnrichStage,
        FetcherApplyConfig,
        FilterConfig,
        FieldFilterConfig,
        LLMStageConfig,
        LimitStage,
        MirrorAttachmentsStage,
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


class TelegramSinkConfig(CommonSinkFields):
    type: Literal["telegram"]
    bot_token: str = "env:TELEGRAM_BOT_TOKEN"
    chat_id: str = "env:TELEGRAM_CHAT_ID"
    brief_input: str | None = None
    items_input: Literal["items", "none"] = "items"
    items_template: str = "{{ item.title }}\n{{ item.url }}"
    split: Literal["per_item", "single"] = "per_item"
    link_preview_disabled: bool = True
    footer_template: str = ""
    on_message_too_long: Literal["truncate", "fail", "split_more"] = "truncate"
    between_messages_delay_seconds: float = 0.0


class FeishuSinkConfig(CommonSinkFields):
    type: Literal["feishu"]
    webhook_url: str = "env:FEISHU_WEBHOOK_URL"
    secret: str | None = None
    brief_input: str | None = None
    items_input: Literal["items", "none"] = "items"
    items_template: str = "{{ item.title }}\n{{ item.url }}"
    split: Literal["per_item", "single"] = "per_item"
    message_type: Literal["post", "text", "interactive"] = "post"
    on_message_too_long: Literal["truncate", "fail", "split_more"] = "truncate"
    card_template: str = ""
    footer_template: str = ""
    between_messages_delay_seconds: float = 0.0
    # bot_api fields
    auth_mode: Literal["webhook", "bot_api"] = "webhook"
    app_id: str | None = None
    app_secret: str | None = None
    receive_id: str | None = None
    receive_id_type: Literal["chat_id", "open_id", "user_id", "email"] = "chat_id"

    @model_validator(mode="after")
    def _validate_bot_api(self) -> "FeishuSinkConfig":
        from sluice.core.errors import ConfigError

        if self.auth_mode == "bot_api":
            missing = [
                name
                for name, val in [
                    ("app_id", self.app_id),
                    ("app_secret", self.app_secret),
                    ("receive_id", self.receive_id),
                ]
                if val is None
            ]
            if missing:
                raise ConfigError(
                    f"feishu bot_api mode requires: {', '.join(missing)}"
                )
            if self.message_type == "interactive":
                raise ConfigError(
                    "feishu bot_api mode does not support message_type='interactive'"
                )
        return self


class EmailSinkConfig(CommonSinkFields):
    type: Literal["email"]
    smtp_host: str = "env:SMTP_HOST"
    smtp_port: int = 587
    smtp_username: str = "env:SMTP_USERNAME"
    smtp_password: str = "env:SMTP_PASSWORD"
    smtp_starttls: bool = True
    from_address: str = "env:SMTP_FROM"
    recipients: list[str] = []
    subject_template: str = "{{ pipeline_id }} · {{ run_date }}"
    brief_input: str | None = None
    items_input: Literal["items", "none"] = "items"
    items_template: str = "{{ item.title }}\n{{ item.url }}"
    split: Literal["per_item", "single"] = "per_item"
    html_template: str = (
        "<html><head>{% if style_block %}<style>{{ style_block }}</style>{% endif %}</head>"
        "<body>{{ body_html }}</body></html>"
    )
    style_block_file: str = ""
    footer_template: str = ""
    attach_run_log: bool = False
    recipient_failure_policy: Literal["fail_fast", "best_effort"] = "fail_fast"


SinkConfig = Annotated[
    Union[
        EmailSinkConfig,
        FeishuSinkConfig,
        FileMdSinkConfig,
        NotionSinkConfig,
        TelegramSinkConfig,
    ],
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
    attachment_dir: str | None = None
    attachment_url_prefix: str | None = None


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


class GCSchedule(BaseModel):
    enabled: bool = False
    cron: str = "0 3 * * *"


class GCConfig(BaseModel):
    failed_items_older_than: str = "90d"
    url_cache_max_rows: int = 50000
    url_cache_keep_expired: str = "7d"
    attachment_unreferenced_after: str = "30d"
    schedule: GCSchedule = Field(default_factory=GCSchedule)


class GlobalConfig(BaseModel):
    state: StateConfig = Field(default_factory=StateConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    fetcher: GlobalFetcherConfig = Field(default_factory=GlobalFetcherConfig)
    fetchers: dict[str, FetcherImplConfig] = Field(default_factory=dict)
    gc: GCConfig = Field(default_factory=GCConfig)


class PipelineConfig(BaseModel):
    id: str
    description: str = ""
    enabled: bool = True
    cron: str | None = None
    timezone: str | None = None
    window: str = "24h"
    lookback_overlap: str | None = None
    run_key_template: str = "{pipeline_id}/{run_date}"
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
