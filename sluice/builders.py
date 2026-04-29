from sluice.config import (
    DedupeConfig,
    FetcherApplyConfig,
    FieldFilterConfig,
    FileMdSinkConfig,
    FilterConfig,
    GlobalConfig,
    LimitStage,
    LLMStageConfig,
    MirrorAttachmentsStage,
    NotionSinkConfig,
    PipelineConfig,
    RenderConfig,
    RssSourceConfig,
)
from sluice.core.errors import ConfigError
from sluice.fetchers.chain import FetcherChain
from sluice.llm.client import LLMClient, StageLLMConfig
from sluice.llm.pool import ProviderPool
from sluice.pricing import model_price
from sluice.registry import (
    get_fetcher,
    get_source,
)
from sluice.state.cache import UrlCacheStore
from sluice.state.failures import FailureStore
from sluice.state.seen import SeenStore


def build_sources(pipe: PipelineConfig):
    out = []
    for i, s in enumerate(pipe.sources):
        if isinstance(s, RssSourceConfig):
            cls = get_source("rss")
            out.append(
                cls(
                    url=s.url,
                    pipeline_id=pipe.id,
                    source_id=s.name or f"rss_{i}",
                    tag=s.tag,
                    name=s.name,
                    timeout=s.timeout,
                )
            )
    return out


def _resolve_fetcher_chain_cfg(
    global_cfg: GlobalConfig, pipe: PipelineConfig
) -> tuple[list[str], int, str, bool, int]:
    g = global_cfg.fetcher
    chain = pipe.fetcher.chain
    if chain is None:
        chain = g.chain
    min_chars = pipe.fetcher.min_chars
    if min_chars is None:
        min_chars = g.min_chars
    on_all_failed = pipe.fetcher.on_all_failed
    if on_all_failed is None:
        on_all_failed = g.on_all_failed
    if chain is None or min_chars is None or on_all_failed is None:
        missing = []
        if chain is None:
            missing.append("fetcher.chain")
        if min_chars is None:
            missing.append("fetcher.min_chars")
        if on_all_failed is None:
            missing.append("fetcher.on_all_failed")
        raise ConfigError(
            f"pipeline {pipe.id!r}: fetcher_apply requires configured {', '.join(missing)}"
        )
    if "cache" in pipe.model_fields_set:
        cache_enabled = pipe.cache.enabled
        ttl_str = pipe.cache.ttl
    else:
        cache_enabled = g.cache.enabled
        ttl_str = g.cache.ttl
    from sluice.window import parse_duration

    ttl = int(parse_duration(ttl_str).total_seconds())
    return (chain, min_chars, on_all_failed, cache_enabled, ttl)


def build_fetcher_chain(
    global_cfg: GlobalConfig, pipe: PipelineConfig, cache: UrlCacheStore | None
) -> FetcherChain:
    chain_names, min_chars, on_all_failed, cache_enabled, ttl = _resolve_fetcher_chain_cfg(
        global_cfg, pipe
    )
    fetchers = []
    for name in chain_names:
        impl = global_cfg.fetchers.get(name)
        if impl is None:
            raise ValueError(f"fetcher {name!r} declared in chain but missing [fetchers.{name}]")
        cls = get_fetcher(impl.type)
        kwargs = impl.model_dump(exclude={"type", "extra"}, exclude_none=True)
        if kwargs.get("api_key") and kwargs["api_key"].startswith("env:"):
            from sluice.loader import resolve_env

            kwargs["api_key"] = resolve_env(kwargs["api_key"])
        fetchers.append(cls(**kwargs))
    return FetcherChain(
        fetchers,
        min_chars=min_chars,
        on_all_failed=on_all_failed,
        cache=cache if cache_enabled else None,
        ttl_seconds=ttl,
    )


def build_processors(
    *,
    pipe: PipelineConfig,
    global_cfg: GlobalConfig,
    seen: SeenStore,
    failures: FailureStore,
    fetcher_chain: FetcherChain | None,
    llm_pool: ProviderPool,
    budget,
    dry_run: bool = False,
    requeued_keys: set[str] | None = None,
    db=None,
):
    eff_failures = None if dry_run else failures
    procs = []
    for st in pipe.stages:
        if isinstance(st, DedupeConfig):
            from sluice.processors.dedupe import DedupeProcessor

            procs.append(
                DedupeProcessor(
                    name=st.name,
                    pipeline_id=pipe.id,
                    seen=seen,
                    failures=failures,
                    requeued_keys=requeued_keys,
                )
            )
        elif isinstance(st, FetcherApplyConfig):
            from sluice.processors.fetcher_apply import FetcherApplyProcessor

            if fetcher_chain is None:
                raise ConfigError(
                    f"pipeline {pipe.id!r}: fetcher_apply stage {st.name!r} has no fetcher chain"
                )
            procs.append(
                FetcherApplyProcessor(
                    name=st.name,
                    chain=fetcher_chain,
                    write_field=st.write_field,
                    skip_if_field_longer_than=st.skip_if_field_longer_than,
                    failures=eff_failures,
                    max_retries=pipe.failures.max_retries,
                    on_all_failed=fetcher_chain.on_all_failed,
                )
            )
        elif isinstance(st, FilterConfig):
            from sluice.processors.filter import FilterProcessor

            procs.append(FilterProcessor(name=st.name, mode=st.mode, rules=st.rules))
        elif isinstance(st, FieldFilterConfig):
            from sluice.processors.field_filter import FieldFilterProcessor

            procs.append(FieldFilterProcessor(name=st.name, ops=st.ops))
        elif isinstance(st, LLMStageConfig):
            from sluice.processors.llm_stage import LLMStageProcessor

            stage_llm = StageLLMConfig(
                model=st.model,
                retry_model=st.retry_model,
                fallback_model=st.fallback_model,
                fallback_model_2=st.fallback_model_2,
                timeout=st.timeout,
            )
            procs.append(
                LLMStageProcessor(
                    name=st.name,
                    mode=st.mode,
                    input_field=st.input_field,
                    output_field=st.output_field,
                    output_target=st.output_target,
                    prompt_file=st.prompt_file,
                    llm_factory=lambda cfg=stage_llm: LLMClient(llm_pool, cfg, budget),
                    output_parser=st.output_parser,
                    on_parse_error=st.on_parse_error,
                    on_parse_error_default=st.on_parse_error_default,
                    max_input_chars=st.max_input_chars,
                    truncate_strategy=st.truncate_strategy,
                    workers=st.workers,
                    failures=eff_failures,
                    budget=budget,
                    pipeline_id=pipe.id,
                    max_retries=pipe.failures.max_retries,
                    model_spec=st.model,
                    price_lookup=lambda spec, pool=llm_pool: model_price(pool, spec),
                )
            )
        elif isinstance(st, LimitStage):
            from sluice.processors.limit import LimitProcessor

            procs.append(
                LimitProcessor(
                    name=st.name,
                    top_n=st.top_n,
                    sort_by=st.sort_by,
                    sort_order=st.sort_order,
                    sort_missing=st.sort_missing,
                    group_by=st.group_by,
                    per_group_max=st.per_group_max,
                )
            )
        elif isinstance(st, RenderConfig):
            from sluice.processors.render import RenderProcessor

            procs.append(
                RenderProcessor(name=st.name, template=st.template, output_field=st.output_field)
            )
        elif isinstance(st, MirrorAttachmentsStage):
            from pathlib import Path

            from sluice.processors.mirror_attachments import MirrorAttachmentsProcessor
            from sluice.state.attachment_store import AttachmentStore

            if db is None:
                raise ConfigError(f"pipeline {pipe.id!r}: mirror_attachments stage requires db")
            att_dir = global_cfg.state.attachment_dir or "./data/attachments"
            prefix_val = global_cfg.state.attachment_url_prefix
            att_prefix = prefix_val if prefix_val is not None else ""
            store = AttachmentStore(db=db, base_dir=Path(att_dir))
            procs.append(
                MirrorAttachmentsProcessor(
                    name=st.name,
                    store=store,
                    pipeline_id=pipe.id,
                    mime_prefixes=st.mime_prefixes,
                    max_bytes=st.max_bytes,
                    on_failure=st.on_failure,
                    rewrite_fields=st.rewrite_fields,
                    attachment_url_prefix=att_prefix,
                )
            )
        else:
            raise ValueError(f"unknown stage type: {type(st).__name__}")
    return procs


def build_sinks(pipe: PipelineConfig):
    out = []
    for s in pipe.sinks:
        if isinstance(s, FileMdSinkConfig):
            from sluice.sinks.file_md import FileMdSink

            out.append(
                FileMdSink(id=s.id, input=s.input, path=s.path, emit_on_empty=s.emit_on_empty)
            )
        elif isinstance(s, NotionSinkConfig):
            from sluice.sinks.notion import NotionSink

            out.append(
                NotionSink(
                    id=s.id,
                    input=s.input,
                    parent_id=s.parent_id,
                    parent_type=s.parent_type,
                    title_template=s.title_template,
                    token=s.token,
                    properties=s.properties,
                    mode=s.mode,
                    max_block_chars=s.max_block_chars,
                    emit_on_empty=s.emit_on_empty,
                )
            )
        else:
            raise ValueError(f"unknown sink type: {type(s).__name__}")
    return out
