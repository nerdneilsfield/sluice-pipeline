import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import cast
from zoneinfo import ZoneInfo

from sluice.builders import (
    build_fetcher_chain,
    build_processors,
    build_sinks,
    build_sources,
)
from sluice.config import FetcherApplyConfig, GlobalConfig, PipelineConfig
from sluice.context import PipelineContext, RunStats
from sluice.llm.budget import RunBudget
from sluice.llm.pool import ProviderPool
from sluice.loader import ConfigBundle
from sluice.processors.dedupe import item_key
from sluice.state.cache import UrlCacheStore
from sluice.state.db import open_db
from sluice.state.emissions import EmissionStore
from sluice.state.failures import FailureStore
from sluice.state.run_log import RunLog
from sluice.state.seen import SeenStore
from sluice.window import compute_window

log = logging.getLogger(__name__)


@dataclass
class RunResult:
    pipeline_id: str
    run_key: str
    status: str
    items_in: int = 0
    items_out: int = 0
    error: str | None = None


def _resolve_db_path(global_cfg: GlobalConfig, pipe: PipelineConfig) -> str:
    db_path = pipe.state.db_path
    if db_path is None:
        db_path = global_cfg.state.db_path
    return cast(str, db_path)


def _resolve_tz(global_cfg: GlobalConfig, pipe: PipelineConfig) -> ZoneInfo:
    return ZoneInfo(pipe.timezone or global_cfg.runtime.timezone)


def _run_key(pipe_id: str, run_date: str) -> str:
    return f"{pipe_id}/{run_date}"


async def run_pipeline(
    bundle: ConfigBundle, *, pipeline_id: str, now: datetime | None = None, dry_run: bool = False
) -> RunResult:
    pipe = bundle.pipelines[pipeline_id]
    global_cfg = bundle.global_cfg
    tz = _resolve_tz(global_cfg, pipe)
    now = now or datetime.now(timezone.utc)
    local_now = now.astimezone(tz)
    run_date = local_now.date().isoformat()
    run_key = _run_key(pipe.id, run_date)

    db_path = _resolve_db_path(global_cfg, pipe)
    async with open_db(db_path) as db:
        seen = SeenStore(db)
        failures = FailureStore(db)
        emissions = EmissionStore(db)
        cache = UrlCacheStore(db)
        runlog = RunLog(db)
        await runlog.start(pipe.id, run_key)

        pool: ProviderPool | None = None
        try:
            budget = RunBudget(
                max_calls=pipe.limits.max_llm_calls_per_run,
                max_usd=pipe.limits.max_estimated_cost_usd,
            )
            pool = ProviderPool(bundle.providers)
            needs_fetcher_chain = any(isinstance(st, FetcherApplyConfig) for st in pipe.stages)
            chain = build_fetcher_chain(global_cfg, pipe, cache) if needs_fetcher_chain else None

            from sluice.builders import _model_price as _mp
            from sluice.config import LLMStageConfig as _LLMStageConfig
            from sluice.core.errors import ConfigError as _ConfigError

            if pipe.limits.max_estimated_cost_usd > 0:
                for st in pipe.stages:
                    if not isinstance(st, _LLMStageConfig):
                        continue
                    for spec in (st.model, st.retry_model, st.fallback_model, st.fallback_model_2):
                        if spec is None:
                            continue
                        ip, op = _mp(pool, spec)
                        if ip == 0.0 and op == 0.0:
                            raise _ConfigError(
                                f"pipeline {pipe.id!r}: max_estimated_cost_usd "
                                f"is set but model {spec!r} has no "
                                f"input/output_price_per_1k in providers.toml "
                                f"— cost cap would silently never trip. "
                                f"Set prices or set max_estimated_cost_usd = 0 "
                                f"to disable the USD cap explicitly."
                            )

            window_start, window_end = compute_window(
                now=now, window=pipe.window, lookback_overlap=pipe.lookback_overlap
            )

            sources = build_sources(pipe)
            collected = []
            for src in sources:
                async for it in src.fetch(window_start, window_end):
                    collected.append(it)

            requeue = await failures.requeue(pipe.id)
            requeued_keys = {item_key(it) for it in requeue}
            items_in = len(collected) + len(requeue)
            if requeued_keys:
                collected = [it for it in collected if item_key(it) not in requeued_keys]

            ctx = PipelineContext(
                pipeline_id=pipe.id,
                run_key=run_key,
                run_date=run_date,
                items=[*collected, *requeue],
                context={},
                stats=RunStats(items_in=items_in),
                items_resolved_from_failures=list(requeue),
            )

            procs = build_processors(
                pipe=pipe,
                global_cfg=global_cfg,
                seen=seen,
                failures=failures,
                fetcher_chain=chain,
                llm_pool=pool,
                budget=budget,
                dry_run=dry_run,
                requeued_keys=requeued_keys,
            )

            cap = pipe.limits.max_items_per_run
            policy = pipe.limits.item_overflow_policy
            min_dt = datetime.min.replace(tzinfo=timezone.utc)

            from sluice.processors.dedupe import DedupeProcessor

            for p in procs:
                ctx = await p.process(ctx)
                if isinstance(p, DedupeProcessor):
                    if len(ctx.items) > cap:
                        ctx.items.sort(
                            key=lambda i: i.published_at or min_dt,
                            reverse=(policy == "drop_oldest"),
                        )
                        ctx.items = ctx.items[:cap]

            ctx.stats.items_out = len(ctx.items)
            ctx.stats.llm_calls = budget.calls
            ctx.stats.est_cost_usd = budget.spent_usd

            if not dry_run:
                for sink in build_sinks(pipe):
                    await sink.emit(ctx, emissions=emissions)

            if not dry_run:
                kept = [(it, item_key(it)) for it in ctx.items]
                await seen.mark_seen_batch(pipe.id, kept)
                surviving_keys = {item_key(it) for it in ctx.items}
                for k in requeued_keys & surviving_keys:
                    await failures.mark_resolved(pipe.id, k)

            await runlog.update_stats(
                pipe.id,
                run_key,
                items_in=ctx.stats.items_in,
                items_out=ctx.stats.items_out,
                llm_calls=ctx.stats.llm_calls,
                est_cost_usd=ctx.stats.est_cost_usd,
            )
            await runlog.finish(pipe.id, run_key, status="success")
            return RunResult(
                pipeline_id=pipe.id,
                run_key=run_key,
                status="success",
                items_in=ctx.stats.items_in,
                items_out=ctx.stats.items_out,
            )
        except Exception as e:
            log.exception("pipeline failed")
            await runlog.finish(pipe.id, run_key, status="failed", error_msg=str(e))
            return RunResult(pipeline_id=pipe.id, run_key=run_key, status="failed", error=str(e))
        finally:
            if pool is not None:
                await pool.aclose()
