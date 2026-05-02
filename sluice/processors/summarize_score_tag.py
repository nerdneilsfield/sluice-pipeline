import asyncio
import math
from dataclasses import replace
from pathlib import Path
from typing import Callable

from jinja2 import Template

from sluice.context import PipelineContext
from sluice.core.errors import BudgetExceeded
from sluice.core.item import Item, compute_item_key
from sluice.llm.json_output import loads_llm_json
from sluice.logging_setup import get_logger
from sluice.processors.score_tag import _clean_tags, _strip_fence, _truncate

log = get_logger(__name__)

_MISSING = object()


def _parse_result(raw: str) -> tuple[int, list[str], str]:
    text = _strip_fence(raw.strip())
    try:
        data = loads_llm_json(text)
    except Exception as exc:
        raise ValueError(f"JSON decode error: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("LLM output must be a JSON object")
    if "score" not in data:
        raise ValueError("score field missing from LLM output")

    raw_score = data["score"]
    if isinstance(raw_score, bool):
        raise ValueError(f"score {raw_score!r} is not numeric")
    try:
        score_float = float(raw_score)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"score {raw_score!r} is not numeric") from exc
    if not math.isfinite(score_float):
        raise ValueError(f"score {raw_score!r} is not finite")
    score = max(1, min(10, round(score_float)))

    raw_tags = data.get("tags", _MISSING)
    if raw_tags is _MISSING:
        raw_tags = []
    elif not isinstance(raw_tags, list):
        raise ValueError(f"tags must be a list, got {type(raw_tags).__name__}")
    tags = _clean_tags(raw_tags)

    raw_summary = data.get("summary", _MISSING)
    if raw_summary is _MISSING:
        raise ValueError("summary field missing from LLM output")
    if not isinstance(raw_summary, str):
        raise ValueError(f"summary must be a string, got {type(raw_summary).__name__}")
    summary = raw_summary.strip()

    return score, tags, summary


class SummarizeScoreTagProcessor:
    _CHARS_PER_TOKEN = 3.0
    _ESTIMATED_OUTPUT_TOKENS = 300

    def __init__(
        self,
        *,
        name: str,
        input_field: str,
        prompt_file: str,
        llm_factory: Callable,
        workers: int = 8,
        score_field: str = "score",
        summary_field: str = "summary",
        tags_merge: str = "append",
        on_parse_error: str = "skip",
        default_score: int = 5,
        default_tags: list[str] | None = None,
        default_summary: str = "",
        max_input_chars: int = 8000,
        truncate_strategy: str = "head_tail",
        budget=None,
        failures=None,
        pipeline_id: str | None = None,
        max_retries: int = 3,
        model_spec: str = "",
        price_lookup: Callable = lambda _: (0.0, 0.0),
    ):
        if workers < 1:
            raise ValueError("summarize_score_tag workers must be >= 1")
        self.name = name
        self.input_field = input_field
        self.template = Template(Path(prompt_file).read_text())
        self.llm_factory = llm_factory
        self.workers = workers
        self.score_field = score_field
        self.summary_field = summary_field
        self.tags_merge = tags_merge
        self.on_parse_error = on_parse_error
        self.default_score = max(1, min(10, round(float(default_score))))
        self.default_tags = _clean_tags(default_tags or [])
        self.default_summary = default_summary
        self.max_input_chars = max_input_chars
        self.truncate_strategy = truncate_strategy
        self.budget = budget
        self.failures = failures
        self.pipeline_id = pipeline_id
        self.max_retries = max_retries
        self.model_spec = model_spec
        self._price_lookup = price_lookup

    def _project_usd(self, prompt_chars: int) -> float:
        if self.budget is None:
            return 0.0
        prompt_tokens = prompt_chars / self._CHARS_PER_TOKEN
        in_price, out_price = self._price_lookup(self.model_spec)
        return (prompt_tokens / 1000.0) * in_price + (
            self._ESTIMATED_OUTPUT_TOKENS / 1000.0
        ) * out_price

    def _render_one(self, item) -> str:
        raw = item.get(self.input_field, default="") or ""
        truncated = _truncate(str(raw), self.max_input_chars, self.truncate_strategy)
        item_view = replace(
            item,
            attachments=list(item.attachments),
            extras=dict(item.extras),
            tags=list(item.tags),
        )
        if self.input_field.startswith("extras."):
            key = self.input_field[len("extras.") :]
            item_view.extras[key] = truncated
        else:
            setattr(item_view, self.input_field, truncated)
        return self.template.render(item=item_view)

    def _merge_tags(self, item, tags: list[str]) -> None:
        if self.tags_merge == "replace":
            seen: set[str] = set()
            out: list[str] = []
            for tag in tags:
                if tag in seen:
                    continue
                out.append(tag)
                seen.add(tag)
            item.tags = out
            return
        seen = set(item.tags)
        for tag in tags:
            if tag in seen:
                continue
            item.tags.append(tag)
            seen.add(tag)

    def _apply_result(self, item, score: int, tags: list[str], summary: str):
        item.extras[self.score_field] = score
        self._write_summary(item, summary)
        self._merge_tags(item, tags)
        return item

    def _write_summary(self, item, summary: str) -> None:
        if self.summary_field == "summary":
            item.summary = summary
            return
        if self.summary_field.startswith("extras."):
            key = self.summary_field[len("extras.") :]
            item.extras[key] = summary
            return
        item.extras[self.summary_field] = summary

    def _apply_default(self, item):
        return self._apply_result(
            item, self.default_score, list(self.default_tags), self.default_summary
        )

    def _preflight(self, prompts: list[str]) -> float:
        projected_usd = self._project_usd(sum(len(p) for p in prompts))
        if self.budget is None:
            return projected_usd
        if not self.budget.project(projected_calls=len(prompts), projected_usd=projected_usd):
            raise BudgetExceeded(f"stage {self.name}: would exceed run budget")
        return projected_usd

    async def _handle_failure(self, item, exc: Exception):
        if self.on_parse_error == "skip":
            return item
        if self.on_parse_error == "default":
            return self._apply_default(item)

        if self.failures is not None and self.pipeline_id:
            try:
                await self.failures.record(
                    self.pipeline_id,
                    compute_item_key(item),
                    item,
                    stage=self.name,
                    error_class=type(exc).__name__,
                    error_msg=str(exc),
                    max_retries=self.max_retries,
                )
            except Exception:
                log.exception("summarize_score_tag: failed to persist per-item failure")
        return None

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        rendered = []
        preflight_prompts = []
        for item in ctx.items:
            try:
                prompt = self._render_one(item)
            except Exception as exc:
                rendered.append((item, None, exc))
                continue
            rendered.append((item, prompt, None))
            preflight_prompts.append(prompt)

        projected_usd = self._preflight(preflight_prompts)
        log.bind(
            stage=self.name,
            items_in=len(ctx.items),
            projected_calls=len(preflight_prompts),
            projected_usd=projected_usd,
        ).info("summarize_score_tag.preflight_ok")

        sem = asyncio.Semaphore(self.workers)

        async def one(item, prompt, render_error):
            if render_error is not None:
                return await self._handle_failure(item, render_error)
            async with sem:
                try:
                    llm = self.llm_factory()
                    raw = await llm.chat([{"role": "user", "content": prompt}])
                    score, tags, summary = _parse_result(raw)
                except Exception as exc:
                    return await self._handle_failure(item, exc)
                return self._apply_result(item, score, tags, summary)

        results = await asyncio.gather(
            *[one(item, prompt, err) for item, prompt, err in rendered],
            return_exceptions=True,
        )
        kept: list[Item] = []
        for result in results:
            if isinstance(result, BaseException):
                log.opt(exception=result).error("summarize_score_tag: unexpected per-item error")
                continue
            if result is not None:
                kept.append(result)
        ctx.items = kept
        log.bind(stage=self.name, items_out=len(ctx.items)).info("summarize_score_tag.done")
        return ctx
