import asyncio, json
from pathlib import Path
from typing import Callable
from jinja2 import Template
from sluice.context import PipelineContext
from sluice.core.errors import StageError, BudgetExceeded
from sluice.core.item import compute_item_key

def _truncate(text: str, n: int, strategy: str) -> str:
    if len(text) <= n: return text
    if strategy == "error":
        raise StageError(f"input exceeds {n} chars")
    if strategy == "head":
        return text[:n]
    half = n // 2
    return text[:half] + "\n…\n" + text[-half:]

def _set_path(target, path: str, value):
    head, _, rest = path.partition(".")
    if not rest:
        if isinstance(target, dict): target[head] = value
        else: setattr(target, head, value)
        return
    if head == "extras":
        target.extras[rest] = value
    else:
        setattr(target, head, value)

class LLMStageProcessor:
    name = "llm_stage"

    def __init__(self, *, name, mode, input_field, prompt_file,
                 llm_factory: Callable, output_field=None, output_target=None,
                 output_parser="text", on_parse_error="fail",
                 on_parse_error_default=None, max_input_chars=20000,
                 truncate_strategy="head_tail", workers=4,
                 failures=None, budget=None, pipeline_id: str | None = None,
                 max_retries: int = 3, model_spec: str = "",
                 price_lookup=None):
        self.name = name; self.mode = mode
        self.input_field = input_field
        self.output_field = output_field
        self.output_target = output_target
        self.template = Template(Path(prompt_file).read_text())
        self.llm_factory = llm_factory
        self.output_parser = output_parser
        self.on_parse_error = on_parse_error
        self.on_parse_error_default = on_parse_error_default or {}
        self.max_input_chars = max_input_chars
        self.truncate_strategy = truncate_strategy
        self.workers = workers
        self.failures = failures
        self.budget = budget
        self.pipeline_id = pipeline_id
        self.max_retries = max_retries
        self.model_spec = model_spec
        self._price_lookup = price_lookup or (lambda _: (0.0, 0.0))

    _CHARS_PER_TOKEN = 3.0
    _ESTIMATED_OUTPUT_TOKENS = 1024

    def _project_usd(self, prompt_chars: int, model_spec: str) -> float:
        if self.budget is None:
            return 0.0
        prompt_tokens = prompt_chars / self._CHARS_PER_TOKEN
        in_price, out_price = self._price_lookup(model_spec)
        return ((prompt_tokens / 1000.0) * in_price
                + (self._ESTIMATED_OUTPUT_TOKENS / 1000.0) * out_price)

    def _preflight(self, projected_calls: int, projected_usd: float) -> None:
        if self.budget is None:
            return
        if not self.budget.project(projected_calls=projected_calls,
                                    projected_usd=projected_usd):
            raise BudgetExceeded(
                f"stage {self.name}: would exceed run budget "
                f"(calls={self.budget.calls}+{projected_calls}/"
                f"{self.budget.max_calls}, "
                f"usd=${self.budget.spent_usd:.4f}+${projected_usd:.4f}/"
                f"${self.budget.max_usd})"
            )

    def _render_one(self, item):
        raw = item.get(self.input_field, default="") or ""
        truncated = _truncate(str(raw), self.max_input_chars, self.truncate_strategy)
        item_view = type("It", (), {**item.__dict__,
                                    self.input_field: truncated})()
        return self.template.render(item=item_view)

    def _parse(self, text):
        if self.output_parser == "text":
            return text
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            if self.on_parse_error == "fail":
                raise StageError(f"json parse failed: {e}")
            if self.on_parse_error == "default":
                return self.on_parse_error_default
            return None

    async def _run_per_item(self, ctx: PipelineContext):
        rendered = [self._render_one(it) for it in ctx.items]
        total_chars = sum(len(s) for s in rendered)
        projected_usd = self._project_usd(total_chars, self.model_spec)
        self._preflight(projected_calls=len(rendered),
                         projected_usd=projected_usd)
        sem = asyncio.Semaphore(self.workers)

        async def one(it, content):
            async with sem:
                llm = self.llm_factory()
                msgs = [{"role": "user", "content": content}]
                try:
                    out = await llm.chat(msgs)
                    parsed = self._parse(out)
                except Exception as e:
                    if self.failures is not None and self.pipeline_id:
                        await self.failures.record(
                            self.pipeline_id, compute_item_key(it), it,
                            stage=self.name,
                            error_class=type(e).__name__,
                            error_msg=str(e),
                            max_retries=self.max_retries,
                        )
                    return None
                if parsed is None and self.on_parse_error == "skip":
                    return None
                _set_path(it, self.output_field, parsed)
                return it

        results = await asyncio.gather(*[one(it, c)
                                          for it, c in zip(ctx.items, rendered)])
        ctx.items = [it for it in results if it is not None]
        return ctx

    async def _run_aggregate(self, ctx: PipelineContext):
        rendered = self.template.render(items=ctx.items, context=ctx.context)
        rendered = _truncate(rendered, self.max_input_chars, self.truncate_strategy)
        self._preflight(projected_calls=1,
                         projected_usd=self._project_usd(len(rendered), self.model_spec))
        llm = self.llm_factory()
        out = await llm.chat([{"role": "user", "content": rendered}])
        parsed = self._parse(out)
        _, _, key = self.output_target.partition(".")
        ctx.context[key] = parsed
        return ctx

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        if self.mode == "per_item":
            return await self._run_per_item(ctx)
        return await self._run_aggregate(ctx)
