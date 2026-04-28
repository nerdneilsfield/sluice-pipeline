from pathlib import Path
from sluice.sinks.base import Sink, register_sink
from sluice.context import PipelineContext

@register_sink("file_md")
class FileMdSink(Sink):
    type = "file_md"

    def __init__(self, *, id: str, input: str, path: str,
                 mode: str = "upsert", emit_on_empty: bool = False):
        super().__init__(id=id, mode=mode, emit_on_empty=emit_on_empty)
        self.input = input
        self.path_template = path

    def _resolve_path(self, ctx: PipelineContext) -> Path:
        return Path(self.path_template.format(
            run_date=ctx.run_date,
            pipeline_id=ctx.pipeline_id,
            run_key=ctx.run_key.replace("/", "_"),
        ))

    def _content(self, ctx: PipelineContext) -> str:
        head, _, key = self.input.partition(".")
        if head != "context":
            raise ValueError("file_md input must be 'context.<key>'")
        return ctx.context[key]

    async def create(self, ctx: PipelineContext) -> str:
        p = self._resolve_path(ctx)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self._content(ctx))
        return str(p)

    async def update(self, external_id: str, ctx: PipelineContext) -> str:
        return await self.create(ctx)
