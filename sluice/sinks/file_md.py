from pathlib import Path
from sluice.sinks.base import Sink, register_sink
from sluice.context import PipelineContext


@register_sink("file_md")
class FileMdSink(Sink):
    type = "file_md"

    def __init__(
        self, *, id: str, input: str, path: str, mode: str = "upsert", emit_on_empty: bool = False
    ):
        super().__init__(id=id, mode=mode, emit_on_empty=emit_on_empty)
        self.input = input
        self.path_template = path

    def _resolve_path(self, ctx: PipelineContext) -> Path:
        safe_pipeline_id = ctx.pipeline_id.replace("..", "_").replace("/", "_")
        safe_run_key = ctx.run_key.replace("..", "_").replace("/", "_")
        return Path(
            self.path_template.format(
                run_date=ctx.run_date,
                pipeline_id=safe_pipeline_id,
                run_key=safe_run_key,
            )
        )

    def _content(self, ctx: PipelineContext) -> str:
        head, _, key = self.input.partition(".")
        if head != "context":
            raise ValueError("file_md input must be 'context.<key>'")
        return ctx.context[key]

    async def create(self, ctx: PipelineContext) -> str:
        p = self._resolve_path(ctx)
        # Defensive: ensure resolved path stays under the template's parent directory
        template_parent = Path(self.path_template).parent.resolve()
        resolved = p.resolve()
        if template_parent != Path(".") and not resolved.is_relative_to(template_parent):
            raise ValueError(f"resolved path {resolved} escapes output directory {template_parent}")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self._content(ctx))
        return str(p)

    async def update(self, external_id: str, ctx: PipelineContext) -> str:
        return await self.create(ctx)
