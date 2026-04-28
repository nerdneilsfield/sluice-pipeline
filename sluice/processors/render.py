from dataclasses import asdict
from pathlib import Path

from jinja2 import Template

from sluice.context import PipelineContext


class RenderProcessor:
    name = "render"

    def __init__(self, *, name: str, template: str, output_field: str):
        self.name = name
        self.template = Template(Path(template).read_text())
        self.output_field = output_field

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        rendered = self.template.render(
            items=ctx.items,
            context=ctx.context,
            pipeline_id=ctx.pipeline_id,
            run_key=ctx.run_key,
            run_date=ctx.run_date,
            stats=asdict(ctx.stats),
        )
        head, _, key = self.output_field.partition(".")
        if head != "context":
            raise ValueError("render output_field must be 'context.<key>'")
        ctx.context[key] = rendered
        return ctx
