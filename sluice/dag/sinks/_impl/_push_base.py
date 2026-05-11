from dataclasses import dataclass

from jinja2 import Template

from sluice.context import PipelineContext
from sluice.dag.sinks._impl.base import Sink


@dataclass
class PushBatchItem:
    kind: str
    payload: object
    recipient: str | None = None


class PushSinkBase(Sink):
    type: str = "push"

    def __init__(
        self,
        *,
        sink_id: str,
        footer_template: str,
        delivery_log: object | None = None,
        emit_on_empty: bool = False,
    ):
        super().__init__(id=sink_id, mode="create_new", emit_on_empty=emit_on_empty)
        self._footer_tmpl = Template(footer_template) if footer_template else None
        self._log = None

    async def create(self, ctx: PipelineContext) -> str:
        raise NotImplementedError(f"{self.type} sink uses emit() override, not create()")

    def render_footer(self, ctx: PipelineContext) -> str:
        if not self._footer_tmpl:
            return ""
        return self._footer_tmpl.render(
            pipeline_id=ctx.pipeline_id,
            run_date=ctx.run_date,
            run_key=ctx.run_key,
        )

    async def send_one(self, payload, recipient=None) -> str:
        raise NotImplementedError

    def build_batch(self, ctx: PipelineContext) -> list[PushBatchItem]:
        raise NotImplementedError

    @property
    def fail_fast(self) -> bool:
        return True
