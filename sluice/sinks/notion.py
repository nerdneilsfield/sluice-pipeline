import asyncio
from typing import Protocol

from sluice.context import PipelineContext
from sluice.loader import resolve_env
from sluice.sinks.base import Sink, register_sink


class NotionifyAdapter(Protocol):
    async def create_page(
        self, *, parent_id: str, parent_type: str, title: str, properties: dict, markdown: str
    ) -> str: ...

    async def replace_page_blocks(self, page_id: str, markdown: str) -> None: ...


class DefaultNotionifyAdapter:
    def __init__(self, token: str):
        from notionify import NotionifyClient

        self._client = NotionifyClient(token=token)

    async def create_page(self, *, parent_id, parent_type, title, properties, markdown):
        result = await asyncio.to_thread(
            self._client.create_page_with_markdown,
            parent_id=parent_id,
            title=title,
            markdown=markdown,
            parent_type=parent_type,
            properties=properties or None,
            title_from_h1=False,
        )
        return result.page_id

    async def replace_page_blocks(self, page_id, markdown):
        await asyncio.to_thread(
            self._client.update_page_from_markdown,
            page_id=page_id,
            markdown=markdown,
            strategy="overwrite",
            on_conflict="overwrite",
        )


def chunk_markdown(text: str, max_chars: int) -> list[str]:
    # Kept as the v1 chunking primitive even though DefaultNotionifyAdapter
    # currently delegates block splitting to notionify.
    if len(text) <= max_chars:
        return [text] if text else []
    chunks = []
    i = 0
    while i < len(text):
        chunks.append(text[i : i + max_chars])
        i += max_chars
    return chunks


@register_sink("notion")
class NotionSink(Sink):
    type = "notion"

    def __init__(
        self,
        *,
        id: str,
        input: str,
        parent_id: str,
        parent_type: str,
        title_template: str,
        token: str = "env:NOTION_TOKEN",
        properties: dict | None = None,
        mode: str = "upsert",
        max_block_chars: int = 1900,
        emit_on_empty: bool = False,
        adapter: NotionifyAdapter | None = None,
    ):
        super().__init__(id=id, mode=mode, emit_on_empty=emit_on_empty)
        self.input = input
        self.parent_id_raw = parent_id
        self.parent_type = parent_type
        self.title_template = title_template
        self.token_raw = token
        self.properties = properties or {}
        self.max_block_chars = max_block_chars
        self._adapter_override = adapter

    def _get_adapter(self) -> NotionifyAdapter:
        if self._adapter_override is not None:
            return self._adapter_override
        return DefaultNotionifyAdapter(token=resolve_env(self.token_raw))

    def _markdown(self, ctx: PipelineContext) -> str:
        head, _, key = self.input.partition(".")
        return ctx.context[key]

    def _title(self, ctx: PipelineContext) -> str:
        return self.title_template.format(run_date=ctx.run_date, pipeline_id=ctx.pipeline_id)

    async def create(self, ctx: PipelineContext) -> str:
        parent = resolve_env(self.parent_id_raw)
        adapter = self._get_adapter()
        page_id = await adapter.create_page(
            parent_id=parent,
            parent_type=self.parent_type,
            title=self._title(ctx),
            properties=self.properties,
            markdown=self._markdown(ctx),
        )
        return page_id

    async def update(self, external_id: str, ctx: PipelineContext) -> str:
        adapter = self._get_adapter()
        await adapter.replace_page_blocks(external_id, self._markdown(ctx))
        return external_id
