import asyncio
from collections.abc import Mapping
from typing import Any, Protocol

from sluice.context import PipelineContext
from sluice.core.errors import SinkError
from sluice.loader import resolve_env
from sluice.logging_setup import get_logger
from sluice.sinks.base import Sink, register_sink

log = get_logger(__name__)

_NOTION_PROPERTY_VALUE_KEYS = {
    "checkbox",
    "date",
    "email",
    "files",
    "multi_select",
    "number",
    "people",
    "phone_number",
    "relation",
    "rich_text",
    "select",
    "status",
    "title",
    "url",
}


class NotionifyAdapter(Protocol):
    async def create_page(
        self, *, parent_id: str, parent_type: str, title: str, properties: dict, markdown: str
    ) -> str: ...

    async def replace_page_blocks(self, page_id: str, markdown: str) -> None: ...


class DefaultNotionifyAdapter:
    def __init__(self, token: str):
        from notionify import NotionifyClient

        self._client = NotionifyClient(token=token)

    def _database_properties(self, database_id: str) -> dict[str, Any]:
        database = self._client._transport.request("GET", f"/databases/{database_id}")
        return database.get("properties", {})

    def _create_database_page_with_markdown(
        self,
        *,
        parent_id: str,
        title: str,
        properties: dict[str, Any],
        markdown: str,
    ) -> str:
        from notionify.utils.chunk import chunk_children

        schema = self._database_properties(parent_id)
        notion_properties = normalize_database_properties(properties, schema)
        title_property = _title_property_name(schema)
        notion_properties.setdefault(title_property, {"title": _rich_text(title)})

        conversion = self._client._converter.convert(markdown)
        blocks = conversion.blocks
        self._client._process_images(conversion)
        self._client._emit_conversion_metrics(conversion)

        batches = chunk_children(blocks)
        page_response = self._client._pages.create(
            parent={"database_id": parent_id},
            properties=notion_properties,
            children=batches[0] if batches else [],
        )
        page_id = page_response["id"]
        for batch in batches[1:]:
            self._client._blocks.append_children(page_id, batch)
        log.bind(
            sink_type="notion",
            parent_type="database",
            property_names=list(notion_properties),
            blocks=len(blocks),
        ).debug("notion.page_created")
        return page_id

    async def create_page(self, *, parent_id, parent_type, title, properties, markdown):
        if parent_type == "database":
            return await asyncio.to_thread(
                self._create_database_page_with_markdown,
                parent_id=parent_id,
                title=title,
                properties=properties or {},
                markdown=markdown,
            )
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


def normalize_database_properties(
    properties: dict[str, Any], schema: dict[str, Any]
) -> dict[str, Any]:
    """Expand friendly TOML values into Notion database property values."""

    unknown = [
        name
        for name, value in properties.items()
        if name not in schema and not _is_explicit_notion_property_value(value)
    ]
    if unknown:
        raise SinkError(
            "Notion database schema did not include properties: "
            f"{', '.join(sorted(unknown))}. Use explicit Notion property values "
            "or check the database parent_id/integration permissions."
        )
    return {
        name: _normalize_database_property_value(value, schema.get(name, {}))
        for name, value in properties.items()
    }


def _normalize_database_property_value(value: Any, schema_entry: dict[str, Any]) -> Any:
    if _is_explicit_notion_property_value(value):
        return value
    property_type = schema_entry.get("type")
    if property_type == "title":
        return {"title": _rich_text(value)}
    if property_type == "rich_text":
        return {"rich_text": _rich_text(value)}
    if property_type == "select":
        return {"select": None if value is None else {"name": str(value)}}
    if property_type == "multi_select":
        return {"multi_select": _multi_select(value)}
    if property_type == "status":
        return {"status": None if value is None else {"name": str(value)}}
    if property_type == "url":
        return {"url": None if value is None else str(value)}
    if property_type == "email":
        return {"email": None if value is None else str(value)}
    if property_type == "phone_number":
        return {"phone_number": None if value is None else str(value)}
    if property_type == "number":
        return {"number": value}
    if property_type == "checkbox":
        return {"checkbox": bool(value)}
    if property_type == "date":
        return {"date": value if isinstance(value, dict) else {"start": str(value)}}
    return value


def _is_explicit_notion_property_value(value: Any) -> bool:
    return isinstance(value, dict) and bool(_NOTION_PROPERTY_VALUE_KEYS & value.keys())


def _title_property_name(schema: Mapping[str, Any]) -> str:
    for name, entry in schema.items():
        if isinstance(entry, Mapping) and entry.get("type") == "title":
            return name
    return "title"


def _rich_text(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return value
    return [{"text": {"content": "" if value is None else str(value)}}]


def _multi_select(value: Any) -> list[dict[str, str]]:
    if value is None:
        return []
    values = value if isinstance(value, list) else [value]
    out = []
    for item in values:
        if isinstance(item, dict):
            out.append(item)
        else:
            out.append({"name": str(item)})
    return out


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
