from typing import Literal

import httpx

from sluice.context import PipelineContext
from sluice.core.dotpath import set_dotpath
from sluice.core.errors import ConfigError
from sluice.core.item import Item
from sluice.fetchers._ssrf import SSRFError, guard
from sluice.processors.base import register_processor
from sluice.state.attachment_store import AttachmentStore
from sluice.state.attachments import format_url


class MirrorAttachmentsProcessor:
    name: str
    type = "mirror_attachments"

    def __init__(
        self,
        *,
        name: str,
        store: AttachmentStore,
        pipeline_id: str,
        mime_prefixes: list[str],
        max_bytes: int,
        on_failure: Literal["skip", "fail", "drop_attachment"],
        rewrite_fields: list[str],
        attachment_url_prefix: str,
        client: httpx.AsyncClient | None = None,
    ):
        self.name = name
        self._store = store
        self._pipeline_id = pipeline_id
        self._mimes = tuple(mime_prefixes)
        self._max = max_bytes
        self._on_failure = on_failure
        self._rewrite = list(rewrite_fields)
        self._prefix = attachment_url_prefix
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=30.0)

    async def aclose(self):
        if self._owns_client:
            await self._client.aclose()

    async def process(self, ctx: PipelineContext) -> PipelineContext:
        for item in ctx.items:
            await self._mirror_attachments(item)
            await self._mirror_extras(item)
        return ctx

    async def _mirror_one(self, url: str) -> tuple[str, str | None] | None:
        try:
            guard(url)
        except SSRFError:
            return None
        try:
            current = url
            for _ in range(5):
                resp = await self._client.get(current, follow_redirects=False)
                if resp.is_redirect:
                    location = resp.headers.get("location")
                    if not location:
                        if self._on_failure == "fail":
                            raise httpx.HTTPError(
                                f"redirect without Location header from {current}"
                            )
                        return None
                    next_url = httpx.URL(current).join(location).human_repr()
                    guard(next_url)
                    current = next_url
                    continue
                resp.raise_for_status()
                break
            else:
                if self._on_failure == "fail":
                    raise httpx.HTTPError("too many redirects")
                return None
        except (httpx.HTTPError, httpx.InvalidURL, SSRFError):
            if self._on_failure == "fail":
                raise
            return None
        mime = resp.headers.get("content-type", "").split(";", 1)[0].strip()
        if self._mimes and not any(mime.startswith(p) for p in self._mimes):
            return None
        data = resp.content
        if len(data) > self._max:
            if self._on_failure == "fail":
                raise ConfigError(f"attachment over max_bytes: {url} {len(data)}")
            return None
        local_path = await self._store.put_bytes(
            url=url,
            mime_type=mime,
            data=data,
            pipeline_id=self._pipeline_id,
        )
        return (format_url(local_path, self._prefix), local_path)

    async def _mirror_attachments(self, item: Item) -> None:
        kept = []
        for att in item.attachments:
            res = await self._mirror_one(att.url)
            if res is None:
                if self._on_failure == "drop_attachment":
                    continue
                kept.append(att)
                continue
            new_url, local_path = res
            att.url = new_url
            att.local_path = local_path
            kept.append(att)
        item.attachments = kept

    async def _mirror_extras(self, item: Item) -> None:
        for path in self._rewrite:
            cur = item.get(path, None)
            if not isinstance(cur, str):
                continue
            res = await self._mirror_one(cur)
            if res is None:
                continue
            set_dotpath(item, path, res[0])


register_processor("mirror_attachments")(MirrorAttachmentsProcessor)
