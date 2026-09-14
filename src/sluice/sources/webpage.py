"""Private WebContentClient-backed webpage Source and literal test helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from sluice.contracts import (
    KEEP_CHECKPOINT,
    HTTPClient,
    JSONValue,
    SourceRecord,
    SourceResult,
)
from sluice.errors import ExecutionError
from sluice.web.contracts import DocumentArtifact, FetchRequest, PrivateWebInvocation
from sluice.web.projectors import project_markdown, project_text
from sluice.web.select import select_list_records


def _next_checkpoint(config: Mapping[str, JSONValue]) -> JSONValue:
    return config["next_checkpoint"] if "next_checkpoint" in config else KEEP_CHECKPOINT


def _record_from_projected(
    logical_id: str, url: str, projected: object, title: str | None
) -> SourceRecord:
    value = getattr(projected, "value", None)
    metadata = getattr(projected, "metadata", None)
    if not isinstance(value, str) or not isinstance(metadata, Mapping):
        raise ExecutionError("webpage projection failed")
    payload: dict[str, JSONValue] = {"url": url, "content": value}
    if title:
        payload["title"] = title
    return SourceRecord(logical_id, payload, metadata)


def _list_records(
    source_url: str,
    final_url: str,
    html: str,
    selector: str,
    *,
    provider_instance: str | None = None,
    provider_type: str | None = None,
) -> tuple[SourceRecord, ...]:
    records: list[SourceRecord] = []
    for match in select_list_records(html, final_url, selector):
        artifact = DocumentArtifact(
            source_url=source_url,
            final_url=final_url,
            content_type="text/plain",
            text=match.text,
            selected_format="text",
            capabilities=frozenset({"text"}),
            provider_instance=provider_instance,
            provider_type=provider_type,
            extraction="list_selector",
        )
        records.append(
            _record_from_projected(
                match.logical_id,
                match.url,
                project_text(artifact),
                match.title,
            )
        )
    return tuple(records)


def _page_record(artifact: DocumentArtifact, output: str) -> SourceRecord:
    if artifact.final_url is None:
        raise ExecutionError("webpage response has no final URL")
    projected = (
        project_markdown(artifact) if output == "markdown" else project_text(artifact)
    )
    return _record_from_projected(
        artifact.final_url, artifact.final_url, projected, artifact.title
    )


def _collect_html(config: Mapping[str, JSONValue], html: str) -> SourceResult:
    """Project a literal HTML body for direct deterministic helper callers."""

    if not isinstance(html, str):
        raise ExecutionError("webpage response must be text")
    url = config.get("url")
    if not isinstance(url, str) or not url:
        raise ExecutionError("webpage Source requires a non-empty url")
    selector = config.get("list_selector", config.get("selector"))
    if selector is not None and (not isinstance(selector, str) or not selector.strip()):
        raise ExecutionError("webpage selector must be a non-empty string")
    if isinstance(selector, str):
        records = _list_records(url, url, html, selector)
    else:
        output = config.get("output", "text")
        if output not in {"markdown", "text"}:
            raise ExecutionError("webpage output must be markdown or text")
        artifact = DocumentArtifact(
            source_url=url,
            final_url=url,
            content_type="text/html",
            raw_html=html,
            selected_format="raw_html",
            capabilities=frozenset({"raw_html"}),
            extraction="input",
        )
        records = (_page_record(artifact, output),)
    return SourceResult(records=records, next_checkpoint=_next_checkpoint(config))


def collect(config: Mapping[str, JSONValue], checkpoint: JSONValue) -> SourceResult:
    """Direct-only literal HTML helper; it never performs Source collection I/O."""

    del checkpoint
    html = config.get("html", config.get("content"))
    if not isinstance(html, str):
        raise ExecutionError("webpage collect requires literal HTML")
    return _collect_html(config, html)


@dataclass(frozen=True, slots=True)
class _WebpageSource:
    http_client: HTTPClient | None
    web_content_client: object | None = None

    def collect(
        self, config: Mapping[str, JSONValue], checkpoint: JSONValue
    ) -> SourceResult:
        del checkpoint
        return collect(config, KEEP_CHECKPOINT)

    def collect_web(
        self, invocation: PrivateWebInvocation, checkpoint: JSONValue
    ) -> SourceResult:
        del checkpoint
        if not isinstance(invocation, PrivateWebInvocation):
            raise ExecutionError("invalid private web invocation")
        if self.web_content_client is None:
            raise ExecutionError("web content client is not configured")
        resolve = getattr(self.web_content_client, "resolve", None)
        if not callable(resolve):
            raise ExecutionError("web content client has no resolve operation")
        config = invocation.with_config
        binding = invocation.web_binding
        url = config.get("url")
        if not isinstance(url, str) or not url:
            raise ExecutionError("webpage Source requires a non-empty url")
        if binding.fetch_chain is None:
            raise ExecutionError("webpage Source has no fetch chain")
        selector = config.get("list_selector")
        if isinstance(selector, str):
            artifact = resolve(
                FetchRequest(
                    url=url,
                    mode="list_html",
                    acceptable=frozenset({"raw_html"}),
                    preferred=("raw_html",),
                    list_selector=selector,
                    quality=binding.quality,
                ),
                binding.fetch_chain,
                None,
            )
            if not isinstance(artifact, DocumentArtifact) or not isinstance(
                artifact.raw_html, str
            ):
                raise ExecutionError("webpage list resolution returned no HTML")
            final_url = artifact.final_url or url
            records = _list_records(
                url,
                final_url,
                artifact.raw_html,
                selector,
                provider_instance=artifact.provider_instance,
                provider_type=artifact.provider_type,
            )
        else:
            if binding.extract_chain is None or binding.output not in {
                "markdown",
                "text",
            }:
                raise ExecutionError("webpage Source has no article projection")
            artifact = resolve(
                FetchRequest(
                    url=url,
                    mode="article",
                    acceptable=frozenset({"raw_html", "text", "markdown"}),
                    preferred=("markdown", "raw_html", "text"),
                    quality=binding.quality,
                ),
                binding.fetch_chain,
                binding.extract_chain,
            )
            if not isinstance(artifact, DocumentArtifact):
                raise ExecutionError("webpage article resolution failed")
            records = (_page_record(artifact, binding.output),)
        return SourceResult(records=records, next_checkpoint=_next_checkpoint(config))


def bind(adapters: object | None) -> _WebpageSource:
    """Bind the immutable startup boundary without widening public contracts."""

    if hasattr(adapters, "http_client") and hasattr(adapters, "web_content_client"):
        return _WebpageSource(adapters.http_client, adapters.web_content_client)
    return _WebpageSource(adapters)  # type: ignore[arg-type]
