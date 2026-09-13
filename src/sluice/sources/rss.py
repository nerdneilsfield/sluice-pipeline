"""Private WebContentClient-backed RSS 2, Atom, and RSS 1 Sources."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from sluice.canonical import canonical_json
from sluice.contracts import HTTPClient, JSONValue, KEEP_CHECKPOINT, SourceRecord, SourceResult
from sluice.errors import ExecutionError
from sluice.web.contracts import (
    DocumentArtifact,
    FetchRequest,
    PrivateWebInvocation,
    WebAttemptCancelled,
    WebStageDeadlineExpired,
)
from sluice.web.feed import validate_raw_feed
from sluice.web.projectors import project_markdown, project_text
from sluice.web.url import normalize_target_url_string


def _local_name(tag: object) -> str:
    return tag.rsplit("}", 1)[-1].lower() if isinstance(tag, str) else ""


def _text(element: Any | None) -> str | None:
    if element is None:
        return None
    value = "".join(element.itertext()).strip()
    return value or None


def _child(element: Any, *names: str) -> Any | None:
    wanted = {name.lower() for name in names}
    for candidate in list(element):
        if _local_name(getattr(candidate, "tag", None)) in wanted:
            return candidate
    return None


def _field(element: Any, *names: str) -> str | None:
    return _text(_child(element, *names))


def _link(element: Any, final_url: str) -> str | None:
    for candidate in list(element):
        if _local_name(getattr(candidate, "tag", None)) != "link":
            continue
        href = candidate.attrib.get("href")
        value = href.strip() if isinstance(href, str) and href.strip() else _text(candidate)
        if not value:
            continue
        try:
            return normalize_target_url_string(urljoin(final_url, value))
        except Exception:
            continue
    return None


def _request_url(config: Mapping[str, JSONValue], checkpoint: JSONValue) -> str:
    url = config.get("url")
    if not isinstance(url, str) or not url:
        raise ExecutionError("RSS Source requires a non-empty url")
    parameter = config.get("cursor_param", config.get("checkpoint_param"))
    if parameter is None or checkpoint is None or checkpoint is KEEP_CHECKPOINT:
        return url
    if not isinstance(parameter, str) or not parameter:
        raise ExecutionError("RSS cursor_param must be a non-empty string")
    cursor_text = checkpoint if isinstance(checkpoint, str) else canonical_json(checkpoint)
    split = urlsplit(url)
    query = [(key, value) for key, value in parse_qsl(split.query, keep_blank_values=True) if key != parameter]
    query.append((parameter, cursor_text))
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(query), split.fragment))


def _is_html(value: str) -> bool:
    stripped = value.lstrip()
    return stripped.startswith("<") and ">" in stripped


def _feed_entries(root: Any, kind: str) -> tuple[Any, ...]:
    """Return only entries permitted by the validated root kind."""

    children = tuple(root)
    if kind == "atom":
        return tuple(child for child in children if _local_name(getattr(child, "tag", None)) == "entry")
    if kind == "rss1":
        return tuple(child for child in children if _local_name(getattr(child, "tag", None)) == "item")
    if kind == "rss2":
        channel = next(
            (child for child in children if _local_name(getattr(child, "tag", None)) == "channel"),
            None,
        )
        if channel is None:
            return ()
        return tuple(
            child for child in tuple(channel)
            if _local_name(getattr(child, "tag", None)) == "item"
        )
    raise ExecutionError("invalid RSS raw source")


def _entry_record(entry: Any, index: int, artifact: DocumentArtifact, output: str) -> SourceRecord:
    final_url = artifact.final_url
    if not isinstance(final_url, str):
        raise ExecutionError("RSS response has no final URL")
    entry_id = _field(entry, "guid", "id")
    link = _link(entry, final_url)
    title = _field(entry, "title")
    content = _field(entry, "content", "encoded", "description", "summary")
    published = _field(entry, "published", "pubdate")
    updated = _field(entry, "updated", "modified")
    logical_id = entry_id or link or title or f"entry-{index}"
    if not isinstance(logical_id, str) or not logical_id.strip():
        raise ExecutionError("RSS record logical_id must be a non-empty string")
    value = content or ""
    source = DocumentArtifact(
        source_url=artifact.source_url,
        final_url=final_url,
        content_type="text/html" if _is_html(value) else "text/plain",
        raw_html=value if _is_html(value) else None,
        text=None if _is_html(value) else value,
        selected_format="raw_html" if _is_html(value) else "text",
        capabilities=frozenset({"raw_html" if _is_html(value) else "text"}),
        title=title,
        provider_instance=artifact.provider_instance,
        provider_type=artifact.provider_type,
        extraction="feed_parser",
    )
    projected = project_markdown(source) if output == "markdown" else project_text(source)
    rendered = getattr(projected, "value", None)
    metadata = getattr(projected, "metadata", None)
    if not isinstance(rendered, str) or not isinstance(metadata, Mapping):
        raise ExecutionError("RSS entry projection failed")
    payload: dict[str, JSONValue] = {}
    if title is not None:
        payload["title"] = title
    if link is not None:
        payload["link"] = link
    if content is not None:
        payload["content"] = rendered
    if published is not None:
        payload["published"] = published
    if updated is not None:
        payload["updated"] = updated
    return SourceRecord(logical_id.strip(), payload, metadata)


def parse_feed_bytes(raw: bytes, final_url: str, *, artifact: DocumentArtifact | None = None, entry_output: str = "text") -> tuple[SourceRecord, ...]:
    """Parse already-bounded literal feed bytes without retaining the raw body."""

    root = validate_raw_feed(raw)
    if root is None:
        raise ExecutionError("invalid RSS raw source")
    if entry_output not in {"text", "markdown"}:
        raise ExecutionError("RSS entry_output must be text or markdown")
    source = artifact or DocumentArtifact(
        source_url=final_url,
        final_url=final_url,
        content_type="application/xml",
        raw_source=raw,
        capabilities=frozenset({"raw_source"}),
        provider_instance="__rss_direct__",
        provider_type="httpx",
    )
    records: list[SourceRecord] = []
    seen: set[str] = set()
    for index, entry in enumerate(_feed_entries(root.root, root.kind)):
        record = _entry_record(entry, index, source, entry_output)
        if record.logical_id not in seen:
            seen.add(record.logical_id)
            records.append(record)
    return tuple(records)


def _next_checkpoint(config: Mapping[str, JSONValue]) -> JSONValue:
    return config["next_checkpoint"] if "next_checkpoint" in config else KEEP_CHECKPOINT


def collect(config: Mapping[str, JSONValue], checkpoint: JSONValue) -> SourceResult:
    """Direct-only literal XML helper; it never performs Source collection I/O."""

    del checkpoint
    xml = config.get("xml", config.get("content"))
    url = config.get("url")
    if not isinstance(xml, str) or not isinstance(url, str) or not url:
        raise ExecutionError("RSS collect requires literal XML and url")
    return SourceResult(
        records=parse_feed_bytes(xml.encode("utf-8"), url, entry_output=str(config.get("entry_output", "text"))),
        next_checkpoint=_next_checkpoint(config),
    )


@dataclass(frozen=True, slots=True)
class _RSSSource:
    http_client: HTTPClient | None
    web_content_client: object | None = None

    def collect(self, config: Mapping[str, JSONValue], checkpoint: JSONValue) -> SourceResult:
        return collect(config, checkpoint)

    def collect_web(self, invocation: PrivateWebInvocation, checkpoint: JSONValue) -> SourceResult:
        if not isinstance(invocation, PrivateWebInvocation):
            raise ExecutionError("invalid private web invocation")
        if self.web_content_client is None:
            raise ExecutionError("web content client is not configured")
        resolve = getattr(self.web_content_client, "resolve", None)
        if not callable(resolve):
            raise ExecutionError("web content client has no resolve operation")
        config = invocation.with_config
        binding = invocation.web_binding
        url = _request_url(config, checkpoint)
        if binding.direct_provider is None:
            raise ExecutionError("RSS Source has no direct provider")
        request = FetchRequest(
            url=url,
            mode="raw_source",
            acceptable=frozenset({"raw_source"}),
            preferred=("raw_source",),
            quality=binding.quality,
        )
        def project(artifact: object) -> SourceResult:
            if not isinstance(artifact, DocumentArtifact) or not isinstance(artifact.raw_source, bytes):
                raise ExecutionError("RSS resolution returned no raw source")
            return SourceResult(
                records=parse_feed_bytes(
                    artifact.raw_source,
                    artifact.final_url or url,
                    artifact=artifact,
                    entry_output=str(config.get("entry_output", "text")),
                ),
                next_checkpoint=_next_checkpoint(config),
            )

        try:
            return project(resolve(request, (binding.direct_provider,), None))
        except (WebAttemptCancelled, WebStageDeadlineExpired):
            raise
        except ExecutionError:
            if binding.fetch_chain is None:
                raise
        if binding.fetch_chain is None:
            raise ExecutionError("invalid RSS raw source")
        return project(resolve(request, binding.fetch_chain, None))


def bind(adapters: object | None) -> _RSSSource:
    """Bind the immutable startup boundary without widening public contracts."""

    if hasattr(adapters, "http_client") and hasattr(adapters, "web_content_client"):
        return _RSSSource(getattr(adapters, "http_client"), getattr(adapters, "web_content_client"))
    return _RSSSource(adapters)  # type: ignore[arg-type]
