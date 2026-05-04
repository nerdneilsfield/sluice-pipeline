from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Awaitable, Callable

import feedparser
from jinja2 import Template

from sluice.dag.item import ITEM_COLUMNS, Item
from sluice.dag.stage_context import ItemDrop, ItemError, StageContext, StageResult
from sluice.dag.stage_executor import StageExecutor, bind_executor, is_bound

MISSING = object()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        text = value.replace("Z", "+00:00")
        try:
            out = datetime.fromisoformat(text)
            return out if out.tzinfo else out.replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                out = parsedate_to_datetime(value)
                return out if out.tzinfo else out.replace(tzinfo=timezone.utc)
            except (TypeError, ValueError):
                return None
    return None


def _json_loads(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return default
    return raw


def _item_value(item: Item, field: str, default: Any = None) -> Any:
    if field.startswith("extras."):
        return item.get_extra(field[len("extras.") :], default)
    if field in ITEM_COLUMNS:
        value = item[field]
        return default if value is None else value
    return item.get_volatile(field, default)


def _set_item_value(item: Item, field: str, value: Any) -> None:
    if field.startswith("extras."):
        item.set_extra(field[len("extras.") :], value)
        return
    if field in ITEM_COLUMNS:
        item[field] = value
        return
    item.set_extra(field, value)


def _maybe_await(value: Any) -> Awaitable[Any]:
    if hasattr(value, "__await__"):
        return value

    async def done() -> Any:
        return value

    return done()


def _item_view(item: Item) -> dict[str, Any]:
    data = {column: item[column] for column in ITEM_COLUMNS}
    data["extras"] = _json_loads(item["extras_json"], {})
    data["tags"] = _json_loads(item["tags_json"], [])
    data["attachments"] = _json_loads(item["attachments_json"], [])
    return data


def _drop(item: Item, reason: str) -> ItemDrop:
    item.mark_dropped(reason)
    return ItemDrop(str(item["item_id"]), reason)


class FetchRssExecutor(StageExecutor):
    type_name = "fetch_rss"

    def __init__(
        self,
        *,
        _fetch_text: Callable[[str], Awaitable[str] | str] | None = None,
        _now: Callable[[], datetime] | None = None,
    ) -> None:
        self._fetch_text = _fetch_text or self._default_fetch_text
        self._now = _now or _utc_now

    async def _default_fetch_text(self, url: str) -> str:
        import httpx

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.text

    async def execute(self, ctx: StageContext) -> StageResult:
        window_start = _parse_dt(ctx.params.get("window_start")) or datetime.min.replace(
            tzinfo=timezone.utc
        )
        window_end = _parse_dt(ctx.params.get("window_end")) or self._now()
        skip_ids = {str(item_id) for item_id in ctx.params.get("skip_item_ids", [])}
        new_items: list[Item] = []
        errors: list[ItemError] = []

        for index, feed_def in enumerate(ctx.params.get("feeds", [])):
            url = feed_def["url"] if isinstance(feed_def, dict) else str(feed_def)
            source_name = (
                feed_def.get("name")
                or feed_def.get("source_name")
                or feed_def.get("source")
                or f"rss_{index}"
                if isinstance(feed_def, dict)
                else f"rss_{index}"
            )
            tag = feed_def.get("tag") if isinstance(feed_def, dict) else None
            try:
                text = await _maybe_await(self._fetch_text(url))
            except Exception as exc:
                errors.append(ItemError(url, type(exc).__name__, str(exc)))
                continue
            feed = feedparser.parse(text)
            for entry in list(feed.entries):
                published = self._entry_date(entry)
                if published is not None:
                    if published > window_end + timedelta(hours=1):
                        continue
                    if published < window_start or published > window_end:
                        continue
                guid = str(
                    getattr(entry, "id", None)
                    or getattr(entry, "guid", None)
                    or getattr(entry, "link", None)
                    or getattr(entry, "title", "")
                )
                item_id = f"{source_name}:{guid}"
                if item_id in skip_ids:
                    continue
                now_text = self._now().isoformat()
                item = Item()
                item["item_id"] = item_id
                item["source_name"] = source_name
                item["guid"] = guid
                item["url"] = str(getattr(entry, "link", "") or "")
                item["title"] = str(getattr(entry, "title", "") or "")
                item["author"] = getattr(entry, "author", None)
                item["published_at"] = published.isoformat() if published else None
                item["ingested_at"] = now_text
                item["ingested_by"] = ctx.pipeline_name
                item["updated_at"] = now_text
                item["tag"] = tag
                item["raw_summary"] = self._raw_summary(entry)
                item["raw_content"] = self._raw_content(entry)
                item["raw_json"] = json.dumps(dict(entry), default=str, ensure_ascii=False)
                item["attachments_json"] = json.dumps(self._attachments(entry), ensure_ascii=False)
                item["extras_json"] = "{}"
                item["tags_json"] = json.dumps([tag] if tag else [], ensure_ascii=False)
                item["dropped"] = 0
                new_items.append(item)
        return StageResult(new_items=new_items, errors=errors)

    @staticmethod
    def _entry_date(entry: Any) -> datetime | None:
        for attr in ("published_parsed", "updated_parsed"):
            parsed = getattr(entry, attr, None)
            if parsed:
                return datetime(*parsed[:6], tzinfo=timezone.utc)
        return _parse_dt(getattr(entry, "published", None) or getattr(entry, "updated", None))

    @staticmethod
    def _raw_summary(entry: Any) -> str | None:
        return getattr(entry, "summary", None) or getattr(entry, "description", None)

    @staticmethod
    def _raw_content(entry: Any) -> str | None:
        content = getattr(entry, "content", None) or []
        for part in content:
            value = part.get("value") if isinstance(part, dict) else getattr(part, "value", None)
            if value:
                return str(value)
        return None

    @staticmethod
    def _attachments(entry: Any) -> list[dict[str, Any]]:
        out = []
        for enc in getattr(entry, "enclosures", []) or []:
            url = enc.get("href") or enc.get("url")
            if not url:
                continue
            out.append(
                {
                    "url": url,
                    "mime_type": enc.get("type"),
                    "length": int(enc["length"]) if str(enc.get("length", "")).isdigit() else None,
                }
            )
        return out


class DedupeExecutor(StageExecutor):
    type_name = "dedupe"

    async def execute(self, ctx: StageContext) -> StageResult:
        seen = {str(value) for value in ctx.params.get("already_seen_ids", [])}
        return StageResult(
            drops=[
                _drop(item, "already_seen")
                for item in ctx.live_items()
                if item["item_id"] in seen
            ]
        )


class CrossDedupeExecutor(StageExecutor):
    type_name = "cross_dedupe"

    async def execute(self, ctx: StageContext) -> StageResult:
        key = ctx.params.get("key", "url")
        seen: set[Any] = set()
        drops: list[ItemDrop] = []
        for item in ctx.live_items():
            value = _item_value(item, key)
            if value in (None, ""):
                continue
            if value in seen:
                drops.append(_drop(item, f"duplicate:{key}"))
                continue
            seen.add(value)
        return StageResult(drops=drops)


class FilterExecutor(StageExecutor):
    type_name = "filter"

    async def execute(self, ctx: StageContext) -> StageResult:
        mode = ctx.params.get("mode", "keep_if_all")
        rules = list(ctx.params.get("rules", []))
        drops = []
        for item in ctx.live_items():
            results = [self._eval(rule, item) for rule in rules]
            keep = self._keep(mode, results)
            if not keep:
                drops.append(_drop(item, "filter"))
        return StageResult(drops=drops)

    @staticmethod
    def _keep(mode: str, results: list[bool]) -> bool:
        if mode == "keep_if_all":
            return all(results)
        if mode == "keep_if_any":
            return any(results)
        if mode == "drop_if_all":
            return not all(results)
        if mode == "drop_if_any":
            return not any(results)
        raise ValueError(f"unknown filter mode: {mode}")

    @staticmethod
    def _eval(rule: dict[str, Any], item: Item) -> bool:
        field = rule["field"]
        op = rule.get("op", "eq")
        target = rule.get("value")
        value = _item_value(item, field)
        if op == "exists":
            return value is not None
        if op == "not_exists":
            return value is None
        if value is None:
            return False
        if op == "eq":
            return value == target
        if op == "neq":
            return value != target
        if op == "contains":
            return str(target) in str(value)
        if op == "not_contains":
            return str(target) not in str(value)
        if op == "matches":
            return bool(re.search(str(target), str(value)))
        if op == "not_matches":
            return not re.search(str(target), str(value))
        if op == "in":
            return value in target
        if op == "not_in":
            return value not in target
        if op in {"gt", "gte", "lt", "lte"}:
            if op == "gt":
                return value > target
            if op == "gte":
                return value >= target
            if op == "lt":
                return value < target
            return value <= target
        if op == "min_length":
            return len(value) >= int(target)
        if op == "max_length":
            return len(value) <= int(target)
        raise ValueError(f"unknown filter op: {op}")


class FieldFilterExecutor(StageExecutor):
    type_name = "field_filter"

    async def execute(self, ctx: StageContext) -> StageResult:
        for item in ctx.live_items():
            for op in ctx.params.get("ops", []):
                self._apply(item, op)
        return StageResult()

    def _apply(self, item: Item, op: dict[str, Any]) -> None:
        name = op["op"]
        field = op["field"]
        value = _item_value(item, field)
        if name == "drop":
            _set_item_value(item, field, None)
        elif name == "truncate" and isinstance(value, str):
            _set_item_value(item, field, value[: int(op.get("n", 0))])
        elif name == "lower" and isinstance(value, str):
            _set_item_value(item, field, value.lower())
        elif name == "strip" and isinstance(value, str):
            _set_item_value(item, field, value.strip(op.get("chars")))
        elif name == "regex_replace" and isinstance(value, str):
            _set_item_value(
                item,
                field,
                re.sub(
                    op["pattern"],
                    op.get("replacement", ""),
                    value,
                    count=int(op.get("count", 0)),
                ),
            )


class _PlainTextHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.chunks: list[str] = []
        self.drop_depth = 0

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        normalized = tag.lower()
        if normalized in {"script", "style", "template"}:
            self.drop_depth += 1
        elif not self.drop_depth and normalized in {"br", "p", "div", "li", "section", "article"}:
            self.chunks.append("\n")

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.lower()
        if normalized in {"script", "style", "template"} and self.drop_depth:
            self.drop_depth -= 1
        elif not self.drop_depth and normalized in {"p", "div", "li", "section", "article"}:
            self.chunks.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.drop_depth:
            self.chunks.append(data)


def _strip_html(value: str) -> str:
    parser = _PlainTextHTMLParser()
    parser.feed(value)
    parser.close()
    lines = [re.sub(r"[^\S\n]+", " ", line).strip() for line in "".join(parser.chunks).splitlines()]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(line for line in lines if line)).strip()


class HtmlStripExecutor(StageExecutor):
    type_name = "html_strip"

    async def execute(self, ctx: StageContext) -> StageResult:
        for item in ctx.live_items():
            for field in ctx.params.get("fields", ["raw_summary", "raw_content", "fulltext"]):
                value = _item_value(item, field)
                if isinstance(value, str):
                    _set_item_value(item, field, _strip_html(value))
        return StageResult()


class FetcherApplyExecutor(StageExecutor):
    type_name = "fetcher_apply"

    def __init__(
        self,
        *,
        fetch: Callable[[Item], Awaitable[str | None] | str | None] | None = None,
    ) -> None:
        self._fetch = fetch

    async def execute(self, ctx: StageContext) -> StageResult:
        write_field = ctx.params.get("write_field", "fulltext")
        skip_field = ctx.params.get("skip_if_field", "raw_summary")
        min_len = int(ctx.params.get("skip_if_field_longer_than", 0) or 0)
        needs_backend = [
            item
            for item in ctx.live_items()
            if not (
                min_len
                and len(str(_item_value(item, skip_field, "") or "")) >= min_len
            )
        ]
        if needs_backend:
            _require_backend(self._fetch, ctx)
        drops: list[ItemDrop] = []
        errors: list[ItemError] = []
        for item in ctx.live_items():
            existing = _item_value(item, skip_field, "") or ""
            if min_len and len(str(existing)) >= min_len:
                _set_item_value(item, write_field, existing)
                continue
            try:
                value = await _maybe_await(self._fetch(item))
            except Exception as exc:
                errors.append(ItemError(str(item["item_id"]), type(exc).__name__, str(exc)))
                if ctx.params.get("on_all_failed") == "drop":
                    drops.append(_drop(item, "fetch_failed"))
                continue
            if value is not None:
                _set_item_value(item, write_field, value)
        return StageResult(drops=drops, errors=errors)


class MirrorAttachmentsExecutor(StageExecutor):
    type_name = "mirror_attachments"

    def __init__(
        self,
        *,
        mirror: Callable[
            [str, dict[str, Any]],
            Awaitable[dict[str, Any] | None] | dict[str, Any] | None,
        ]
        | None = None,
    ) -> None:
        self._mirror = mirror

    async def execute(self, ctx: StageContext) -> StageResult:
        _require_backend(self._mirror, ctx)
        errors: list[ItemError] = []
        for item in ctx.live_items():
            attachments = _json_loads(item["attachments_json"], [])
            changed = False
            out = []
            for attachment in attachments:
                url = attachment.get("url") if isinstance(attachment, dict) else None
                if not url:
                    out.append(attachment)
                    continue
                try:
                    mirrored = await _maybe_await(self._mirror(url, attachment))
                except Exception as exc:
                    errors.append(ItemError(str(item["item_id"]), type(exc).__name__, str(exc)))
                    if ctx.params.get("on_failure") == "drop_attachment":
                        changed = True
                        continue
                    out.append(attachment)
                    continue
                out.append(mirrored or attachment)
                changed = changed or mirrored is not None
            if changed:
                item["attachments_json"] = json.dumps(out, ensure_ascii=False)
        return StageResult(errors=errors)


class EnrichExecutor(StageExecutor):
    type_name = "enrich"

    def __init__(self, *, enrich: Callable[[Item], Awaitable[Any] | Any] | None = None) -> None:
        self._enrich = enrich

    async def execute(self, ctx: StageContext) -> StageResult:
        _require_backend(self._enrich, ctx)
        output_key = ctx.params.get("output_key", ctx.params.get("output_field", "enrich"))
        errors: list[ItemError] = []
        for item in ctx.live_items():
            try:
                value = await _maybe_await(self._enrich(item))
            except Exception as exc:
                errors.append(ItemError(str(item["item_id"]), type(exc).__name__, str(exc)))
                continue
            if value is not None:
                _set_item_value(item, output_key, value)
        return StageResult(errors=errors)


class LLMStageExecutor(StageExecutor):
    type_name = "llm_stage"

    def __init__(self, *, llm: Callable[[str, Item], Awaitable[Any] | Any] | None = None) -> None:
        self._llm = llm

    async def execute(self, ctx: StageContext) -> StageResult:
        _require_backend(self._llm, ctx)
        input_field = ctx.params.get("input_field", "fulltext")
        output_field = ctx.params.get("output_field", "extras.llm")
        template = Template(ctx.params.get("prompt", "{{ input }}"))
        errors: list[ItemError] = []
        for item in ctx.live_items():
            prompt = template.render(
                item=_item_view(item),
                input=_item_value(item, input_field, ""),
            )
            try:
                value = await _maybe_await(self._llm(prompt, item))
            except Exception as exc:
                errors.append(ItemError(str(item["item_id"]), type(exc).__name__, str(exc)))
                continue
            _set_item_value(item, output_field, value)
        return StageResult(errors=errors)


class ScoreTagExecutor(StageExecutor):
    type_name = "score_tag"

    def __init__(self, *, llm: Callable[[str, Item], Awaitable[Any] | Any] | None = None) -> None:
        self._llm = llm

    async def execute(self, ctx: StageContext) -> StageResult:
        return await _score_or_summarize(ctx, self._llm, include_summary=False)


class SummarizeScoreTagExecutor(StageExecutor):
    type_name = "summarize_score_tag"

    def __init__(self, *, llm: Callable[[str, Item], Awaitable[Any] | Any] | None = None) -> None:
        self._llm = llm

    async def execute(self, ctx: StageContext) -> StageResult:
        return await _score_or_summarize(ctx, self._llm, include_summary=True)


async def _score_or_summarize(
    ctx: StageContext,
    llm: Callable[[str, Item], Awaitable[Any] | Any] | None,
    *,
    include_summary: bool,
) -> StageResult:
    _require_backend(llm, ctx)
    input_field = ctx.params.get("input_field", "fulltext")
    template = Template(ctx.params.get("prompt", "{{ input }}"))
    errors: list[ItemError] = []
    for item in ctx.live_items():
        prompt = template.render(item=_item_view(item), input=_item_value(item, input_field, ""))
        try:
            raw = await _maybe_await(llm(prompt, item))
            data = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(data, dict):
                raise ValueError("LLM result must be an object")
            if "score" in data:
                item["score"] = max(1, min(10, round(float(data["score"]))))
            if "tags" in data:
                item["tags_json"] = json.dumps(list(data["tags"] or []), ensure_ascii=False)
            if include_summary and "summary" in data:
                item[ctx.params.get("summary_field", "summary")] = str(data["summary"])
        except Exception as exc:
            errors.append(ItemError(str(item["item_id"]), type(exc).__name__, str(exc)))
    return StageResult(errors=errors)


class SortExecutor(StageExecutor):
    type_name = "sort"
    reorders_items = True

    async def execute(self, ctx: StageContext) -> StageResult:
        sort_by = ctx.params.get("sort_by", ctx.params.get("field", "published_at"))
        reverse = ctx.params.get("sort_order", "desc") == "desc"
        missing = ctx.params.get("sort_missing", "last")

        def key(item: Item) -> tuple[int, Any]:
            value = _item_value(item, sort_by, MISSING)
            if value is MISSING or value is None:
                return (0 if missing == "first" else 2, "")
            return (1, self._coerce(value))

        present = [item for item in ctx.items if key(item)[0] == 1]
        absent = [item for item in ctx.items if key(item)[0] != 1]
        present.sort(key=lambda item: key(item)[1], reverse=reverse)
        if missing == "drop":
            drops = []
            for item in absent:
                item.mark_dropped("sort_missing")
                drops.append(ItemDrop(str(item["item_id"]), "sort_missing"))
            ctx.items[:] = present
            return StageResult(drops=drops)
        elif missing == "first":
            ctx.items[:] = absent + present
        else:
            ctx.items[:] = present + absent
        return StageResult()

    @staticmethod
    def _coerce(value: Any) -> Any:
        if isinstance(value, str):
            parsed = _parse_dt(value)
            if parsed is not None:
                return parsed.timestamp()
            try:
                return float(value)
            except ValueError:
                return value.lower()
        if isinstance(value, datetime):
            return value.timestamp()
        return value


class LimitExecutor(StageExecutor):
    type_name = "limit"

    async def execute(self, ctx: StageContext) -> StageResult:
        top_n = int(ctx.params.get("top_n", ctx.params.get("limit", len(ctx.items))))
        live_ids = {item["item_id"] for item in ctx.live_items()}
        keep_ids = {item["item_id"] for item in ctx.items[:top_n] if item["item_id"] in live_ids}
        return StageResult(
            drops=[
                _drop(item, "limit")
                for item in ctx.live_items()
                if item["item_id"] not in keep_ids
            ]
        )


class RenderExecutor(StageExecutor):
    type_name = "render"

    async def execute(self, ctx: StageContext) -> StageResult:
        template = ctx.params.get("template", "")
        if "template_file" in ctx.params:
            path = Path(ctx.params["template_file"])
            config_root = ctx.params.get("config_root")
            if not path.is_absolute() and not config_root:
                raise RuntimeError("render template_file requires config_root for relative paths")
            if config_root and not path.is_absolute():
                path = Path(config_root) / path
            template = path.read_text(encoding="utf-8")
        rendered = Template(template).render(
            items=[_item_view(item) for item in ctx.live_items()],
            context=ctx.params.get("context", {}),
            task_name=ctx.task_name,
            pipeline_name=ctx.pipeline_name,
            run_key=ctx.run_key,
            cycle_anchor=ctx.cycle_anchor,
        )
        return StageResult(rendered_output=rendered)


def bind_all() -> None:
    for cls in [
        FetchRssExecutor,
        DedupeExecutor,
        CrossDedupeExecutor,
        FilterExecutor,
        FieldFilterExecutor,
        HtmlStripExecutor,
        FetcherApplyExecutor,
        MirrorAttachmentsExecutor,
        EnrichExecutor,
        LLMStageExecutor,
        ScoreTagExecutor,
        SummarizeScoreTagExecutor,
        SortExecutor,
        LimitExecutor,
        RenderExecutor,
    ]:
        if not is_bound(cls.type_name):
            bind_executor(cls.type_name, cls)


def _require_backend(backend: object | None, ctx: StageContext) -> None:
    if backend is None and ctx.live_items():
        raise RuntimeError(f"{ctx.stage_type} requires a backend")
