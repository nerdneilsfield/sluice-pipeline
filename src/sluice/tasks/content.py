"""Deterministic content Tasks and the startup HTTP boundary."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from html.parser import HTMLParser
import re
from typing import Any

from ..contracts import NEW, HTTPClient, Item
from ..canonical import canonical_json
from ..errors import ExecutionError
from .collection import field_value


def _input_text(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, Item):
        content = field_value(value, "content")
        if isinstance(content, str):
            return content
    raise ExecutionError("content Task input must be HTML/text or an Item with content")


def _url(config: Mapping[str, Any], value: object) -> str:
    configured = config.get("url")
    if configured is not None:
        if not isinstance(configured, str) or not configured:
            raise ExecutionError("fetch_webpage.url must be a non-empty string")
        return configured
    if isinstance(value, str) and value:
        return value
    if isinstance(value, Item):
        path = config.get("url_field", "url")
        if not isinstance(path, str) or not path:
            raise ExecutionError("fetch_webpage.url_field must be a non-empty string")
        found = field_value(value, path)
        if found is None and path == "url":
            found = field_value(value, "link")
        if isinstance(found, str) and found:
            return found
    raise ExecutionError("fetch_webpage requires a URL or Item URL field")


@dataclass(frozen=True, slots=True)
class _FetchTask:
    http_client: HTTPClient | None = None
    input_mode: str = "each"

    def bind(self, adapters: object) -> "_FetchTask":
        return _FetchTask(getattr(adapters, "http_client", None))

    def run(self, config: Mapping[str, Any], input: object) -> object:
        if self.http_client is None:
            raise ExecutionError("fetch_webpage has no startup HTTP adapter")
        url = _url(config, input)
        try:
            response = self.http_client.get(url)
        except BaseException as error:
            raise ExecutionError("webpage HTTP request failed") from error
        if not isinstance(response, str):
            raise ExecutionError("webpage HTTP response must be text")
        return NEW(response)

    def __call__(self, config: Mapping[str, Any], input: object) -> object:
        return self.run(config, input)


class _TextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._ignored = 0

    def handle_starttag(self, tag: str, attrs):
        if tag.lower() in {"script", "style", "noscript", "template"}:
            self._ignored += 1

    def handle_endtag(self, tag: str):
        if tag.lower() in {"script", "style", "noscript", "template"} and self._ignored:
            self._ignored -= 1
        elif tag.lower() in {"p", "div", "section", "article", "li", "h1", "h2", "h3", "h4", "h5", "h6", "br"}:
            self.parts.append("\n")

    def handle_data(self, data: str):
        if self._ignored:
            return
        text = re.sub(r"\s+", " ", data)
        if text.strip():
            self.parts.append(text)


def _plain_text(html: str) -> str:
    parser = _TextParser()
    try:
        parser.feed(html)
        parser.close()
    except (TypeError, ValueError) as error:
        raise ExecutionError("invalid HTML content") from error
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in "".join(parser.parts).split("\n")]
    return "\n".join(line for line in lines if line).strip()


def _extract_content(config: Mapping[str, Any], input: object) -> object:
    """Extract visible, normalized text from an HTML value."""

    del config
    return NEW(_plain_text(_input_text(input)))


class _MarkdownParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.link_href: str | None = None
        self._ignored = 0

    def _newline(self) -> None:
        if self.parts and not self.parts[-1].endswith("\n"):
            self.parts.append("\n")

    def handle_starttag(self, tag: str, attrs):
        tag = tag.lower()
        if tag in {"script", "style", "noscript", "template"}:
            self._ignored += 1
            return
        if self._ignored:
            return
        attributes = {key: value or "" for key, value in attrs}
        if tag in {"p", "div", "section", "article", "blockquote", "li"}:
            self._newline()
        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._newline()
            self.parts.append("#" * int(tag[1]) + " ")
        elif tag == "li":
            self.parts.append("- ")
        elif tag in {"strong", "b"}:
            self.parts.append("**")
        elif tag in {"em", "i"}:
            self.parts.append("_")
        elif tag == "code":
            self.parts.append("`")
        elif tag == "a":
            self.parts.append("[")
            self.link_href = attributes.get("href")
        elif tag == "br":
            self._newline()

    def handle_endtag(self, tag: str):
        tag = tag.lower()
        if tag in {"script", "style", "noscript", "template"}:
            if self._ignored:
                self._ignored -= 1
            return
        if self._ignored:
            return
        if tag in {"strong", "b"}:
            self.parts.append("**")
        elif tag in {"em", "i"}:
            self.parts.append("_")
        elif tag == "code":
            self.parts.append("`")
        elif tag == "a":
            self.parts.append("](" + (self.link_href or "") + ")")
            self.link_href = None
        if tag in {"p", "div", "section", "article", "blockquote", "li", "h1", "h2", "h3", "h4", "h5", "h6"}:
            self._newline()

    def handle_data(self, data: str):
        if self._ignored:
            return
        self.parts.append(re.sub(r"\s+", " ", data))


def _markdown(value: str) -> str:
    parser = _MarkdownParser()
    try:
        parser.feed(value)
        parser.close()
    except (TypeError, ValueError) as error:
        raise ExecutionError("invalid HTML content") from error
    text = "".join(parser.parts)
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]
    # Collapse adjacent blank lines while retaining paragraph boundaries.
    output: list[str] = []
    for line in lines:
        if not line:
            if output and output[-1] != "":
                output.append("")
            continue
        output.append(line)
    while output and output[-1] == "":
        output.pop()
    return "\n".join(output)


def _to_markdown(config: Mapping[str, Any], input: object) -> object:
    """Convert HTML/text into deterministic Markdown text."""

    del config
    value = _input_text(input)
    return NEW(_markdown(value))


def _render_payload(config: Mapping[str, Any], input: object) -> str:
    """Render an Item into deterministic channel text.

    Renderers deliberately have no clock, network, or process-global state.
    A configured ``template`` may use ordinary ``str.format`` placeholders
    against a mapping payload; without one, scalar payloads are rendered as
    text and structured payloads as canonical JSON.  The output is always a
    JSON-compatible string so it can be cached by the normal Stage runtime.
    """

    if not isinstance(input, Item):
        raise ExecutionError("channel renderer input must be an Item")
    payload = input.payload
    template = config.get("template")
    if template is not None:
        if not isinstance(template, str):
            raise ExecutionError("renderer.template must be a string")
        if isinstance(payload, Mapping):
            class _Values(dict[str, object]):
                def __missing__(self, key: str) -> object:
                    return ""

            try:
                return template.format_map(_Values(payload))
            except (KeyError, IndexError, ValueError) as error:
                raise ExecutionError("renderer.template is invalid") from error
        try:
            return template.format(payload)
        except (IndexError, KeyError, ValueError) as error:
            raise ExecutionError("renderer.template is invalid") from error
    if isinstance(payload, str):
        return payload
    if payload is None or isinstance(payload, (bool, int, float)):
        return str(payload)
    return canonical_json(payload)


def _render_email(config: Mapping[str, Any], input: object) -> object:
    return NEW(_render_payload(config, input))


def _render_telegram(config: Mapping[str, Any], input: object) -> object:
    return NEW(_render_payload(config, input))


@dataclass(frozen=True, slots=True)
class _FunctionTask:
    input_mode: str
    function: Any

    def run(self, config: Mapping[str, Any], input: object) -> object:
        return self.function(config, input)

    def __call__(self, config: Mapping[str, Any], input: object) -> object:
        return self.run(config, input)


# Public Task objects remain directly callable in unit tests and are also
# consumed by the startup resolution boundary.
fetch_webpage = _FetchTask()
extract_content = _FunctionTask("each", _extract_content)
to_markdown = _FunctionTask("each", _to_markdown)
render_email = _FunctionTask("each", _render_email)
render_telegram = _FunctionTask("each", _render_telegram)
