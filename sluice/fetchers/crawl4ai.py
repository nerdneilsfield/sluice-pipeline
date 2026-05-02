import asyncio
import time
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

import httpx

from sluice.fetchers._ssrf import guard
from sluice.fetchers._utils import has_auth_header
from sluice.fetchers.base import register_fetcher
from sluice.logging_setup import get_logger

log = get_logger(__name__)

_SUCCESS_STATUSES = {"completed", "success", "done"}
_FAILURE_STATUSES = {"failed", "error", "cancelled"}
_DEFAULT_POLL_PATHS = [
    "/task/{task_id}",
    "/jobs/{task_id}",
]
_LOCAL_SERVICE_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}


def _markdown_value(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, Mapping):
        for key in ("raw_markdown", "fit_markdown", "markdown"):
            nested_value = value.get(key)
            if isinstance(nested_value, str) and nested_value:
                return nested_value
    return None


def _raw_value(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _candidate_mappings(data: dict) -> list[Mapping]:
    candidates: list[Any] = []
    results = data.get("results")
    if isinstance(results, Sequence) and not isinstance(results, (str, bytes)) and results:
        candidates.append(results[0])

    for key in ("result", "data"):
        value = data.get(key)
        if isinstance(value, Mapping):
            candidates.append(value)

    candidates.append(data)
    return [candidate for candidate in candidates if isinstance(candidate, Mapping)]


def _extract_markdown(data: dict) -> str | None:
    """Return the first non-empty markdown string from known Crawl4AI shapes."""
    for candidate in _candidate_mappings(data):
        markdown = _markdown_value(candidate.get("markdown"))
        if markdown:
            return markdown
        nested_results = candidate.get("results")
        if (
            isinstance(nested_results, Sequence)
            and not isinstance(nested_results, (str, bytes))
            and nested_results
            and isinstance(nested_results[0], Mapping)
        ):
            markdown = _markdown_value(nested_results[0].get("markdown"))
            if markdown:
                return markdown
    return None


def _extract_raw(data: dict) -> str | None:
    """Return the first non-empty raw HTML/XML string from known Crawl4AI shapes."""
    for candidate in _candidate_mappings(data):
        for key in ("html", "raw_html", "cleaned_html", "content"):
            raw = _raw_value(candidate.get(key))
            if raw:
                return raw
        nested_results = candidate.get("results")
        if (
            isinstance(nested_results, Sequence)
            and not isinstance(nested_results, (str, bytes))
            and nested_results
            and isinstance(nested_results[0], Mapping)
        ):
            for key in ("html", "raw_html", "cleaned_html", "content"):
                raw = _raw_value(nested_results[0].get(key))
                if raw:
                    return raw
    return None


def _get_task_id(data: dict) -> str | None:
    for key in ("task_id", "job_id", "id"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _guard_poll_url(poll_url: str) -> None:
    host = urlsplit(poll_url).hostname
    if host is not None and host.rstrip(".").lower() in _LOCAL_SERVICE_HOSTS:
        return
    guard(poll_url)


@register_fetcher("crawl4ai")
class Crawl4AIFetcher:
    name = "crawl4ai"

    def __init__(
        self,
        *,
        base_url: str = "http://localhost:11235",
        api_key: str | None = None,
        api_headers: dict | None = None,
        timeout: float = 120.0,
        poll_interval: float = 2.0,
        poll_timeout: float | None = None,
        poll_paths: list | None = None,
        wait_for: str | None = None,
        wait_for_timeout: int | None = None,
        wait_until: str | None = None,
        delay_before_return_html: float | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.api_headers = dict(api_headers) if api_headers else {}
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.poll_timeout = poll_timeout if poll_timeout is not None else timeout
        self.poll_paths = list(poll_paths) if poll_paths else list(_DEFAULT_POLL_PATHS)
        self.wait_for = wait_for
        self.wait_for_timeout = wait_for_timeout
        self.wait_until = wait_until
        self.delay_before_return_html = delay_before_return_html

    def _build_headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        headers.update(self.api_headers)
        if self.api_key and not has_auth_header(headers):
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _build_payload(self, url: str) -> dict:
        payload = {"urls": [url]}
        if self.wait_for is not None:
            payload["wait_for"] = self.wait_for
        if self.wait_for_timeout is not None:
            payload["wait_for_timeout"] = self.wait_for_timeout
        if self.wait_until is not None:
            payload["wait_until"] = self.wait_until
        if self.delay_before_return_html is not None:
            payload["delay_before_return_html"] = self.delay_before_return_html
        return payload

    async def _poll(
        self,
        client: httpx.AsyncClient,
        task_id: str,
        *,
        extractor,
        content_label: str,
    ) -> str:
        headers = self._build_headers()
        start = time.monotonic()
        working_path: str | None = None

        while True:
            elapsed = time.monotonic() - start
            if elapsed >= self.poll_timeout:
                raise RuntimeError(
                    f"crawl4ai: poll timeout after {elapsed:.1f}s for task {task_id!r}"
                )

            paths_to_try = [working_path] if working_path else self.poll_paths
            found_working_path = False
            saw_retryable_error = False

            for path_template in paths_to_try:
                poll_url = self.base_url + path_template.replace("{task_id}", task_id)
                _guard_poll_url(poll_url)
                try:
                    response = await client.get(poll_url, headers=headers)
                except httpx.HTTPError as exc:
                    log.bind(
                        fetcher="crawl4ai",
                        task_id=task_id,
                        exception_type=type(exc).__name__,
                    ).debug("crawl4ai.poll_request_error")
                    saw_retryable_error = True
                    continue

                if response.status_code == 404:
                    continue
                if response.status_code >= 500:
                    log.bind(
                        fetcher="crawl4ai",
                        task_id=task_id,
                        status_code=response.status_code,
                    ).debug("crawl4ai.poll_retryable_http_status")
                    working_path = path_template
                    found_working_path = True
                    saw_retryable_error = True
                    break

                response.raise_for_status()
                data = response.json()

                content = extractor(data)
                if content:
                    return content

                status = str(data.get("status", "")).lower()
                if status in _FAILURE_STATUSES:
                    raise RuntimeError(f"crawl4ai: task {task_id!r} ended with status={status!r}")

                if status in _SUCCESS_STATUSES:
                    raise RuntimeError(
                        f"crawl4ai: task {task_id!r} completed but no {content_label} in response"
                    )

                working_path = path_template
                found_working_path = True
                break

            if not found_working_path and working_path is None and not saw_retryable_error:
                raise RuntimeError(f"crawl4ai: no poll path responded for task {task_id!r}")

            await asyncio.sleep(self.poll_interval)

    async def extract(self, url: str) -> str:
        guard(url)
        headers = self._build_headers()
        payload = self._build_payload(url)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/crawl",
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            data = response.json()

            markdown = _extract_markdown(data)
            if markdown:
                return markdown

            task_id = _get_task_id(data)
            if not task_id:
                raise RuntimeError(
                    "crawl4ai: no markdown and no task id in /crawl response "
                    f"(keys: {list(data.keys())})"
                )

            log.bind(fetcher="crawl4ai", task_id=task_id).debug("crawl4ai.polling_started")
            return await self._poll(
                client,
                task_id,
                extractor=_extract_markdown,
                content_label="markdown",
            )

    async def extract_raw(self, url: str) -> str:
        guard(url)
        headers = self._build_headers()
        payload = self._build_payload(url)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/crawl",
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            data = response.json()

            raw = _extract_raw(data)
            if raw:
                return raw

            task_id = _get_task_id(data)
            if not task_id:
                raise RuntimeError(
                    "crawl4ai: no raw content and no task id in /crawl response "
                    f"(keys: {list(data.keys())})"
                )

            log.bind(fetcher="crawl4ai", task_id=task_id).debug("crawl4ai.raw_polling_started")
            return await self._poll(
                client,
                task_id,
                extractor=_extract_raw,
                content_label="raw content",
            )
