import asyncio
import time
from collections.abc import Mapping, Sequence
from typing import Any

import httpx

from sluice.fetchers._ssrf import guard
from sluice.fetchers.base import register_fetcher
from sluice.logging_setup import get_logger

log = get_logger(__name__)

_SUCCESS_STATUSES = {"completed", "success", "done"}
_FAILURE_STATUSES = {"failed", "error", "cancelled"}
_DEFAULT_POLL_PATHS = [
    "/crawl/job/{task_id}",
    "/task/{task_id}",
    "/job/{task_id}",
]


def _has_auth_header(headers: Mapping[str, Any]) -> bool:
    return any(key.lower() == "authorization" for key in headers)


def _markdown_value(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, Mapping):
        for key in ("raw_markdown", "fit_markdown", "markdown"):
            nested_value = value.get(key)
            if isinstance(nested_value, str) and nested_value:
                return nested_value
    return None


def _extract_markdown(data: dict) -> str | None:
    """Return the first non-empty markdown string from known Crawl4AI shapes."""
    candidates: list[Any] = []
    results = data.get("results")
    if isinstance(results, Sequence) and not isinstance(results, (str, bytes)) and results:
        candidates.append(results[0])

    for key in ("result", "data"):
        value = data.get(key)
        if isinstance(value, Mapping):
            candidates.append(value)

    candidates.append(data)

    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            continue
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


def _get_task_id(data: dict) -> str | None:
    for key in ("task_id", "job_id", "id"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return None


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
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.api_headers = dict(api_headers) if api_headers else {}
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.poll_timeout = poll_timeout if poll_timeout is not None else timeout
        self.poll_paths = list(poll_paths) if poll_paths else list(_DEFAULT_POLL_PATHS)

    def _build_headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        headers.update(self.api_headers)
        if self.api_key and not _has_auth_header(headers):
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def _poll(self, client: httpx.AsyncClient, task_id: str) -> str:
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

            for path_template in paths_to_try:
                poll_url = self.base_url + path_template.replace("{task_id}", task_id)
                try:
                    response = await client.get(poll_url, headers=headers)
                except httpx.HTTPError as exc:
                    log.bind(
                        fetcher="crawl4ai",
                        task_id=task_id,
                        exception_type=type(exc).__name__,
                    ).debug("crawl4ai.poll_request_error")
                    continue

                if response.status_code == 404:
                    continue

                response.raise_for_status()
                data = response.json()

                markdown = _extract_markdown(data)
                if markdown:
                    return markdown

                status = str(data.get("status", "")).lower()
                if status in _FAILURE_STATUSES:
                    raise RuntimeError(f"crawl4ai: task {task_id!r} ended with status={status!r}")

                if status in _SUCCESS_STATUSES:
                    raise RuntimeError(
                        f"crawl4ai: task {task_id!r} completed but no markdown in response"
                    )

                working_path = path_template
                found_working_path = True
                break

            if not found_working_path and working_path is None:
                raise RuntimeError(f"crawl4ai: no poll path responded for task {task_id!r}")

            await asyncio.sleep(self.poll_interval)

    async def extract(self, url: str) -> str:
        guard(url)
        headers = self._build_headers()

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/crawl/job",
                headers=headers,
                json={"urls": [url]},
            )
            if response.status_code == 404:
                response = await client.post(
                    f"{self.base_url}/crawl",
                    headers=headers,
                    json={"urls": [url]},
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
            return await self._poll(client, task_id)
