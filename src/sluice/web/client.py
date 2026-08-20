"""Startup-bound WebContentClient shell.

The ordered fallback coordinator is intentionally completed by Task 7.  This
shell owns no definition-specific chain state: every call receives the
expanded provider/extractor tuples for that invocation.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from ..errors import ExecutionError
from .contracts import (
    DocumentArtifact,
    ExtractorBinding,
    FetchRequest,
    PrivateWebInvocation,
    ProviderBinding,
)


class WebContentClient:
    def __init__(self, catalog: object | None = None) -> None:
        self.catalog = catalog
        self._closed = False

    def resolve(
        self,
        request: FetchRequest,
        fetch_chain: Iterable[ProviderBinding],
        extract_chain: Iterable[ExtractorBinding] | None,
    ) -> DocumentArtifact:
        """Execute one provider operation using the call's exact chain.

        Task 7 replaces this single-candidate shell with the full sequential
        fallback algorithm.  Keeping the first operation useful makes startup
        binding testable without creating a hidden chain cache.
        """

        if self._closed:
            raise ExecutionError("web content client is closed")
        providers = tuple(fetch_chain)
        if not providers:
            raise ExecutionError("web fetch chain is empty")
        catalog = self.catalog
        for binding in providers:
            try:
                provider = catalog.provider(binding) if catalog is not None else None
                if provider is None:
                    raise ExecutionError("web provider catalog is unavailable")
                artifact = provider.fetch(request)
                if not isinstance(artifact, DocumentArtifact):
                    raise ExecutionError("web provider returned an invalid artifact")
                return artifact
            except ExecutionError:
                raise
            except BaseException as error:
                raise ExecutionError("web provider operation failed") from error
        raise ExecutionError("web provider chain exhausted")

    def close(self) -> None:
        self._closed = True

    def run_web(self, invocation: PrivateWebInvocation, input: object) -> object:
        """Private built-in Task boundary.

        Task-specific artifact projection is layered on top of this startup
        client by the content-task slice.  Keeping the method here is
        important even before that coordinator is installed: a configured
        web Task must fail at the private boundary rather than silently
        falling back to the legacy ``http_client`` Task implementation.
        """

        del input
        if self._closed:
            raise ExecutionError("web content client is closed")
        if not isinstance(invocation, PrivateWebInvocation):
            raise ExecutionError("invalid private web invocation")
        raise ExecutionError("web task coordinator is not configured")

    def collect_web(
        self, invocation: PrivateWebInvocation, checkpoint: object
    ) -> object:
        """Private built-in Source boundary; see :meth:`run_web`."""

        del checkpoint
        if self._closed:
            raise ExecutionError("web content client is closed")
        if not isinstance(invocation, PrivateWebInvocation):
            raise ExecutionError("invalid private web invocation")
        raise ExecutionError("web source coordinator is not configured")


__all__ = ["WebContentClient"]
