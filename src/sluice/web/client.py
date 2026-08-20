"""Startup-bound WebContentClient shell.

The ordered fallback coordinator is intentionally completed by Task 7.  This
shell owns no definition-specific chain state: every call receives the
expanded provider/extractor tuples for that invocation.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from ..errors import ExecutionError
from .contracts import DocumentArtifact, ExtractorBinding, FetchRequest, ProviderBinding


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


__all__ = ["WebContentClient"]
