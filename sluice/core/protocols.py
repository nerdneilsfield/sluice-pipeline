from __future__ import annotations

from typing import Protocol, AsyncIterator
from dataclasses import dataclass
from sluice.core.item import Item
from sluice.context import PipelineContext


class Source(Protocol):
    name: str

    async def fetch(self, window_start, window_end) -> AsyncIterator[Item]: ...


class Fetcher(Protocol):
    name: str

    async def extract(self, url: str) -> str: ...


class Processor(Protocol):
    name: str

    async def process(self, ctx) -> PipelineContext: ...


class Sink(Protocol):
    id: str
    type: str

    async def emit(self, ctx) -> SinkResult: ...


class LLMProvider(Protocol):
    name: str

    async def chat(self, messages: list[dict], model: str) -> str: ...


@dataclass
class SinkResult:
    sink_id: str
    sink_type: str
    external_id: str
    created: bool
