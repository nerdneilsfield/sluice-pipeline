from sluice.core.item import Item
from sluice.core.protocols import Enricher


class _DummyEnricher:
    name = "dummy"

    async def enrich(self, item: Item) -> str | None:
        return "x"


def test_enricher_protocol_match():
    e: Enricher = _DummyEnricher()
    assert e.name == "dummy"
