import pytest

from sluice.core.item import Item
from sluice.processors.enrich import EnrichProcessor
from tests.conftest import make_ctx


class _PassEnricher:
    name = "pass"

    async def enrich(self, item):
        return f"E({item.url})"


class _SkipEnricher:
    name = "skip"

    async def enrich(self, item):
        return None


class _FailEnricher:
    name = "fail"

    async def enrich(self, item):
        raise RuntimeError("nope")


def _it(url):
    return Item(
        source_id="s",
        pipeline_id="p",
        guid="g",
        url=url,
        title="t",
        published_at=None,
        raw_summary=None,
    )


@pytest.mark.asyncio
async def test_enrich_writes_output_field():
    p = EnrichProcessor(
        name="x",
        enricher=_PassEnricher(),
        output_field="extras.notes",
        on_failure="skip",
        max_chars=1000,
        concurrency=2,
    )
    out = await p.process(make_ctx(items=[_it("u1")]))
    assert out.items[0].extras["notes"] == "E(u1)"


@pytest.mark.asyncio
async def test_enrich_skip_on_none():
    p = EnrichProcessor(
        name="x",
        enricher=_SkipEnricher(),
        output_field="extras.notes",
        on_failure="skip",
        max_chars=1000,
        concurrency=2,
    )
    out = await p.process(make_ctx(items=[_it("u1")]))
    assert "notes" not in out.items[0].extras


@pytest.mark.asyncio
async def test_enrich_failure_skip():
    class _F:
        def __init__(self):
            self.rows = []

        async def record(
            self, pipeline_id, item_key, item, *, stage, error_class, error_msg, max_retries
        ):
            self.rows.append(
                {"pipeline_id": pipeline_id, "stage": stage, "error_class": error_class}
            )

    f = _F()
    p = EnrichProcessor(
        name="x",
        enricher=_FailEnricher(),
        output_field="extras.notes",
        on_failure="skip",
        max_chars=1000,
        concurrency=2,
        failures=f,
        max_retries=3,
    )
    out = await p.process(make_ctx(items=[_it("u1")]))
    assert len(out.items) == 1
    assert f.rows and f.rows[0]["stage"] == "enrich:fail"


@pytest.mark.asyncio
async def test_enrich_failure_record_error_does_not_crash_skip_mode():
    class _BadFailures:
        async def record(self, *args, **kwargs):
            raise RuntimeError("db locked")

    p = EnrichProcessor(
        name="x",
        enricher=_FailEnricher(),
        output_field="extras.notes",
        on_failure="skip",
        max_chars=1000,
        concurrency=2,
        failures=_BadFailures(),
        max_retries=3,
    )
    out = await p.process(make_ctx(items=[_it("u1")]))
    assert len(out.items) == 1


@pytest.mark.asyncio
async def test_enrich_max_chars_truncation():
    class _Long:
        name = "long"

        async def enrich(self, item):
            return "x" * 100

    p = EnrichProcessor(
        name="x",
        enricher=_Long(),
        output_field="extras.notes",
        on_failure="skip",
        max_chars=10,
        concurrency=1,
    )
    out = await p.process(make_ctx(items=[_it("u1")]))
    assert len(out.items[0].extras["notes"]) == 10
