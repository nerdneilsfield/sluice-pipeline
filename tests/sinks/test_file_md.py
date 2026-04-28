import pytest
from pathlib import Path
from sluice.context import PipelineContext
from sluice.sinks.file_md import FileMdSink
from sluice.state.db import open_db
from sluice.state.emissions import EmissionStore

@pytest.mark.asyncio
async def test_writes_file(tmp_path):
    out = tmp_path / "out" / "{run_date}.md"
    s = FileMdSink(id="local", input="context.markdown", path=str(out))
    ctx = PipelineContext("p","p/2026-04-28","2026-04-28",
                          [object()], {"markdown": "# hi"})
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        res = await s.emit(ctx, emissions=e)
    written = tmp_path / "out" / "2026-04-28.md"
    assert written.read_text() == "# hi"
    assert res.external_id == str(written)

@pytest.mark.asyncio
async def test_overwrite_on_retry(tmp_path):
    out = tmp_path / "{run_date}.md"
    s = FileMdSink(id="local", input="context.markdown", path=str(out),
                   mode="upsert")
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        await s.emit(PipelineContext("p","p/r","r",[1],{"markdown":"v1"}),
                     emissions=e)
        await s.emit(PipelineContext("p","p/r","r",[1],{"markdown":"v2"}),
                     emissions=e)
    assert (tmp_path / "r.md").read_text() == "v2"

@pytest.mark.asyncio
async def test_path_traversal_rejected(tmp_path):
    out = tmp_path / "out" / "{run_date}.md"
    s = FileMdSink(id="local", input="context.markdown", path=str(out))
    ctx = PipelineContext("../../../etc/passwd","p/2026-04-28","2026-04-28",
                          [object()], {"markdown": "# hi"})
    async with open_db(tmp_path / "d.db") as db:
        e = EmissionStore(db)
        res = await s.emit(ctx, emissions=e)
    written = Path(res.external_id)
    assert str(written).startswith(str(tmp_path / "out"))
    assert ".." not in written.name
