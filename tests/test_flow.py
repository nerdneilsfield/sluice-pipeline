import pytest
from unittest.mock import AsyncMock, patch
from sluice.flow import build_flow


@pytest.mark.asyncio
async def test_flow_calls_runner():
    fake_run = AsyncMock(return_value=type("R", (), {"status": "success", "items_out": 5})())
    with (
        patch("sluice.flow.run_pipeline", fake_run),
        patch("sluice.flow.load_all", return_value=None),
    ):
        flow = build_flow("ai_news")
        result = await flow.fn(config_dir="/tmp/x")
    fake_run.assert_called_once()
