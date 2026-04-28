from pathlib import Path

from prefect import flow

from sluice.loader import load_all
from sluice.runner import run_pipeline


def build_flow(pipeline_id: str):
    @flow(name=f"sluice-{pipeline_id}")
    async def _flow(config_dir: str):
        bundle = load_all(Path(config_dir))
        result = await run_pipeline(bundle, pipeline_id=pipeline_id)
        if result.status != "success":
            raise RuntimeError(f"pipeline {pipeline_id} failed: {result.error}")
        return result

    return _flow
