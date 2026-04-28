from prefect import flow
from pathlib import Path
from sluice.loader import load_all
from sluice.runner import run_pipeline


def build_flow(pipeline_id: str):
    @flow(name=f"sluice-{pipeline_id}")
    async def _flow(config_dir: str):
        bundle = load_all(Path(config_dir))
        return await run_pipeline(bundle, pipeline_id=pipeline_id)
    return _flow
