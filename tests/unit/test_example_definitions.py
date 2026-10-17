"""Bundled examples must stay loadable."""

from pathlib import Path

from sluice.config import load_definition

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"


def test_hn_to_files_example_loads():
    definition = load_definition(EXAMPLES / "hn-to-files.yml")
    assert definition.sources["hn"].use == "sluice.sources.hacker_news"
    assert definition.file_root == "hn-files"
    deliver = definition.pipelines["hn_files"].nodes["files"].deliver
    assert deliver.use == "sluice.sinks.file"
