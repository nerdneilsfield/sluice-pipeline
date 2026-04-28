import tomllib, pytest
from sluice.config import PipelineConfig

PIPE = """
id = "ai_news"
cron = "0 8 * * *"
window = "24h"

[[sources]]
type = "rss"
url  = "https://example.com/feed"
tag  = "ai"

[[stages]]
name = "dedupe"
type = "dedupe"

[[stages]]
name = "fetch"
type = "fetcher_apply"
write_field = "fulltext"

[[stages]]
name = "summarize"
type = "llm_stage"
mode = "per_item"
input_field = "fulltext"
output_field = "summary"
prompt_file = "prompts/summarize_zh.md"
model = "glm/glm-4-flash"

[[stages]]
name = "drop_short"
type = "filter"
mode = "keep_if_all"
rules = [{ field = "summary", op = "min_length", value = 50 }]

[[stages]]
name = "render"
type = "render"
template = "templates/daily.md.j2"
output_field = "context.markdown"

[[sinks]]
id = "local"
type = "file_md"
input = "context.markdown"
path = "./out/{run_date}.md"

[[sinks]]
id = "notion_main"
type = "notion"
input = "context.markdown"
parent_id = "env:N"
parent_type = "database"
title_template = "X · {run_date}"
mode = "upsert"
"""


def test_parse_pipeline():
    cfg = PipelineConfig.model_validate(tomllib.loads(PIPE))
    assert cfg.id == "ai_news"
    assert cfg.window == "24h"
    assert cfg.stages[0].type == "dedupe"
    assert cfg.stages[2].type == "llm_stage"
    assert cfg.stages[2].model == "glm/glm-4-flash"
    assert cfg.sinks[0].id == "local"


def test_sink_id_required():
    bad = PIPE.replace('id = "local"\n', "")
    with pytest.raises(Exception):
        PipelineConfig.model_validate(tomllib.loads(bad))


def test_unknown_stage_type_rejected():
    bad = PIPE.replace('type = "dedupe"', 'type = "made_up"')
    with pytest.raises(Exception):
        PipelineConfig.model_validate(tomllib.loads(bad))


def test_filter_op_validated():
    bad = PIPE.replace('"min_length"', '"weird_op"')
    with pytest.raises(Exception):
        PipelineConfig.model_validate(tomllib.loads(bad))
