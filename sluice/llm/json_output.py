from typing import Any

import json_repair


def loads_llm_json(text: str) -> Any:
    """Parse JSON emitted by an LLM, repairing common malformed output first."""
    stripped = text.lstrip()
    data = json_repair.loads(text)
    if stripped.startswith("{") and not isinstance(data, dict):
        raise ValueError(f"repaired JSON root must be object, got {type(data).__name__}")
    if stripped.startswith("[") and not isinstance(data, list):
        raise ValueError(f"repaired JSON root must be array, got {type(data).__name__}")
    return data
