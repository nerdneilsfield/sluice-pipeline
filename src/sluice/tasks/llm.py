"""Structured LLM Tasks and the startup-only LLM boundary."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from ..contracts import NEW, BatchInput, Item, JSONValue, LLMClient
from ..errors import ExecutionError, InvalidStructuredOutput


def _copy_json(value: object) -> object:
    """Thaw immutable Item payload containers before handing them to user code."""

    if isinstance(value, Mapping):
        return {str(key): _copy_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_copy_json(item) for item in value]
    return value


def _config_mapping(config: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(config, Mapping):
        raise ExecutionError("LLM Task configuration must be a mapping")
    return config


def _required_model(config: Mapping[str, Any]) -> str:
    model = config.get("model")
    if not isinstance(model, str) or not model:
        raise ExecutionError("LLM Task model must be a non-empty string")
    return model


def _read_verified_file(config: Mapping[str, Any], key: str) -> str:
    path_value = config.get(key)
    if not isinstance(path_value, str) or not path_value:
        raise ExecutionError(f"LLM Task {key} must be a non-empty file path")
    path = Path(path_value)
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise ExecutionError(f"LLM Task {key} file is unavailable: {path}") from error
    expected = config.get(f"{key}_digest")
    if expected is not None:
        if not isinstance(expected, str) or not expected:
            raise ExecutionError(f"LLM Task {key}_digest must be a non-empty string")
        actual = hashlib.sha256(raw).hexdigest()
        if actual != expected:
            raise ExecutionError(f"LLM Task {key} file changed or is missing: {path}")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ExecutionError(f"LLM Task {key} file is not UTF-8: {path}") from error


def _prompt(config: Mapping[str, Any]) -> str:
    return _read_verified_file(config, "prompt")


def _schema_from_file(config: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    text = _read_verified_file(config, key)
    try:
        value = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ExecutionError(f"LLM Task {key} file is not valid JSON") from error
    if not isinstance(value, Mapping):
        raise ExecutionError(f"LLM Task {key} schema must be an object")
    return value


def _schema(config: Mapping[str, Any]) -> Mapping[str, Any] | None:
    # ``output`` is the initial inline shape.  ``schema`` and
    # ``output_schema`` are retained for the loader's file-digest convention.
    if "output" in config:
        value = config["output"]
        if not isinstance(value, Mapping):
            raise ExecutionError("LLM Task output schema must be an object")
        return value
    for key in ("schema", "output_schema"):
        if key not in config:
            continue
        value = config[key]
        if isinstance(value, Mapping):
            return value
        if isinstance(value, str) and value:
            return _schema_from_file(config, key)
        raise ExecutionError(f"LLM Task {key} schema must be an object or file path")
    return None


def _invalid(message: str) -> InvalidStructuredOutput:
    return InvalidStructuredOutput(message)


def _validate_shape(value: object, shape: object, path: str) -> None:
    if shape == "string":
        if not isinstance(value, str):
            raise _invalid(f"structured output {path} must be a string")
        return
    if shape == "list[string]":
        if not isinstance(value, (list, tuple)) or any(
            not isinstance(item, str) for item in value
        ):
            raise _invalid(f"structured output {path} must be a list of strings")
        return
    if shape == "object":
        if not isinstance(value, Mapping):
            raise _invalid(f"structured output {path} must be an object")
        return
    if isinstance(shape, Mapping):
        if not isinstance(value, Mapping):
            raise _invalid(f"structured output {path} must be an object")
        for key, child_shape in shape.items():
            if not isinstance(key, str):
                raise _invalid(
                    f"structured output schema key at {path} must be a string"
                )
            if key not in value:
                raise _invalid(f"structured output is missing {path}.{key}")
            _validate_shape(value[key], child_shape, f"{path}.{key}")
        return
    raise _invalid(f"unsupported structured output shape at {path}")


def validate_structured(value: JSONValue, schema: Mapping[str, JSONValue]) -> JSONValue:
    """Validate the small declarative shape used by structured LLM Tasks."""

    if not isinstance(schema, Mapping):
        raise InvalidStructuredOutput("structured output schema must be an object")
    if not isinstance(value, Mapping):
        raise InvalidStructuredOutput("structured output must be an object")
    for key, shape in schema.items():
        if not isinstance(key, str):
            raise InvalidStructuredOutput(
                "structured output schema keys must be strings"
            )
        if key not in value:
            raise InvalidStructuredOutput(f"structured output is missing {key}")
        _validate_shape(value[key], shape, key)
    return value


def _extract_payload(input: object) -> JSONValue:
    if not isinstance(input, Item):
        raise ExecutionError("llm_extract input must be an Item")
    payload = _copy_json(input.payload)
    return payload  # type: ignore[return-value]


def _summary_payload(input: object) -> list[JSONValue]:
    if isinstance(input, BatchInput):
        values = input.items
    elif isinstance(input, Sequence) and not isinstance(input, (str, bytes, bytearray)):
        values = input
    else:
        raise ExecutionError("llm_summary input must be BatchInput or an Item sequence")
    if any(not isinstance(item, Item) for item in values):
        raise ExecutionError("llm_summary input must contain Item values")
    return [_copy_json(item.payload) for item in values]  # type: ignore[return-value]


def _complete(
    client: LLMClient | None,
    config: Mapping[str, Any],
    payload: JSONValue,
) -> JSONValue:
    if client is None:
        raise ExecutionError("LLM Task has no startup LLM adapter")
    model = _required_model(config)
    prompt = _prompt(config)
    complete = getattr(client, "complete", None)
    if not callable(complete):
        raise ExecutionError("LLM startup adapter does not expose complete")
    try:
        value = complete(model, prompt, payload)
    except ExecutionError:
        raise
    except BaseException as error:
        raise ExecutionError("LLM completion failed") from error
    return value


def llm_extract(config: Mapping[str, JSONValue], input: Item) -> NEW:
    """Unbound convenience implementation; startup resolution supplies a client."""

    return _LLMTask(None, "each", False).run(config, input)  # type: ignore[return-value]


def llm_summary(
    config: Mapping[str, JSONValue], input: BatchInput | Sequence[Item]
) -> tuple[NEW, ...]:
    """Unbound convenience implementation; startup resolution supplies a client."""

    return _LLMTask(None, "batch", True).run(config, input)  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class _LLMTask:
    llm_client: LLMClient | None
    input_mode: Literal["each", "batch"]
    summary: bool

    def run(self, config: Mapping[str, JSONValue], input: object) -> object:
        config = _config_mapping(config)
        payload = _summary_payload(input) if self.summary else _extract_payload(input)
        value = _complete(self.llm_client, config, payload)  # type: ignore[arg-type]
        schema = _schema(config)
        if schema is not None:
            value = validate_structured(value, schema)  # type: ignore[arg-type]
        if self.summary:
            return (NEW(value),)
        return NEW(value)

    def __call__(self, config: Mapping[str, JSONValue], input: object) -> object:
        return self.run(config, input)


def bind_llm(
    llm_client: LLMClient | None,
    *,
    input_mode: Literal["each", "batch"] = "each",
    summary: bool = False,
) -> _LLMTask:
    """Capture one startup LLM adapter in an immutable Task wrapper."""

    if input_mode not in {"each", "batch"}:
        raise ValueError("LLM Task input mode must be each or batch")
    return _LLMTask(llm_client, input_mode, summary)


def bind_extract(adapters: object) -> _LLMTask:
    return bind_llm(getattr(adapters, "llm_client", None), input_mode="each")


def bind_summary(adapters: object) -> _LLMTask:
    return bind_llm(
        getattr(adapters, "llm_client", None), input_mode="batch", summary=True
    )


__all__ = [
    "InvalidStructuredOutput",
    "bind_extract",
    "bind_llm",
    "bind_summary",
    "llm_extract",
    "llm_summary",
    "validate_structured",
]
