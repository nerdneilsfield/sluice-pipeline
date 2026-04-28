from typing import Literal
from pydantic import BaseModel, Field


class KeyConfig(BaseModel):
    value: str
    weight: int = 1
    quota_duration: int | None = None
    reset_time: str | None = None
    quota_error_tokens: list[str] = Field(default_factory=list)


class BaseEndpoint(BaseModel):
    url: str
    weight: int = 1
    key: list[KeyConfig]
    extra_headers: dict[str, str] = Field(default_factory=dict)
    active_windows: list[str] = Field(default_factory=list)
    active_timezone: str | None = None


class ModelEntry(BaseModel):
    model_name: str
    is_stream: bool = True
    is_support_json_schema: bool = False
    is_support_json_object: bool = False
    input_price_per_1k: float = 0.0
    output_price_per_1k: float = 0.0


class Provider(BaseModel):
    name: str
    type: Literal["openai_compatible"]
    base: list[BaseEndpoint]
    models: list[ModelEntry]
    extra_headers: dict[str, str] = Field(default_factory=dict)


class ProvidersConfig(BaseModel):
    providers: list[Provider]
