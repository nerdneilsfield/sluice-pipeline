"""Retained web snapshot decoding and private invocation boundaries."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

from sluice.canonical import canonical_json
from sluice.config import decode_stage_snapshot
from sluice.contracts import FORWARD, Item, StageDef, TaskDef
from sluice.errors import ExecutionError
from sluice.resolution import private_web_invocation
from sluice.sources.webpage import bind as bind_webpage_source
from sluice.stage_runtime import run_stage_tasks
from sluice.tasks.content import fetch_webpage
from sluice.web.client import WebContentClient
from sluice.web.contracts import (
    DocumentArtifact,
    ExtractorBinding,
    FetchRequest,
    PrivateWebInvocation,
    ProviderBinding,
    QualityPolicy,
    WebBinding,
)
from sluice.web.registry import ProviderLimiter


def _stage(tasks: list[dict[str, object]]) -> dict[str, object]:
    return {
        "name": "article",
        "input": "each",
        "cache_version": 1,
        "tasks": tasks,
        "output_schema": None,
    }


def test_limiter_rechecks_cooldown_after_a_waiting_permit_arrives() -> None:
    limiter = ProviderLimiter(concurrency=1, cooldown_seconds=30)

    class Semaphore:
        release_calls = 0

        def acquire(self, timeout: float | None = None) -> bool:
            del timeout
            limiter.cooldown()
            return True

        def release(self) -> None:
            self.release_calls += 1

    semaphore = Semaphore()
    limiter._semaphore = semaphore  # type: ignore[assignment]

    assert limiter.acquire(timeout=1) is False
    assert semaphore.release_calls == 1


def test_snapshot_decoder_preserves_canonical_bytes_for_both_task_shapes():
    binding = WebBinding(output="markdown")
    legacy = canonical_json(
        _stage(
            [
                {
                    "use": "sluice.tasks.fetch_webpage",
                    "with": {"url_field": "url"},
                }
            ]
        )
    )
    current = canonical_json(
        _stage(
            [
                {
                    "use": "sluice.tasks.to_markdown",
                    "with": {},
                    "web_binding": binding.semantic_projection(),
                }
            ]
        )
    )

    legacy_stage = decode_stage_snapshot(json.loads(legacy))
    current_stage = decode_stage_snapshot(json.loads(current))

    assert legacy_stage.tasks[0].web_binding is None
    assert legacy_stage.canonical_config_json == legacy
    assert current_stage.tasks[0].web_binding is not None
    assert current_stage.canonical_config_json == current


def _current_web_snapshot(
    projection: dict[str, object], use: str = "sluice.tasks.to_markdown"
) -> dict[str, object]:
    return _stage(
        [
            {
                "use": use,
                "with": {},
                "web_binding": projection,
            }
        ]
    )


def _projection(binding: WebBinding | None = None) -> dict[str, object]:
    value = binding or WebBinding(output="markdown")
    return json.loads(canonical_json(value.semantic_projection()))


def _provider_projection() -> dict[str, object]:
    return _projection(
        WebBinding(
            fetch_chain=(
                ProviderBinding(
                    name="provider",
                    type="httpx",
                    timeout=timedelta(seconds=30),
                    connect_timeout=timedelta(seconds=10),
                ),
            ),
            fetch_chain_name="chain",
            extract_chain=(ExtractorBinding(name="identity", type="identity"),),
            extract_chain_name="extract",
        )
    )


def test_snapshot_decoder_rejects_missing_web_binding_projection_field():
    projection = _projection()
    projection.pop("output")

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_missing_web_binding_quality_field():
    projection = _projection()
    quality = projection["quality"]
    assert isinstance(quality, dict)
    quality.pop("min_chars")

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_missing_web_binding_chain_field():
    binding = WebBinding(
        fetch_chain=(ProviderBinding(name="provider", type="httpx"),),
        fetch_chain_name="chain",
    )
    projection = _projection(binding)
    fetch_chain = projection["fetch_chain"]
    assert isinstance(fetch_chain, dict)
    fetch_chain.pop("providers")

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_missing_web_binding_provider_field():
    binding = WebBinding(
        fetch_chain=(ProviderBinding(name="provider", type="httpx"),),
        fetch_chain_name="chain",
    )
    projection = _projection(binding)
    fetch_chain = projection["fetch_chain"]
    assert isinstance(fetch_chain, dict)
    providers = fetch_chain["providers"]
    assert isinstance(providers, list)
    provider = providers[0]
    assert isinstance(provider, dict)
    provider.pop("type")

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_unknown_web_binding_projection_field():
    projection = _projection()
    projection["unexpected"] = True

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_disabled_web_binding_challenge_safety():
    projection = _projection()
    quality = projection["quality"]
    assert isinstance(quality, dict)
    quality["reject_challenges"] = False

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_web_binding_on_custom_task():
    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(_projection(), use="custom.task"))


@pytest.mark.parametrize(
    "use",
    [
        "sluice.tasks.fetch_webpage",
        "sluice.tasks.extract_content",
        "sluice.tasks.to_markdown",
        "sluice.tasks.to_text",
    ],
)
def test_snapshot_decoder_rejects_explicit_null_binding_for_web_task(use):
    snapshot = _stage(
        [
            {
                "use": use,
                "with": {},
                "web_binding": None,
            }
        ]
    )

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(snapshot)


def test_snapshot_decoder_rejects_source_only_cache_version_on_stage_task():
    projection = _projection()
    projection["web_cache_version"] = 1

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


@pytest.mark.parametrize(
    "use",
    ["sluice.tasks.fetch_webpage", "sluice.tasks.extract_content"],
)
def test_snapshot_decoder_rejects_empty_web_task_chain(use):
    projection = _projection(
        WebBinding(
            fetch_chain=() if use.endswith("fetch_webpage") else None,
            extract_chain=() if use.endswith("extract_content") else None,
            fetch_chain_name="chain" if use.endswith("fetch_webpage") else None,
            extract_chain_name="chain" if use.endswith("extract_content") else None,
        )
    )

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection, use=use))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timeout", None),
        ("connect_timeout", None),
        ("timeout", 30.5),
        ("connect_timeout", 10.5),
        ("timeout_seconds", 31),
    ],
)
def test_snapshot_decoder_rejects_invalid_retained_provider_timing(field, value):
    projection = _provider_projection()
    fetch_chain = projection["fetch_chain"]
    assert isinstance(fetch_chain, dict)
    providers = fetch_chain["providers"]
    assert isinstance(providers, list)
    provider = providers[0]
    assert isinstance(provider, dict)
    provider[field] = value

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_snapshot_decoder_rejects_provider_connection_timeout_longer_than_timeout():
    projection = _provider_projection()
    fetch_chain = projection["fetch_chain"]
    assert isinstance(fetch_chain, dict)
    providers = fetch_chain["providers"]
    assert isinstance(providers, list)
    provider = providers[0]
    assert isinstance(provider, dict)
    provider["connect_timeout"] = 31

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


@pytest.mark.parametrize(
    ("chain", "field"),
    [("fetch_chain", "name"), ("extract_chain", "name")],
)
def test_snapshot_decoder_rejects_missing_retained_chain_name(chain, field):
    projection = _provider_projection()
    chain_value = projection[chain]
    assert isinstance(chain_value, dict)
    chain_value[field] = ""
    use = "sluice.tasks.fetch_webpage"

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection, use=use))


@pytest.mark.parametrize("chain", ["fetch_chain", "extract_chain"])
def test_snapshot_decoder_rejects_duplicate_retained_chain_members(chain):
    projection = _provider_projection()
    chain_value = projection[chain]
    assert isinstance(chain_value, dict)
    members = chain_value["providers" if chain == "fetch_chain" else "extractors"]
    assert isinstance(members, list)
    members.append(dict(members[0]))
    use = "sluice.tasks.fetch_webpage"

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection, use=use))


def test_snapshot_decoder_normalizes_whole_float_timeout_seconds():
    projection = _provider_projection()
    fetch_chain = projection["fetch_chain"]
    assert isinstance(fetch_chain, dict)
    providers = fetch_chain["providers"]
    assert isinstance(providers, list)
    provider = providers[0]
    assert isinstance(provider, dict)
    provider["timeout_seconds"] = 30.0

    stage = decode_stage_snapshot(
        _current_web_snapshot(projection, use="sluice.tasks.fetch_webpage")
    )
    timeout_seconds = stage.tasks[0].web_binding.providers[0].timeout_seconds  # type: ignore[union-attr]
    assert timeout_seconds == 30
    assert isinstance(timeout_seconds, int)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("base_url", "http://["),
        ("base_url", "https://user:secret@example.test"),
        ("proxy", "http://["),
        ("proxy", "http://user:secret@example.test"),
    ],
)
def test_snapshot_decoder_rejects_malformed_provider_urls(field, value):
    projection = _provider_projection()
    fetch_chain = projection["fetch_chain"]
    assert isinstance(fetch_chain, dict)
    providers = fetch_chain["providers"]
    assert isinstance(providers, list)
    provider = providers[0]
    assert isinstance(provider, dict)
    provider[field] = value

    with pytest.raises(ExecutionError):
        decode_stage_snapshot(_current_web_snapshot(projection))


def test_private_invocation_is_impossible_for_a_custom_task():
    custom_task_def = TaskDef(use="custom.task", with_config={"value": 1})
    assert private_web_invocation(custom_task_def) is None


def test_private_invocation_is_typed_and_frozen_for_a_web_builtin():
    binding = WebBinding(output="markdown")
    task_def = TaskDef(
        use="sluice.tasks.to_markdown",
        with_config={"input_format": "auto"},
        web_binding=binding,
    )

    invocation = private_web_invocation(task_def)

    assert isinstance(invocation, PrivateWebInvocation)
    assert invocation.web_binding is binding
    assert invocation.with_config["input_format"] == "auto"
    assert not isinstance(invocation, dict)


def test_default_web_client_does_not_fall_back_to_legacy_http_task() -> None:
    class LegacyHTTP:
        calls = 0

        def get(self, url: str) -> str:
            self.calls += 1
            raise AssertionError("legacy HTTP task path was used")

    legacy = LegacyHTTP()
    task = fetch_webpage.bind(type("Adapters", (), {"http_client": legacy})())
    stage = StageDef(
        name="article",
        input_mode="each",
        tasks=(
            TaskDef(
                use="sluice.tasks.fetch_webpage",
                with_config={"url_field": "url"},
                web_binding=WebBinding(output="artifact"),
            ),
        ),
        output_schema=None,
        cache_version=1,
        canonical_config_json="{}",
    )
    item = Item(
        id="item",
        producer_scope=("source", "feed"),
        logical_id="item",
        payload={"url": "https://example.test/article"},
        metadata={},
        content_hash="hash",
        observed_at="2026-08-20T00:00:00Z",
        produced_by=1,
    )

    with pytest.raises(ExecutionError):
        run_stage_tasks(
            stage,
            (item,),
            (),
            (task,),
            web_content_client=WebContentClient(),
        )
    assert legacy.calls == 0


def test_default_web_client_does_not_fall_back_to_legacy_http_source() -> None:
    class LegacyHTTP:
        calls = 0

        def get(self, url: str) -> str:
            self.calls += 1
            raise AssertionError("legacy HTTP source path was used")

    legacy = LegacyHTTP()
    source = type(
        "Source",
        (),
        {
            "use": "sluice.sources.webpage",
            "with_config": {"url": "https://example.test/page"},
            "web_binding": WebBinding(output="text"),
        },
    )()
    contract = bind_webpage_source(legacy)
    app = type("App", (), {"web_content_client": WebContentClient()})()

    from sluice.source_runtime import _collect_contract

    with pytest.raises(ExecutionError):
        _collect_contract(app, source, contract, None)
    assert legacy.calls == 0


def test_private_web_task_receives_expanded_binding_while_custom_task_gets_mapping() -> (
    None
):
    binding = WebBinding(output="artifact")
    item = Item(
        id="item",
        producer_scope=("source", "feed"),
        logical_id="item",
        payload={"url": "https://example.test/article"},
        metadata={},
        content_hash="hash",
        observed_at="2026-08-20T00:00:00Z",
        produced_by=1,
    )

    class WebSpy:
        def __init__(self) -> None:
            self.invocations: list[PrivateWebInvocation] = []

        def run_web(self, invocation: PrivateWebInvocation, input: object) -> object:
            self.invocations.append(invocation)
            assert input is item
            return FORWARD(item)

    web_spy = WebSpy()
    web_stage = StageDef(
        name="article",
        input_mode="each",
        tasks=(
            TaskDef(
                use="sluice.tasks.to_markdown",
                with_config={"input_format": "auto"},
                web_binding=binding,
            ),
        ),
        output_schema=None,
        cache_version=1,
        canonical_config_json="{}",
    )
    run_stage_tasks(
        web_stage,
        (item,),
        (),
        (
            type(
                "Task",
                (),
                {"input_mode": "each", "run": lambda self, config, value: None},
            )(),
        ),
        web_content_client=web_spy,
    )
    assert len(web_spy.invocations) == 1
    assert web_spy.invocations[0].web_binding is binding

    seen: list[object] = []

    class CustomTask:
        input_mode = "each"

        def run(self, config, value):
            seen.append(config)
            return FORWARD(item)

    custom_stage = StageDef(
        name="custom",
        input_mode="each",
        tasks=(TaskDef(use="custom.task", with_config={"nested": {"x": 1}}),),
        output_schema=None,
        cache_version=1,
        canonical_config_json="{}",
    )
    run_stage_tasks(
        custom_stage,
        (item,),
        (),
        (CustomTask(),),
        web_content_client=web_spy,
    )
    assert seen == [{"nested": {"x": 1}}]


def test_passing_provider_markdown_never_runs_local_extractor() -> None:
    request = FetchRequest(
        url="https://example.test/article",
        mode="article",
        acceptable=frozenset({"markdown", "raw_html", "text"}),
        preferred=("markdown", "raw_html", "text"),
        quality=QualityPolicy(min_chars=1),
    )
    binding = ProviderBinding(
        name="reader",
        type="startup_http",
        options={"trusted": True},
        capabilities=frozenset({"markdown", "raw_html", "text"}),
    )
    artifact = DocumentArtifact(
        source_url=request.url,
        final_url=request.url,
        content_type="text/markdown",
        markdown='provider <script>alert("private")</script> supplied markdown',
        selected_format="markdown",
        capabilities=frozenset({"markdown"}),
        provider_instance="reader",
        provider_type="startup_http",
    )

    class Provider:
        def fetch(self, received):
            assert received is request
            return artifact

    class ExplodingExtractor:
        def extract(self, received):
            raise AssertionError("local extractor must not run")

    class Catalog:
        def provider(self, received):
            assert received is binding
            return Provider()

        def limiter(self, received):
            class Limiter:
                def acquire(self, timeout=None):
                    return True

                def release(self):
                    pass

            return Limiter()

        def extractor(self, received):
            assert received.type == "identity"
            return ExplodingExtractor()

    result = WebContentClient(Catalog()).resolve(
        request,
        (binding,),
        (ExtractorBinding(name="identity", type="identity"),),
    )

    assert result.selected_format == "markdown"
    assert result.markdown == "provider  supplied markdown"
    assert result.extraction == "identity"
    assert result.extract_binding_digest is not None
