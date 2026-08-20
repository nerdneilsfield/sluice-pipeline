# Sluice v4 Web Content Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the approved configurable, safe, cache-correct web-content pipeline: named fetch and extraction chains turn URLs into deterministic Markdown or text through the existing Source and Stage runtime.

**Architecture:** YAML expands into frozen WebBindings captured by Source and Stage snapshots. A startup-owned WebContentClient holds fixed provider/extractor registries and performs sequential per-Item fallback; existing node concurrency remains the sole Item fan-out. Transient DocumentArtifacts only pass between built-in web Tasks, while projectors and Sources sanitize them into canonical JSON payloads and bounded provenance.

**Tech Stack:** Python 3.12; HTTPX; Beautiful Soup 4; defusedxml; markdown-it-py; html-to-markdown; optional curl_cffi, Trafilatura, Newspaper4k, Resiliparse; SQLite; PyYAML; pytest.

**Spec:** docs/superpowers/specs/2026-08-18-sluice-v4-web-content-pipeline-design.md

## Global Constraints

- Follow Design §§1–19 exactly. It is authoritative for content Tasks, webpage Source, and RSS fallback.
- SQLite remains the only durable store. StageResult/materialization keys remain the only content cache; add no URL TTL cache, provider queue, provider-attempt table, repository layer, event bus, plugin discovery, or migration framework.
- Provider fallback is ordered and sequential per Item. policy.concurrency is the only Item fan-out; do not add nested executors or provider races.
- At `open_app`, fully resolve current plus queued/running/waiter bindings. Validate terminal retained snapshots structurally and against fixed maps, but defer terminal secret resolution until an explicit rebuild. Custom Sources and Tasks receive only their authored with configuration.
- DocumentArtifact, raw HTML/XML, headers, cookies, screenshots, resolved secrets, and vendor response bodies are transient and never persist or enter logs/errors.
- Only an eligible exhausted each fetch_webpage chain creates an empty succeeded StageResult; it must not use the LLM invalid:drop branch.
- Direct networking enforces HTTP(S), normalized URLs, DNS/redirect/peer restrictions, at most ten redirects, and the effective decompressed body limit min(provider.max_body_bytes, 10485760). startup_http is only the explicit trusted compatibility exception.
- Legacy retained task snapshots shaped {use,with} decode with web_binding null. Legacy web work is never reinterpreted with current YAML.
- Keep .superpowers/ ignored and absent from history. Track SDD documents only in docs/superpowers/.
- Use TDD for every task: write focused red test, run it, add minimal production code, rerun it, then commit only the listed paths.
- After Tasks 3, 6, 9, and 12, perform the Design-required parallel architecture/contract/scope and SQLite/concurrency/test-topology review wave. Commit each accepted review fix separately.

## File Map

- pyproject.toml — core dependencies and optional web dependency extra.
- src/sluice/web/contracts.py — frozen WebBinding/artifact/request/attempt/cancellation contracts.
- src/sluice/web/config.py — strict top-level web decode, materialized defaults, secret references, and semantic/runtime projections.
- src/sluice/web/url.py, http.py, quality.py, sanitize.py — URL/SSRF transport, quality classification, safe Markdown, metadata bounds.
- src/sluice/web/extractors.py, providers.py, registry.py, select.py, client.py — local extractors, fixed provider adapters, runtime catalog, shared list selector, and sequential mode-aware coordinator.
- src/sluice/contracts.py, config.py, canonical.py, resolution.py, app.py — Definition fields, snapshot/key handling, startup binding, retained execution, close lifecycle.
- src/sluice/tasks/content.py and src/sluice/sources/webpage.py, rss.py — artifact-aware built-ins and source projection.
- src/sluice/source_runtime.py, stage_runtime.py, reconcile.py, cli.py, prefect_flow.py — private web invocation, deadlines, durable terminal exhaustion, close callers.
- tests/unit/test_web_contracts.py, test_web_config.py, test_web_safety.py, test_web_quality.py, test_web_providers.py, test_web_runtime.py — deterministic web units.
- tests/integration/test_web_pipeline.py, test_web_retention.py, test_web_sources.py, test_web_stage_lifecycle.py — production SQLite web behavior.
- Existing tests/integration/test_shared_materialization.py, test_source_lifecycle.py, tests/unit/test_collection_and_graph.py, test_content_extraction.py, tests/smoke/test_management.py, test_prefect_parity.py — explicit WebBinding fixture migration.

## Design Coverage

| Design section | Implementation tasks |
| --- | --- |
| §§1–3 purpose, retained behavior, principles | Tasks 1, 6–9, 10–12 |
| §4 public configuration | Task 2 |
| §5 expansion, secrets, snapshots, keys | Tasks 2–3 |
| §6 contracts and three resolution modes | Tasks 1, 3, 7, 10 |
| §7 provider semantics | Tasks 6, 8–9 |
| §8 extraction and projection | Tasks 5 and 10 |
| §9 quality policy | Task 4 |
| §10 retry/drop/recovery | Task 12 |
| §11 concurrency, deadlines, cancellation, close | Tasks 3, 9, 12 |
| §12 network and secret safety | Task 4 and Task 12 safe-error assertions |
| §13 webpage and RSS Sources | Task 11 |
| §14 startup retention and rebuild | Task 3 and Task 12 retention tests |
| §15 dependencies/tool set | Task 1 |
| §16 observability and management | Tasks 9 and 12 |
| §17 acceptance criteria | Tasks 1–12 plus Final Verification |
| §18 Git/review constraints | Global Constraints and Review Waves 1–4 |

---

### Task 1: Add dependencies and immutable web contracts

**Files:**
- Modify: pyproject.toml
- Modify: src/sluice/contracts.py
- Modify: src/sluice/errors.py
- Create: src/sluice/web/__init__.py
- Create: src/sluice/web/contracts.py
- Test: tests/unit/test_web_contracts.py

**Interfaces:**
- Produce frozen/slotted QualityPolicy, ProviderBinding, ExtractorBinding, WebBinding, PrivateWebInvocation, FetchRequest, DocumentArtifact, WebAttemptFailure, FetchChainExhausted, WebAttemptCancelled, WebStageDeadlineExpired, WebExecutionProbe, and module singleton NO_MATCH. Add WebSafetyError to errors.py.
- Extend SourceDef and TaskDef with web_binding: WebBinding | None. Keep with_config authored and immutable.
- Define `WebExecutionProbe.check() -> None` and the context-variable accessor in `sluice.web.contracts`; Task 12 owns setting and clearing it around a claimed Stage execution.

- [ ] **Step 1: Write the failing contract tests.**

~~~python
def test_artifact_is_frozen_and_no_match_is_not_none():
    artifact = DocumentArtifact(
        source_url="https://example.test/a", final_url="https://example.test/a",
        status_code=200, content_type="text/html", raw_html="<p>a</p>",
        capabilities=frozenset({"raw_html"}), provider_instance="direct",
        provider_type="httpx",
    )
    assert artifact.selected_format is None
    assert NO_MATCH is not None
    with pytest.raises((AttributeError, TypeError)):
        artifact.final_url = "https://example.test/b"


def test_request_enforces_mode_and_list_selector_shape():
    with pytest.raises(ValueError):
        FetchRequest(url="https://example.test", mode="article", acceptable=frozenset())
    with pytest.raises(ValueError):
        FetchRequest(url="https://example.test", mode="article",
                     acceptable=frozenset({"text"}), list_selector="article")
~~~

- [ ] **Step 2: Run the focused test.**

Run: python3 -m pytest -q tests/unit/test_web_contracts.py

Expected: FAIL during collection because sluice.web.contracts does not exist.

- [ ] **Step 3: Implement the minimal contracts and dependencies.**

~~~toml
[project]
dependencies = [
  "PyYAML>=6.0,<7", "httpx>=0.27,<1", "beautifulsoup4>=4.12,<5",
  "defusedxml>=0.7,<1", "markdown-it-py>=4.2,<5",
  "html-to-markdown>=3.10,<4",
]

[project.optional-dependencies]
web = ["curl_cffi>=0.16,<1", "trafilatura>=2,<3",
       "newspaper4k>=0.9,<1", "resiliparse>=0.16,<1"]
~~~

Define exactly these accepted Artifact capabilities: raw_html, raw_source, text, markdown, rendered_js. FetchRequest accepts modes article, list_html, raw_source; list_selector is required only for list_html. FetchChainExhausted stores only an ordered tuple of safe attempts, never a cause string.

- [ ] **Step 4: Verify focused and existing contract tests.**

Run: python3 -m pytest -q tests/unit/test_web_contracts.py tests/unit/test_contracts_and_keys.py

Expected: PASS.

- [ ] **Step 5: Commit.**

~~~bash
git add pyproject.toml src/sluice/contracts.py src/sluice/errors.py src/sluice/web/__init__.py src/sluice/web/contracts.py tests/unit/test_web_contracts.py
git commit -m "feat(web): add immutable web contracts"
~~~

### Task 2: Expand strict WebBinding configuration and identity

**Files:**
- Create: src/sluice/web/config.py
- Modify: src/sluice/config.py
- Modify: src/sluice/canonical.py
- Test: tests/unit/test_web_config.py
- Test: tests/unit/test_web_contracts.py

**Interfaces:**
- Produce decode_web_config(raw, base_dir) -> WebRegistryConfig.
- Produce expand_web_binding(use, with_config, registry) -> WebBinding | None.
- WebBinding.semantic_projection() includes materialized defaults, version, provider/extractor order, quality/output, and secret references; runtime_digest() may additionally key clients/limiters but never serializes secret values.
- Attach the frozen authored `WebRegistryConfig` to `Definition.web`; attach only the expanded binding to each built-in web `TaskDef`/`SourceDef`. `web_cache_version` stays in an authored web Source `with` mapping and is copied into that Source's expanded semantic binding; non-web source digest shape is unchanged.

- [ ] **Step 1: Write failing strict-loader tests.**

~~~python
def test_implicit_webpage_and_fetch_config_is_rejected(tmp_path):
    with pytest.raises(DefinitionError, match="fetch_chain"):
        load_definition(write_yaml(tmp_path, implicit_webpage_and_fetch_yaml()))


def test_expansion_materializes_defaults_without_resolving_secret(monkeypatch, tmp_path):
    monkeypatch.setenv("TEST_KEY", "actual-secret")
    definition = load_definition(write_yaml(tmp_path, explicit_web_yaml("env:TEST_KEY")))
    binding = definition.stages["article"].tasks[0].web_binding
    assert binding is not None
    projection = canonical_json(binding.semantic_projection())
    assert "env:TEST_KEY" in projection
    assert "actual-secret" not in projection
~~~

- [ ] **Step 2: Run the loader test.**

Run: python3 -m pytest -q tests/unit/test_web_config.py

Expected: FAIL because web is an unknown top-level key and TaskDef has no binding.

- [ ] **Step 3: Implement config expansion.**

Add web to config.py top-level keys. Materialize all Design §4 defaults, exact env:NAME references, provider/extractor whitelists, fixed Firecrawl v2 rejection, startup_http trusted requirement, quality fields, WebBinding output derivation, and Task/Source built-in field validation. Reject list_selector with output/extract/quality fields. Require fetch_chain and extract_chain for fetch_webpage and a final built-in projector after any Artifact-producing built-in.

Store each canonical Task entry as:

~~~python
{"use": task.use, "with": config,
 "web_binding": None if task.web_binding is None
                else task.web_binding.semantic_projection()}
~~~

Produce the expanded semantic Source binding that Task 6 will place in `_source_identity_config`; `web_cache_version` remains inside that binding. Add a unit test for expansion showing two otherwise identical webpage Sources differing only in `web_cache_version` have different semantic bindings. Task 6 adds the actual source-runtime digest test and preserves the non-web `{source,use,with}` digest shape.

- [ ] **Step 4: Verify key changes are deterministic.**

~~~python
def test_provider_order_and_minimum_change_stage_key(tmp_path):
    left = load_definition(write_yaml(tmp_path, explicit_web_yaml(chain=["a", "b"], min_chars=100)))
    reordered = load_definition(write_yaml(tmp_path, explicit_web_yaml(chain=["b", "a"], min_chars=100)))
    threshold = load_definition(write_yaml(tmp_path, explicit_web_yaml(chain=["a", "b"], min_chars=101)))
    assert left.stages["article"].canonical_config_json != reordered.stages["article"].canonical_config_json
    assert left.stages["article"].canonical_config_json != threshold.stages["article"].canonical_config_json
~~~

Run: python3 -m pytest -q tests/unit/test_web_config.py tests/unit/test_web_contracts.py tests/unit/test_collection_and_graph.py

Expected: PASS.

- [ ] **Step 5: Commit.**

~~~bash
git add src/sluice/web/config.py src/sluice/config.py src/sluice/contracts.py src/sluice/canonical.py tests/unit/test_web_config.py tests/unit/test_web_contracts.py tests/unit/test_collection_and_graph.py
git commit -m "feat(web): expand and key web bindings"
~~~

### Task 3: Decode web snapshots and define private built-in invocation

**Files:**
- Modify: src/sluice/resolution.py
- Modify: src/sluice/config.py
- Test: tests/unit/test_web_runtime.py

**Interfaces:**
- `decode_stage_snapshot` accepts the current `{use,with,web_binding}` Task shape and legacy `{use,with}` shape. A legacy entry receives `web_binding=None` in its in-memory TaskDef, but its canonical snapshot bytes are never rewritten.
- Define frozen `PrivateWebInvocation(with_config, web_binding)` construction for built-ins. It is not a public Mapping and is never passed to `FunctionTask.run`/`FunctionSource.collect`: resolution creates private `BuiltinWebTask.run_web(invocation, input)` and `BuiltinWebSource.collect_web(invocation, checkpoint)` wrappers, while custom callables always receive an independent thawed authored Mapping through their unchanged methods. Task 6 installs these per-definition wrappers.
- This Task deliberately creates no runtime catalog, opens no web App, and does not claim to resolve or supersede retained execution. Those operations require fixed registry/client construction and belong to Task 6.

- [ ] **Step 1: Write red snapshot and isolation tests.**

~~~python
def test_snapshot_decoder_preserves_canonical_bytes_for_both_task_shapes():
    legacy = canonical_json(legacy_snapshot_with_fetch_task())
    current = canonical_json(current_snapshot_with_web_binding())
    stage = decode_stage_snapshot(json.loads(legacy))
    assert stage.tasks[0].web_binding is None
    assert stage.canonical_config_json == legacy
    assert decode_stage_snapshot(json.loads(current)).canonical_config_json == current


def test_private_invocation_is_impossible_for_a_custom_task():
    assert private_web_invocation(custom_task_def) is None
~~~

- [ ] **Step 2: Run the focused test.**

Run: python3 -m pytest -q tests/unit/test_web_runtime.py -k 'legacy or snapshot or private'

Expected: FAIL because decode_stage_snapshot rejects the current 3-key Task shape and private invocation helpers do not exist.

- [ ] **Step 3: Implement exact decoder and private-data behavior.**

Decode current task snapshots shaped `{use,with,web_binding}` and legacy `{use,with}`. For the latter, create a TaskDef with a null binding while retaining the original two-key canonical JSON as `StageDef.canonical_config_json`; never normalize legacy persisted bytes by adding a null key. Add the immutable private-invocation helper, but do not yet wire it into runtime execution.

~~~python
def private_web_invocation(task: TaskDef) -> PrivateWebInvocation | None: ...
~~~

- [ ] **Step 4: Verify retained key consistency.**

Run: python3 -m pytest -q tests/unit/test_web_runtime.py tests/unit/test_contracts_and_keys.py

Expected: PASS.

- [ ] **Step 5: Commit.**

~~~bash
git add src/sluice/resolution.py src/sluice/config.py tests/unit/test_web_runtime.py
git commit -m "feat(web): decode retained web snapshots"
~~~

### Review Wave 1: Configuration and snapshot review

- [ ] Run parallel architecture/contract/scope and SQLite/concurrency/test-topology reviews against Tasks 1–3 and Design §§4–6, §14, and §18.
- [ ] Run python3 -m pytest -q tests/unit/test_web_contracts.py tests/unit/test_web_config.py tests/unit/test_web_runtime.py and git diff --check.
- [ ] Apply every accepted review fix in its own fix(web): commit before Task 4.

### Task 4: Implement safe transport, quality, and sanitizer primitives

**Files:**
- Create: src/sluice/web/url.py
- Create: src/sluice/web/http.py
- Create: src/sluice/web/quality.py
- Create: src/sluice/web/sanitize.py
- Test: tests/unit/test_web_safety.py
- Test: tests/unit/test_web_quality.py

**Interfaces:**
- Produce normalize_target_url, validate_target_headers, redact_url, `guard_resolved_addresses(host, target_policy)`, bounded_manual_get, evaluate_quality, sanitize_markdown, sanitize_metadata, and sanitize_web_error.
- All errors map to stable safe reasons; original vendor/transport text is never persisted.

- [ ] **Step 1: Write red URL, SSRF, quality, and Markdown tests.**

~~~python
@pytest.mark.parametrize("url", ["file:///etc/passwd", "http://127.0.0.1/", "http://user:pass@example.test/"])
def test_unsafe_target_urls_are_rejected(url):
    with pytest.raises(WebSafetyError):
        normalize_target_url(url)


def test_mixed_dns_and_private_redirect_are_rejected(monkeypatch):
    monkeypatch.setattr("sluice.web.url.resolve_host", lambda host: ["93.184.216.34", "10.0.0.1"])
    with pytest.raises(WebSafetyError, match="ssrf_blocked"):
        guard_resolved_addresses("example.test", DEFAULT_TARGET_POLICY)


def test_pinned_peer_service_redirect_and_protected_headers_are_enforced(fake_transport):
    assert bounded_manual_get(public_request(), direct_binding(fake_transport)).peer == "93.184.216.34"
    with pytest.raises(WebSafetyError, match="peer_mismatch"):
        bounded_manual_get(peer_rebinding_request(), direct_binding(fake_transport))
    with pytest.raises(WebSafetyError, match="service_redirect"):
        hosted_service_request_redirects(fake_transport)
    with pytest.raises(WebSafetyError, match="forbidden_header"):
        validate_target_headers({"Host": "internal.test"})


def test_challenge_and_unsafe_markdown_fail_or_sanitize():
    assert evaluate_quality(html_artifact("<div class='cf-turnstile'></div>"), QualityPolicy(min_chars=1)).quality.reason == "challenge"
    assert "javascript:" not in sanitize_markdown("[x](javascript:alert(1))<script>x</script>", "https://example.test/")
~~~

- [ ] **Step 2: Run the focused tests.**

Run: python3 -m pytest -q tests/unit/test_web_safety.py tests/unit/test_web_quality.py

Expected: FAIL during collection because the safety modules are absent.

- [ ] **Step 3: Implement exact Design safety semantics.**

Use manual HTTPX redirects with a maximum of ten hops; reject mixed/public-private DNS answers, unsafe IDNA/userinfo/numeric hosts, forbidden target headers, service redirects, oversized decompressed streams, and unsafe reported final URLs. Implement the version-1 challenge selector/path/phrase set, visible text/word/link ratio, MIME/status/sniffing rules, markdown-it-py raw-HTML/link/image removal, metadata field allowlists, and deterministic byte/list/string pruning.

~~~python
def bounded_manual_get(request: FetchRequest, binding: ProviderBinding) -> DirectResponse:
    for hop in range(11):
        target = normalize_target_url(request.url if hop == 0 else redirect_url)
        addresses = guard_resolved_addresses(target.host, binding.target_policy)
        response = send_pinned_httpx_request(target, addresses)
        if not response.is_redirect:
            return read_decompressed_limited(response, min(binding.max_body_bytes, 10_485_760))
        redirect_url = guard_redirect(response, target)
    raise WebSafetyError("too_many_redirects")
~~~

- [ ] **Step 4: Verify focused tests.**

Run: python3 -m pytest -q tests/unit/test_web_safety.py tests/unit/test_web_quality.py

Expected: PASS.

- [ ] **Step 5: Commit.**

~~~bash
git add src/sluice/web/url.py src/sluice/web/http.py src/sluice/web/quality.py src/sluice/web/sanitize.py tests/unit/test_web_safety.py tests/unit/test_web_quality.py
git commit -m "feat(web): add safe transport and content sanitization"
~~~

### Task 5: Add fixed offline extractors and projectors

**Files:**
- Create: src/sluice/web/extractors.py
- Create: src/sluice/web/projectors.py
- Modify: tests/unit/test_content_extraction.py
- Test: tests/unit/test_web_quality.py

**Interfaces:**
- identity accepts only provider-original text/Markdown.
- Trafilatura outputs sanitized Markdown; Newspaper4k, Resiliparse, and BeautifulSoup output normalized text.
- `project_markdown` and `project_text` accept an Artifact, an Item, or a bare string; they implement `input_format=auto|html|text|markdown`, preserve/merge existing Item metadata, require Artifact `selected_format`, and return NEW with every metadata.web key. Task 10 only binds these projectors into built-in Tasks; it does not duplicate conversion logic.

- [ ] **Step 1: Write failing extractor/projector tests.**

~~~python
def test_reextracting_under_new_digest_clears_prior_non_identity_output():
    reset = prepare_reextraction(extracted_markdown_artifact("chain-a"), "chain-b")
    assert reset.selected_format is None
    assert reset.extract_binding_digest is None
    assert reset.markdown is None


def test_projector_requires_selection_and_emits_bounded_metadata():
    with pytest.raises(ExecutionError, match="selected_format"):
        project_markdown(raw_html_artifact())
    result = project_markdown(markdown_artifact("# Title"))
    assert {"source_url", "final_url", "authors", "published_at", "warnings"} <= set(result.metadata["web"])


def test_projector_accepts_item_and_string_with_deterministic_metadata_merge():
    assert project_text(Item(...), input_format="auto").metadata["existing"] == "kept"
    assert project_markdown("<p>body</p>", input_format="html").value == "body"
~~~

- [ ] **Step 2: Run the focused tests.**

Run: python3 -m pytest -q tests/unit/test_content_extraction.py tests/unit/test_web_quality.py -k 'extract or projector'

Expected: FAIL because current Tasks use a handwritten parser and string values.

- [ ] **Step 3: Implement local-only extractor/projector behavior.**

Trafilatura calls extract() only on supplied HTML. Newspaper4k receives supplied HTML with fetch_images false and never download(); Resiliparse/BeautifulSoup never network. Network access maps to extract_failed. Project only selected_format; output RFC3339-Z-or-null published_at, ordered deduplicated authors, fixed warnings, and bounded sanitized metadata.

~~~python
EXTRACTOR_FORMATS = {"trafilatura": "markdown", "newspaper4k": "text",
                     "resiliparse": "text", "beautifulsoup": "text"}

def project_markdown(input: DocumentArtifact | Item | str, *, input_format: str = "auto") -> NEW:
    artifact = coerce_projector_input(input, input_format=input_format)
    if artifact.selected_format is None:
        raise ExecutionError("web Artifact has no selected_format")
    return NEW(render_markdown(artifact), metadata={"web": bounded_web_metadata(artifact, "markdown")})
~~~

- [ ] **Step 4: Verify extractors/projectors.**

Run: python3 -m pytest -q tests/unit/test_content_extraction.py tests/unit/test_web_quality.py

Expected: PASS.

- [ ] **Step 5: Commit.**

~~~bash
git add src/sluice/web/extractors.py src/sluice/web/projectors.py tests/unit/test_content_extraction.py tests/unit/test_web_quality.py
git commit -m "feat(web): add offline extractors and projectors"
~~~

#### Task 5 review-fix report (2026-08-20)

The follow-up review hardening keeps BeautifulSoup plain extraction aligned
with quality-visible text by removing `head`/`title` (and non-visible embedded
fallback nodes) before text selection while retaining a sanitized title
provenance field. Projector `input_format=auto` now recognizes only
deterministic actual HTML start tags; Markdown autolinks, email autolinks,
angle syntax, and fenced examples stay on the Markdown path. Identity reuse
requires advertised `text`/`markdown` capabilities for provider-origin
artifacts and uses the explicit `input` marker for direct/local artifacts.
Nested-parenthesis unsafe link/image destinations are consumed as complete
tokens before sanitization, so no trailing delimiter survives. Markdown link
metadata is AST-derived (excluding code/raw HTML), and the local HTML fallback
escapes literal text nodes and drops embedded metadata links safely. The
newspaper4k adapter consumes supplied HTML through its in-memory
`download(input_html=...)` path, disables read-more/meta-refresh traversal, and
shadows image probing so optional parsing cannot perform network requests.

Focused verification: `python3 -m pytest -q tests/unit/test_content_extraction.py tests/unit/test_web_quality.py`
(`39 passed`).

### Task 6: Implement fixed core providers and registry

**Files:**
- Create: src/sluice/web/providers.py
- Create: src/sluice/web/registry.py
- Create: src/sluice/web/client.py
- Modify: src/sluice/resolution.py
- Modify: src/sluice/app.py
- Modify: src/sluice/source_runtime.py
- Modify: src/sluice/reconcile.py
- Modify: src/sluice/stage_runtime.py
- Modify: src/sluice/cli.py
- Modify: src/sluice/prefect_flow.py
- Test: tests/unit/test_web_providers.py
- Test: tests/unit/test_web_safety.py
- Create: tests/integration/test_web_retention.py

**Interfaces:**
- build_provider(binding, runtime) -> WebProvider and build_extractor(binding) -> WebExtractor use fixed maps only.
- Implement httpx, startup_http, optional curl_cffi, and combined Trafilatura provider profile.
- Define `WebRuntimeCatalog`: fixed-map binding validation plus runtime-digest keyed provider clients/limiters. It owns a `WebContentClient` shell; Task 7 adds the shell's `resolve` algorithm.
- Complete startup binding only after this registry exists: `freeze_adapters` accepts `web_content_client`, and `open_app(..., web_content_client=None)` accepts an explicit test/deployment client in preference to a catalog-built client. Resolution wraps every built-in web definition in `run_web(PrivateWebInvocation, input)`/`collect_web(PrivateWebInvocation, checkpoint)` and constructs a fresh invocation per definition; it never passes that dataclass to public Mapping-based function contracts. Custom callables receive only thawed authored configuration. The path cache must never capture a chain from one Source/Task definition for another.
- Add synchronous/idempotent `App.close()` to the frozen App dataclass. An admission lock/condition shared with reconcile rejects new drains, waits existing local executor drains, closes catalog clients, then closes SQLite. CLI/Prefect call only `app.close()`.
- At startup fully resolve current plus queued/running/waiter bindings. Validate terminal retained snapshots structurally/fixed-map only, with secret resolution deferred to explicit rebuild. A legacy web Stage is a retained snapshot with `web_binding is None` on any `fetch_webpage`, `extract_content`, `to_markdown`, or `to_text` Task: queued/running instances are CAS-superseded before a provider call, terminal instances remain readable, and `rebuild_stage` raises `DefinitionError("legacy web snapshot cannot rebuild")` without current-YAML fallback. A legacy web Source is the analogous built-in RSS/webpage snapshot without an expanded binding; queued/running instances are superseded. Any Source whose stored binding digest differs from current is likewise CAS-superseded before either can call a provider. Cover both fetch and projector-only legacy Stages.

- [ ] **Step 1: Write red core-provider tests.**

~~~python
def test_trafilatura_provider_returns_raw_html_and_provider_original_markdown(fake_transport):
    artifact = build_provider(trafilatura_binding(), runtime(fake_transport)).fetch(article_request())
    assert artifact.raw_html == "<article>body</article>"
    assert artifact.markdown == "body"


def test_startup_http_requires_trust_and_has_no_peer_claim():
    with pytest.raises(DefinitionError, match="trusted"):
        build_provider(startup_http_binding(False), runtime(FakeHttp("ok")))
    assert build_provider(startup_http_binding(True), runtime(FakeHttp("ok"))).fetch(text_request()).status_code == 200


def test_app_close_is_idempotent_and_waits_before_closing_catalog(tmp_path):
    app, catalog = opened_app_with_blocked_drain(tmp_path)
    close_thread = Thread(target=app.close)
    close_thread.start(); assert catalog.close_calls == 0
    release_drain(); close_thread.join(5)
    app.close(); assert catalog.close_calls == 1


def test_terminal_missing_secret_does_not_block_startup_but_rebuild_fails(tmp_path):
    app = open_app_with_terminal_env_ref(tmp_path, missing="RETIRED_KEY")
    with pytest.raises(DefinitionError, match="RETIRED_KEY"):
        rebuild_stage(app, terminal_execution_id(app), instant(1))


def test_terminal_legacy_web_snapshot_is_readable_but_never_rebuildable(tmp_path):
    app = open_app_with_terminal_legacy_web_snapshot(tmp_path)
    with pytest.raises(DefinitionError, match="legacy web snapshot"):
        rebuild_stage(app, terminal_execution_id(app), instant(1))
~~~

- [ ] **Step 2: Run the focused tests.**

Run: python3 -m pytest -q tests/unit/test_web_providers.py tests/integration/test_web_retention.py -k 'startup_http or close or retained or custom or supersede'

Expected: FAIL because the fixed provider registry does not exist.

- [ ] **Step 3: Implement core adapters, runtime catalog, and startup lifecycle.**

Direct adapters return only representations allowed by FetchRequest. startup_http maps its string to status 200/final request URL only with trusted true and emits a catalog startup warning. Trafilatura returns raw HTML plus provider-original Markdown and never changes extract-chain order. Import curl_cffi only when configured; missing optional dependency is a startup DefinitionError naming the instance. The client shell exists after this Task, but gains its fallback algorithm only in Task 7.

~~~python
CORE_PROVIDER_BUILDERS = {"httpx": build_httpx_provider,
                          "startup_http": build_startup_http_provider,
                          "curl_cffi": build_curl_cffi_provider,
                          "trafilatura": build_trafilatura_provider}
~~~

Separate web Source identity from its persisted/collect configuration. The former may contain the expanded semantic binding; the latter always stays authored and contains no binding, resolved secret, or transient artifact:

~~~python
def _source_identity_config(source: SourceDef) -> Mapping[str, JSONValue]:
    if source.web_binding is None:
        return {"source": source.name, "use": source.use, "with": source.with_config}
    return {"source": source.name, "use": source.use, "with": source.with_config,
            "web_binding": source.web_binding.semantic_projection()}

def _source_execution_config(source: SourceDef) -> Mapping[str, JSONValue]:
    return {"source": source.name, "use": source.use, "with": source.with_config}
~~~

Use `_source_identity_config` only for `_binding_digest`; use `_source_execution_config` for `executions.config_json` and collection input. Add same-path/different-chain isolation, custom configuration isolation, changed-binding queued/running Source supersession, retained Stage snapshot, double-close, and SQLite assertions that Source `config_json` has no `web_binding` or secret value.

- [ ] **Step 4: Verify core providers.**

Run: python3 -m pytest -q tests/unit/test_web_providers.py tests/unit/test_web_safety.py tests/unit/test_web_runtime.py tests/integration/test_web_retention.py

Expected: PASS.

- [ ] **Step 5: Commit.**

~~~bash
git add src/sluice/web/providers.py src/sluice/web/registry.py src/sluice/web/client.py src/sluice/resolution.py src/sluice/app.py src/sluice/source_runtime.py src/sluice/reconcile.py src/sluice/stage_runtime.py src/sluice/cli.py src/sluice/prefect_flow.py tests/unit/test_web_providers.py tests/unit/test_web_safety.py tests/unit/test_web_runtime.py tests/integration/test_web_retention.py
git commit -m "feat(web): bind fixed provider runtimes at startup"
~~~

### Review Wave 2: Security, extraction, and core provider review

- [ ] Run the two required review tracks against Tasks 4–6 and Design §§5, 7–9, 11–14, and AC8–AC15.
- [ ] Run python3 -m pytest -q tests/unit/test_web_safety.py tests/unit/test_web_quality.py tests/unit/test_web_providers.py and python3 -m compileall -q src tests.
- [ ] Commit accepted review fixes separately before Task 7.

### Task 7: Implement the sequential WebContentClient coordinator

**Files:**
- Modify: src/sluice/web/client.py
- Create: src/sluice/web/select.py
- Create: src/sluice/web/feed.py
- Modify: src/sluice/web/contracts.py
- Test: tests/integration/test_web_pipeline.py
- Test: tests/unit/test_web_runtime.py

**Interfaces:**
- WebContentClient.resolve(request, fetch_chain, extract_chain) -> DocumentArtifact.
- Task 7 owns `select_list_records(raw_html, final_url, selector)` in `sluice.web.select`; both the coordinator and Task 11 webpage Source import this one CSS/SoupSieve helper.
- Task 7 owns `validate_raw_feed(raw_source) -> ValidatedFeedRoot | None` in `sluice.web.feed`: bounded defusedxml parse, DTD/entity/XInclude rejection, and RSS2/Atom/RSS1 root validation. The coordinator uses it for raw-source winner selection; Task 11 reuses it to project entries.

- [ ] **Step 1: Write failing three-mode fallback tests.**

~~~python
def test_article_falls_through_challenge_to_second_provider_markdown(client):
    artifact = client.resolve(article_request(), ["first", "second"], identity_chain())
    assert artifact.provider_instance == "second"
    assert client.calls == ["first", "second"]


def test_list_mode_uses_selector_inside_provider_loop(client):
    artifact = client.resolve(list_request("article.release"), ["static", "browser"], None)
    assert artifact.provider_instance == "browser"
    assert [m.href for m in select_list_records(artifact.raw_html, artifact.final_url, "article.release")] == ["/new"]


def test_raw_source_rejects_markdown_and_accepts_zero_entry_atom(client):
    artifact = client.resolve(raw_source_request(), ["reader", "feed"], None)
    assert artifact.provider_instance == "feed"
    assert artifact.raw_source.startswith(b"<feed")
~~~

- [ ] **Step 2: Run the coordinator test.**

Run: python3 -m pytest -q tests/integration/test_web_pipeline.py -k 'falls_through or list_mode or raw_source'

Expected: FAIL because WebContentClient.resolve is absent.

- [ ] **Step 3: Implement Design §6.7 exactly.**

Pre-filter capabilities, acquire providers in declared order, then apply article identity/extractor fallback, list shared-selector match/earliest-valid-empty handling, or raw-source defusedxml validation. Never prefetch/race. Record ordered redacted attempts. Raise FetchChainExhausted only after eligible article providers exhaust.

~~~python
if request.mode == "list_html":
    matches = select_list_records(artifact.raw_html, artifact.final_url, request.list_selector)
    if matches:
        return artifact
    empty_list_candidate = empty_list_candidate or artifact
elif request.mode == "raw_source" and validate_raw_feed(artifact.raw_source) is not None:
    return artifact
~~~

- [ ] **Step 4: Verify identity avoids duplicate work.**

~~~python
def test_passing_provider_markdown_never_runs_local_extractor(client):
    artifact = client.resolve(article_request(), ["reader"], exploding_extractor_chain())
    assert artifact.selected_format == "markdown"
    assert client.extractor_calls == []
~~~

Run: python3 -m pytest -q tests/integration/test_web_pipeline.py tests/unit/test_web_runtime.py

Expected: PASS.

- [ ] **Step 5: Commit.**

~~~bash
git add src/sluice/web/client.py src/sluice/web/select.py src/sluice/web/feed.py src/sluice/web/contracts.py tests/integration/test_web_pipeline.py tests/unit/test_web_runtime.py
git commit -m "feat(web): resolve ordered fallback chains"
~~~

### Task 8: Add Firecrawl and Crawl4AI adapters

**Files:**
- Modify: src/sluice/web/providers.py
- Test: tests/unit/test_web_providers.py
- Test: tests/integration/test_web_pipeline.py

**Interfaces:**
- Firecrawl uses v2 POST /v2/scrape only.
- Crawl4AI uses synchronous POST /crawl with cache_mode bypass only; no job polling state.

- [ ] **Step 1: Write request mapping red tests.**

~~~python
def test_firecrawl_article_requests_markdown_and_raw_html_once(fake_service):
    FirecrawlProvider(binding("fire"), fake_service).fetch(article_request())
    payload = fake_service.requests[0].json
    assert fake_service.requests[0].path == "/v2/scrape"
    assert payload["formats"] == ["markdown", "rawHtml"]
    assert payload["maxAge"] == 0 and payload["storeInCache"] is False
    assert payload["skipTlsVerification"] is False


def test_crawl4ai_uses_sync_crawl_never_legacy_poll_paths(fake_service):
    Crawl4AIProvider(binding("browser"), fake_service).fetch(article_request())
    assert [request.path for request in fake_service.requests] == ["/crawl"]
    assert fake_service.requests[0].json["cache_mode"] == "bypass"
~~~

- [ ] **Step 2: Run the focused tests.**

Run: python3 -m pytest -q tests/unit/test_web_providers.py -k 'firecrawl or crawl4ai'

Expected: FAIL because both types are unsupported.

- [ ] **Step 3: Implement adapters.**

Reject Firecrawl v1 at loading. Article requests markdown plus rawHtml; list/raw-source requests rawHtml only; raw source is conditional on feed validation. Map every validated Firecrawl option: `only_main_content`, `wait_for_selector`, `wait_for`, `mobile`, and the canonical Design action list, alongside the fixed freshness/TLS fields below. Crawl4AI sends optional Bearer authentication only when configured and supports unauthenticated self-host instances. Do not implement /crawl/job, /job, /task, or /jobs.

~~~python
def firecrawl_payload(binding: ProviderBinding, request: FetchRequest) -> dict[str, JSONValue]:
    formats = ["markdown", "rawHtml"] if request.mode == "article" else ["rawHtml"]
    return {"url": request.url, "formats": formats,
            "onlyMainContent": binding.only_main_content,
            "waitFor": binding.wait_for, "waitForSelector": binding.wait_for_selector,
            "mobile": binding.mobile, "actions": firecrawl_actions(binding.actions),
            "maxAge": 0, "storeInCache": False, "skipTlsVerification": False}
~~~

- [ ] **Step 4: Verify adapters.**

Run: python3 -m pytest -q tests/unit/test_web_providers.py tests/integration/test_web_pipeline.py -k 'firecrawl or crawl4ai or raw_source'

Expected: PASS.

- [ ] **Step 5: Commit.**

~~~bash
git add src/sluice/web/providers.py tests/unit/test_web_providers.py tests/integration/test_web_pipeline.py
git commit -m "feat(web): add firecrawl and crawl4ai adapters"
~~~

### Task 9: Add Jina, Zyte, Bright Data, limiter, and cooldown

**Files:**
- Modify: src/sluice/web/providers.py
- Modify: src/sluice/web/client.py
- Test: tests/unit/test_web_providers.py
- Test: tests/integration/test_web_pipeline.py
- Test: tests/unit/test_web_runtime.py

**Interfaces:**
- Runtime catalog shares client/semaphore/cooldown by runtime digest.
- Every provider failure becomes WebAttemptFailure with no secret/vendor body.

- [ ] **Step 1: Write red hosted-provider and limiter tests.**

~~~python
def test_zyte_browser_mode_maps_page_content_and_type_action(fake_service):
    ZyteProvider(zyte_browser_binding(), fake_service).fetch(article_request())
    payload = fake_service.requests[0].json
    assert payload["browserHtml"] is True
    assert payload["pageContentOptions"]["extractFrom"] == "browserHtml"
    assert payload["actions"] == [{"action": "type", "selector": {"type": "css", "value": "#q"}, "text": "sluice"}]


def test_busy_provider_advances_without_a_network_call(client):
    client.hold_limiter("first")
    artifact = client.resolve(article_request(), ["first", "second"], identity_chain())
    assert artifact.provider_instance == "second"
    assert client.attempt_reasons == ["provider_busy"]


def test_source_attempt_without_stage_deadline_uses_provider_deadline_only(client):
    client.resolve(source_request_without_stage_deadline(), ["first"], identity_chain())
    assert client.effective_deadline == client.provider_attempt_started + 15
~~~

- [ ] **Step 2: Run the focused tests.**

Run: python3 -m pytest -q tests/unit/test_web_providers.py tests/unit/test_web_runtime.py -k 'zyte or bright or jina or busy'

Expected: FAIL because hosted adapters and runtime limiter do not exist.

- [ ] **Step 3: Implement exact mappings and operational control.**

Jina is Markdown-only. Zyte performs one Basic-auth POST /v1/extract with Design mode/action mappings. Bright Data uses format raw and adds data_format markdown only for Markdown output. Limiter acquisition, Retry-After cooldown, connect/read/body streaming, and provider timeout consume one effective provider budget. `provider_attempt_started` is recorded before limiter acquisition, so waiting consumes the bound. busy/rate-limited paths advance without a request and remain in-memory only.

~~~python
provider_attempt_started = monotonic()
provider_deadline = provider_attempt_started + binding.timeout_seconds
effective_deadline = provider_deadline if stage_deadline is None else min(stage_deadline, provider_deadline)
~~~

- [ ] **Step 4: Verify provider behavior.**

Run: python3 -m pytest -q tests/unit/test_web_providers.py tests/unit/test_web_runtime.py tests/integration/test_web_pipeline.py

Expected: PASS.

- [ ] **Step 5: Commit.**

~~~bash
git add src/sluice/web/providers.py src/sluice/web/client.py tests/unit/test_web_providers.py tests/unit/test_web_runtime.py tests/integration/test_web_pipeline.py
git commit -m "feat(web): add hosted providers and limits"
~~~

### Review Wave 3: Fallback, vendor, and paid-call review

- [ ] Run the required parallel reviews against Tasks 7–9 and Design §§3, 6–7, 9, 11–12, and AC1–AC7.
- [ ] Run python3 -m pytest -q tests/unit/test_web_providers.py tests/unit/test_web_runtime.py tests/integration/test_web_pipeline.py.
- [ ] Inspect tests to ensure fake transports/coordinators are used and no external endpoint is contacted; commit accepted fixes separately.

### Task 10: Make built-in content Tasks artifact-aware

**Files:**
- Modify: src/sluice/tasks/content.py
- Modify: src/sluice/resolution.py
- Modify: src/sluice/stage_runtime.py
- Modify: src/sluice/config.py
- Modify: src/sluice/tasks/__init__.py
- Modify: tests/unit/test_content_extraction.py
- Test: tests/integration/test_web_pipeline.py

**Interfaces:**
- fetch_webpage returns NEW(DocumentArtifact).
- extract_content accepts Item, string, or DocumentArtifact and reruns a different extract digest safely.
- to_markdown/to_text are the sole Artifact projectors and return JSON NEW values.
- Register `sluice.tasks.to_text` as an each built-in in `BUILTIN_TASK_MODES` and `BUILTIN_TASK_BINDERS`, export it from `sluice.tasks`, and construct its bound Task in content.py. `to_markdown` remains registered and receives the new projector behavior.

- [ ] **Step 1: Write red Task-chain tests.**

~~~python
def test_fetch_extract_project_chain_materializes_json_not_artifact(app_with_fake_web_client):
    tick(app_with_fake_web_client, instant(0))
    payload, metadata = app_with_fake_web_client.conn.execute(
        "SELECT payload_json,metadata_json FROM items WHERE producer_kind='stage'"
    ).fetchone()
    assert "DocumentArtifact" not in payload
    assert '"web"' in metadata


def test_loader_rejects_terminal_fetch_artifact(tmp_path):
    with pytest.raises(DefinitionError, match="to_markdown or to_text"):
        load_definition(write_fetch_only_stage(tmp_path))
~~~

- [ ] **Step 2: Run the focused tests.**

Run: python3 -m pytest -q tests/unit/test_content_extraction.py tests/integration/test_web_pipeline.py -k 'fetch or artifact or projector'

Expected: FAIL because fetch_webpage returns a string and stage runtime passes only authored config.

- [ ] **Step 3: Implement private built-in Task invocation.**

Only built-in web Tasks receive immutable private invocation data composed from TaskDef.with_config plus WebBinding through the Task 6 `run_web` wrapper; `FunctionTask.run` remains Mapping-based for customs. Bind the tested `project_markdown`/`project_text` implementations from Task 5; preserve direct Item/string projection compatibility. Reject a Stage final Artifact through loader validation.

~~~python
def _task_run(task: object, task_def: TaskDef, value: object) -> object:
    if isinstance(task, BuiltinWebTask):
        return task.run_web(private_web_invocation(task_def), value)
    return task.run(task_def.with_config, value)
~~~

- [ ] **Step 4: Verify Tasks.**

Run: python3 -m pytest -q tests/unit/test_content_extraction.py tests/integration/test_web_pipeline.py

Expected: PASS.

- [ ] **Step 5: Commit.**

~~~bash
git add src/sluice/tasks/content.py src/sluice/resolution.py src/sluice/stage_runtime.py src/sluice/config.py src/sluice/tasks/__init__.py tests/unit/test_content_extraction.py tests/integration/test_web_pipeline.py
git commit -m "feat(web): run content tasks through artifacts"
~~~

### Task 11: Integrate webpage and RSS Sources

**Files:**
- Modify: src/sluice/sources/webpage.py
- Modify: src/sluice/sources/rss.py
- Modify: src/sluice/resolution.py
- Modify: src/sluice/source_runtime.py
- Create: tests/integration/test_web_sources.py
- Modify: tests/integration/test_source_lifecycle.py

**Interfaces:**
- Webpage list resolves list_html and calls shared select_list_records; single-page resolves article then projects content.
- RSS uses synthetic __rss_direct__ then raw_source fallback, and parses bounded defusedxml RSS2/Atom/RSS1 only.

- [ ] **Step 1: Write red Source tests.**

~~~python
def test_list_source_defers_valid_empty_static_page_to_browser(tmp_path, scripted_client):
    app = open_app(write_list_source_definition(tmp_path), tmp_path / "state.db",
                   web_content_client=scripted_client)
    collect_source(app, "releases", instant(0))
    assert [row[0] for row in app.conn.execute("SELECT logical_id FROM items ORDER BY logical_id")] == ["https://example.test/new"]


def test_rss_fallback_accepts_valid_zero_entry_atom_only(tmp_path, scripted_client):
    app = open_app(write_rss_definition(tmp_path), tmp_path / "state.db",
                   web_content_client=scripted_client)
    collect_source(app, "feed", instant(0))
    assert app.conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 0
~~~

- [ ] **Step 2: Run the focused tests.**

Run: python3 -m pytest -q tests/integration/test_web_sources.py tests/integration/test_source_lifecycle.py -k 'webpage or rss or builtin'

Expected: FAIL because Sources use HTTPClient strings, webpage stores raw HTML, and RSS uses xml.etree.

- [ ] **Step 3: Implement safe Source behavior.**

Webpage list normalizes/deduplicates HTTP(S) hrefs in document order and attaches list_selector provenance. Single-page projects text/Markdown before SourceRecord construction. RSS rejects literal YAML XML/content, reuses Task 7's `validate_raw_feed`, sanitizes entry markup, and discards raw feed bytes before SourceResult validation. The pure literal helper remains direct-call-only and never persists config/body.

~~~python
def parse_feed_bytes(raw: bytes, final_url: str) -> tuple[SourceRecord, ...]:
    root = validate_raw_feed(raw)
    if root is None:
        raise ExecutionError("invalid RSS raw source")
    return tuple(project_feed_entry(entry, final_url) for entry in ordered_feed_entries(root.root))
~~~

- [ ] **Step 4: Migrate old fixture definitions and verify.**

Update built-in Source fixtures to name trusted startup_http. Webpage fixtures add fetch_chain, extract_chain, and output. Update the loader RSS/projector fixture in `tests/unit/test_collection_and_graph.py` to assert the new expanded snapshot shape, and confirm its RSS `url`-only direct-provider case remains valid. Do not alter `test_acceptance.py`: it uses custom Sources and is intentionally outside web fixture migration. Run:

python3 -m pytest -q tests/integration/test_web_sources.py tests/integration/test_source_lifecycle.py tests/unit/test_collection_and_graph.py

Expected: PASS.

- [ ] **Step 5: Commit.**

~~~bash
git add src/sluice/sources/webpage.py src/sluice/sources/rss.py src/sluice/resolution.py src/sluice/source_runtime.py tests/integration/test_web_sources.py tests/integration/test_source_lifecycle.py tests/unit/test_collection_and_graph.py
git commit -m "feat(web): integrate safe webpage and rss sources"
~~~

### Task 12: Add durable retry, cancellation, shared cache, and end-to-end regression

**Files:**
- Modify: src/sluice/stage_runtime.py
- Modify: src/sluice/reconcile.py
- Modify: src/sluice/app.py
- Modify: src/sluice/web/client.py
- Modify: src/sluice/web/sanitize.py
- Modify: tests/integration/test_shared_materialization.py
- Create: tests/integration/test_web_stage_lifecycle.py
- Modify: tests/smoke/test_management.py
- Modify: tests/smoke/test_prefect_parity.py

**Interfaces:**
- _execute_claimed_stage handles FetchChainExhausted, WebAttemptCancelled, and WebStageDeadlineExpired before generic failures.
- Exhausted eligible fetch_webpage creates one guarded empty succeeded StageResult with executions.error SQL NULL.
- Define `WebTaskFailure(task_index: int, cause: FetchChainExhausted)` in stage_runtime.py; `run_stage_tasks` raises it only when a built-in web Task raises FetchChainExhausted, preserving the actual failing index. Define `terminal_fetch_drop_eligible(stage: StageDef, failed_task_index: int) -> bool` there: only an each Stage; failed Task is the first Artifact-producing built-in `fetch_webpage`; it has zero or one immediately following built-in `extract_content`; it ends in exactly one `to_markdown` or `to_text`; and no custom Task follows fetch. Define `retries_exhausted(execution, policy) -> bool` from persisted retry count/policy, not a synthetic counter.
- `sanitize_web_error(error: BaseException) -> str` lives in `web/sanitize.py`. Task 12 sets/clears the Task 1 `WebExecutionProbe` context variable in `_execute_claimed_stage`; client.py checks it before/after limiter, send, response/extraction, and every provider advance.

- [ ] **Step 1: Write red durable lifecycle tests.**

~~~python
def test_fetch_exhaustion_retries_then_publishes_empty_success(tmp_path):
    app = web_stage_app(tmp_path, retries=1, provider_results=[FetchChainExhausted(())] * 2)
    tick(app, instant(0))
    rows = app.conn.execute("SELECT state,error,retry_count FROM executions ORDER BY id").fetchall()
    assert [(row[0], row[2]) for row in rows] == [("failed", 0), ("succeeded", 1)]
    assert rows[-1][1] is None
    assert app.conn.execute("SELECT COUNT(*) FROM stage_result_current").fetchone()[0] == 1


def test_batch_custom_and_extractor_failures_never_convert_to_empty_success(tmp_path):
    for definition in (batch_fetch_definition, custom_after_fetch_definition, extract_failure_definition):
        app = open_app(definition(tmp_path), tmp_path / "state.db", web_content_client=failing_client())
        tick(app, instant(0))
        assert app.conn.execute("SELECT state FROM executions ORDER BY id DESC LIMIT 1").fetchone()[0] == "failed"


@pytest.mark.parametrize("definition", [
    batch_fetch_definition, fetch_not_first_definition, custom_after_fetch_definition,
    two_extractors_definition, no_projector_definition, two_projectors_definition,
])
def test_terminal_fetch_drop_eligibility_rejects_every_disallowed_shape(tmp_path, definition):
    stage = load_definition(definition(tmp_path)).stages["web"]
    assert terminal_fetch_drop_eligible(stage, failed_task_index=0) is False


def test_stale_timeout_cannot_start_second_paid_provider_or_publish(tmp_path):
    app, client = blocking_web_stage_app(tmp_path)
    worker = Thread(target=tick, args=(app, instant(0)))
    worker.start(); assert client.first_started.wait(5)
    tick(app, instant(1)); client.release_first.set(); worker.join(5)
    assert client.calls == ["first"]
    assert app.conn.execute("SELECT COUNT(*) FROM stage_results").fetchone()[0] == 0


def test_web_errors_and_management_views_never_expose_vendor_body_or_secret(tmp_path, caplog):
    app = web_stage_app(tmp_path, retries=0,
                         provider_results=[provider_error("Authorization: env:KEY body=private")])
    tick(app, instant(0))
    error = app.conn.execute("SELECT error FROM executions ORDER BY id DESC LIMIT 1").fetchone()[0]
    assert "private" not in error and "KEY" not in error
    assert "private" not in caplog.text
~~~

- [ ] **Step 2: Run the lifecycle tests.**

Run: python3 -m pytest -q tests/integration/test_web_stage_lifecycle.py tests/integration/test_shared_materialization.py -k 'exhaustion or timeout or web'

Expected: FAIL because stage runtime only has generic and LLM invalid-drop terminal handling.

- [ ] **Step 3: Implement guarded WebExecutionProbe and terminal conversion.**

Install the probe before built-in Task execution and clear it in `finally`. The client checks it before/after limiter acquisition, before send/poll, after response/extractor, and before another provider. Derive `stage_deadline = execution.started_at + policy.timeout` only when policy.timeout is not None; otherwise use None. Combine it with the provider-attempt deadline exactly as in Task 9; Sources pass None. On final eligible fetch exhaustion, guarded-finalize StageOutcome((), "succeeded"), set error NULL, attach shared waiters, and update stage_result_current. All other failure shapes remain failed; do not alter InvalidStructuredOutput behavior.

Emit provider-attempt structured log fields only through safe WebAttemptFailure values. Before logging or storing any web error, call sanitize_web_error; unknown exceptions become provider_error without str(error). Management/read-model output continues to read Execution state/error only and gains no provider-attempt table.

~~~python
except WebTaskFailure as failure:
    if (terminal_fetch_drop_eligible(stage, failure.task_index)
            and retries_exhausted(execution, policy)):
        finalize_terminal_empty_fetch(app, execution.id, failure.cause, at)
        return execution_id
    retry_id = _fail_execution(app, execution.id, sanitize_web_error(failure.cause), at)
    return execution_id if retry_id is None else _execute_claimed_stage(app, retry_id, at)
~~~

- [ ] **Step 4: Update shared/smoke fixtures and verify end to end.**

In test_shared_materialization.shared_app, declare trusted startup_http, named fetch/extract chains, min_chars 1 for tiny bodies, and final projector. Update management/Prefect expected metadata for the intentional WebBinding cache partition. Run:

python3 -m pytest -q tests/integration/test_web_stage_lifecycle.py tests/integration/test_web_pipeline.py tests/integration/test_web_sources.py tests/integration/test_shared_materialization.py tests/smoke/test_management.py tests/smoke/test_prefect_parity.py

Expected: PASS.

- [ ] **Step 5: Commit.**

~~~bash
git add src/sluice/stage_runtime.py src/sluice/reconcile.py src/sluice/app.py src/sluice/web/client.py src/sluice/web/sanitize.py tests/integration/test_shared_materialization.py tests/integration/test_web_stage_lifecycle.py tests/smoke/test_management.py tests/smoke/test_prefect_parity.py
git commit -m "feat(web): make web retries and cancellation durable"
~~~

### Review Wave 4: Durable runtime and acceptance review

- [ ] Run the Design-required architecture/contract/scope and SQLite/concurrency/test-topology reviews against Tasks 10–12 and Design §§10–18.
- [ ] Run a fresh database init_schema twice, PRAGMA foreign_key_check, PRAGMA integrity_check, and exact retained snapshot/rebuild tests.
- [ ] Run python3 -m pytest -q tests/unit/test_web_*.py tests/integration/test_web_*.py and verify all provider interactions use fake transports/coordinators.
- [ ] Commit every accepted review fix separately before final verification.

## Final Verification and Handoff

- [ ] Install implementation dependencies: python3 -m pip install -e '.[test,web,ui,prefect]'.
- [ ] Run python3 -m pytest -p no:cacheprovider -q tests and record the exact result. Treat the known Prefect closed-console teardown traceback as non-test output only when pytest exits zero.
- [ ] Run python3 -m compileall -q src tests, python3 -m pip check, git diff --check, and fresh-schema foreign-key/integrity checks.
- [ ] Run sluice --help, a temporary explicit-web-definition sluice tick, and the Prefect parity smoke.
- [ ] Confirm git status --short --untracked-files=all is clean and .superpowers remains ignored/absent from reachable history.
- [ ] Use superpowers:verification-before-completion before claiming completion, then superpowers:finishing-a-development-branch to decide integration.
