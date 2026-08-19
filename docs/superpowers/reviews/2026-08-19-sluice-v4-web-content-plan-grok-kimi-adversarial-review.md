# Web content pipeline Plan adversarial review

Date: 2026-08-19

Scope: `docs/superpowers/plans/2026-08-19-sluice-v4-web-content-pipeline.md`,
the approved Design, and the v4-cleanroom baseline. Reviews were read-only;
neither reviewer edited the repository.

## Independent reviewers

- Paseo / Grok Build, model `grok-4.6`, agent `5c3d1d1b-05f4-45d9-aa7f-18a42669ff9c`.
- Paseo / Kimi CLI, model `kimi-code/k3-256k` (K3-256K), agent
  `0ffbc968-2e3f-4bac-8e1b-70452ce67d27`.

Kimi CLI's ACP launcher requires an executable whose argv[0] ends in `kimi`.
The isolated Paseo daemon used a local `kimi` symlink to `kimi-cli`, then ran
`kimi acp`; no shared Paseo configuration was changed.

## Accepted findings and Plan changes

| Finding | Decision | Revision |
| --- | --- | --- |
| Startup catalog/client lifecycle was scheduled before fixed registry/client implementation. | Accepted. | Task 3 is now snapshot/private-invocation only. Task 6 owns registry-backed catalog construction, startup binding, admission/drain close, retained-binding validation, and Source supersession. |
| Task 3 and Task 12 both claimed the lifecycle test file. | Accepted. | Task 3 is unit-only; Task 6 creates `test_web_retention.py`; Task 12 exclusively owns `test_web_stage_lifecycle.py`. |
| `to_text` had no explicit built-in registration path. | Accepted. | Task 10 now modifies config, resolution, and task exports as well as content implementation. |
| Current and legacy snapshot canonical bytes need distinct handling. | Accepted. | Task 3 preserves legacy two-key canonical JSON while decoding it to a null binding in memory; current three-key snapshots round-trip exactly. |
| Terminal empty-success helper was underspecified. | Accepted. | Task 12 defines `terminal_fetch_drop_eligible`, persisted retry exhaustion, a complete negative eligibility matrix, probe ownership, and error-sanitizer ownership. |
| Optional Stage/Source deadlines could call `min(None, ...)`. | Accepted. | Tasks 9 and 12 define provider-only deadlines when the Stage deadline is absent and add coverage. |
| Source binding/private invocation and changed binding recovery were too implicit. | Accepted. | Task 6 defines per-definition private wrappers, same-path isolation, web-only semantic source digest, and queued/running Source CAS supersession. |
| Firecrawl option mapping and SSRF test specificity were incomplete. | Accepted. | Tasks 4 and 8 now cover peer pinning, service redirect/protected headers, and every validated Firecrawl option. |
| `select_list_records` lacked an owning task. | Accepted. | Task 7 creates `sluice.web.select`; Task 11 imports it. |
| Terminal retained snapshots must not force missing secret resolution during read-only startup. | Accepted. | Global constraints and Task 6 distinguish executable retained work from terminal structural validation and explicit rebuild resolution. |

## Rejected or narrowed findings

- `to_markdown` is already a built-in in the baseline. Only `to_text` needed a
  new registration path.
- `range(11)` in the manual redirect sketch is intentional: one initial request
  plus at most ten redirects. It is not a Plan ambiguity.
- The existing code's lack of web functionality is implementation work, not a
  Plan defect; revisions address only missing task ownership and contracts.

## Result

The Plan now has a coherent dependency order and explicit owners for snapshot
compatibility, runtime lifecycle, selectors, provider options, terminal fetch
exhaustion, and test migration. A local follow-up review completed after the
revision; it found and the Plan then corrected the remaining Task 7 commit
ownership gap for `web/select.py` and `web/feed.py`. Its final high-priority
verdict was PASS.
