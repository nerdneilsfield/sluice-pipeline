# Write a definition

This page collects the definition rules that go beyond the overview in the [README](../README.md#pipeline-concepts).

## Task chains

Web fetch Tasks produce transient artifacts; end their chain with `sluice.tasks.to_markdown` or `sluice.tasks.to_text` before materialization.

## Binding identity

When a provider credential is supplied as an environment reference, changing the secret's value does not change the authored binding identity.

## Design contracts and custom callables

Configuration and extension contracts are documented in the [core design](superpowers/specs/2026-08-14-sluice-v4-cleanroom-design.md) and [web design](superpowers/specs/2026-08-18-sluice-v4-web-content-pipeline-design.md).

Custom dotted-path callables must be importable when the App starts.
