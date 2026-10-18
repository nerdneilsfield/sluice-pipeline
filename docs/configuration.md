# Write a definition

This page collects the definition rules that go beyond the overview in the [README](../README.md#pipeline-concepts).

## Task chains

Web fetch Tasks produce transient artifacts; end their chain with `sluice.tasks.to_markdown` or `sluice.tasks.to_text` before materialization.

## Binding identity

When a provider credential is supplied as an environment reference, changing the secret's value does not change the authored binding identity.

## File output root

The optional top-level key `file_root` confines the `directory` of every `sluice.sinks.file` Sink. It is relative to the definition file's directory, which is also the default, or absolute. For the rules and a Docker mount example, see [`file_root` and Docker](sources-and-sinks.md#file_root-and-docker).

## Upgrading from 0.4.0

Built-in Sink credentials must now be exact `env:NAME` references: Telegram `bot_token`, email `password`, Feishu `webhook` and `secret`, and `sluice.sinks.webhook` `url`. A definition with a literal value fails to load with an error that names the key, not the value. Move the value into an environment variable and reference it.

Rows already stored in an existing database are not rewritten. Delivery configuration retained from a 0.4.0 run still contains whatever literal was authored then, so rotate any such secret and delete the old database rows if the file is shared.

Template fields that hold a list or an object now render as compact JSON, for example `["a","b"]`, instead of a Python representation such as `('a', 'b')`. The rendered text is part of the rendered Item's identity, so a template that references such a field produces new Items after the upgrade, and each new Item gets a new Delivery: affected messages may be sent once more. Templates that reference only strings, numbers, or indexed members like `{author[name]}` are unaffected.

The Docker image no longer runs as root. It runs as uid/gid 10001 (`sluice`), so a host directory mounted at `/app/data` that was created by an earlier root-run container must be handed over: `sudo chown -R 10001:10001 ./data`, or run the container with `--user "$(id -u):$(id -g)"`.

## Design contracts and custom callables

Configuration and extension contracts are documented in the [core design](superpowers/specs/2026-08-14-sluice-v4-cleanroom-design.md) and [web design](superpowers/specs/2026-08-18-sluice-v4-web-content-pipeline-design.md).

Custom dotted-path callables must be importable when the App starts.
