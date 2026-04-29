# Changelog

## 0.2.0 — 2026-04-29 (sluice "v1.1" milestone)

### Added
- `sluice gc` command for reclaiming `failed_items`, `url_cache`, `attachment_mirror`.
- Sub-daily pipelines: `run_key_template` with `{run_hour}`, `{run_minute}`, `{run_iso}`, `{run_epoch}`.
- `mirror_attachments` stage with three URL-prefix modes (`""`, `file://`, `https://`).
- `limit` stage processor with `sort_by`, `group_by`, `per_group_max`.
- `enrich` processor type + `hn_comments` enricher (via hckrnws.com).
- `field_filter` ops: `lower`, `strip`, `regex_replace`.
- Fetcher chain `on_all_failed = "use_raw_summary"`.
- New sinks: `telegram`, `feishu`, `email` (all `create_new`-only with `sink_delivery_log` audit).
- Custom Prometheus collector + `sluice stats` + `sluice metrics-server` + `sluice deliveries`.
- New schema (`user_version 2`): `attachment_mirror`, `gc_log`, `sink_delivery_log`.

### Changed
- `lookback_overlap` default formula (now `max(min(1h, w*0.5), w*0.2)`).
- `Attachment` dataclass gained `local_path: str | None` field.

### Tooling
- New extras: `[telegram]`, `[feishu]`, `[email]`, `[channels]`, `[metrics]`, `[enrich-hn]`, `[all]`.
