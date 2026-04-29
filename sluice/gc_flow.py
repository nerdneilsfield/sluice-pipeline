from datetime import datetime, timezone
from pathlib import Path

from prefect import flow

from sluice.gc import gc_attachments, gc_failed_items, gc_url_cache
from sluice.state.db import open_db
from sluice.window import parse_duration


@flow(name="sluice-gc")
async def sluice_gc_flow(config_dir: str) -> dict[str, int]:
    from sluice.loader import load_all

    bundle = load_all(Path(config_dir))
    cfg = bundle.global_cfg
    counts: dict[str, int] = {}
    cutoff_failed = (
        datetime.now(timezone.utc) - parse_duration(cfg.gc.failed_items_older_than)
    ).isoformat(timespec="seconds")
    cutoff_cache = (
        datetime.now(timezone.utc) - parse_duration(cfg.gc.url_cache_keep_expired)
    ).isoformat(timespec="seconds")
    cutoff_att = (
        datetime.now(timezone.utc) - parse_duration(cfg.gc.attachment_unreferenced_after)
    ).isoformat(timespec="seconds")
    att_dir = cfg.state.attachment_dir or "./data/attachments"
    if cfg.state.db_path is None:
        raise RuntimeError("sluice-gc requires global [state].db_path to be set")
    async with open_db(cfg.state.db_path) as db:
        counts["failed_items"] = await gc_failed_items(db, older_than_iso=cutoff_failed)
        counts["url_cache"] = await gc_url_cache(
            db, max_rows=cfg.gc.url_cache_max_rows, keep_expired_iso=cutoff_cache
        )
        counts["attachment_mirror"] = await gc_attachments(
            db, base_dir=Path(att_dir), older_than_iso=cutoff_att
        )
    return counts
