from datetime import datetime, timezone
from pathlib import Path

import aiosqlite


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def gc_failed_items(
    db: aiosqlite.Connection, *, older_than_iso: str, pipeline: str | None = None
) -> int:
    sql = (
        "DELETE FROM failed_items "
        "WHERE status IN ('resolved', 'dead_letter') "
        "AND last_failed_at < ?"
    )
    args: list = [older_than_iso]
    if pipeline:
        sql += " AND pipeline_id = ?"
        args.append(pipeline)
    cur = await db.execute(sql, args)
    n = cur.rowcount
    criteria = f"status IN (resolved,dead_letter) AND last_failed_at < {older_than_iso}" + (
        f" AND pipeline_id={pipeline}" if pipeline else ""
    )
    await db.execute(
        "INSERT INTO gc_log (ran_at, table_name, rows_deleted, criteria) VALUES (?, ?, ?, ?)",
        (_now_iso(), "failed_items", n, criteria),
    )
    await db.commit()
    return n


async def gc_url_cache(db: aiosqlite.Connection, *, max_rows: int, keep_expired_iso: str) -> int:
    cur = await db.execute("DELETE FROM url_cache WHERE expires_at < ?", (keep_expired_iso,))
    n_exp = cur.rowcount
    cur2 = await db.execute("SELECT COUNT(*) FROM url_cache")
    total = (await cur2.fetchone())[0]
    n_size = 0
    if total > max_rows:
        excess = total - max_rows
        cur3 = await db.execute(
            "DELETE FROM url_cache WHERE url_hash IN ("
            "  SELECT url_hash FROM url_cache "
            "  ORDER BY fetched_at ASC LIMIT ?)",
            (excess,),
        )
        n_size = cur3.rowcount
    await db.execute(
        "INSERT INTO gc_log (ran_at, table_name, rows_deleted, criteria) VALUES (?, ?, ?, ?)",
        (
            _now_iso(),
            "url_cache",
            n_exp + n_size,
            f"expired<{keep_expired_iso} or excess over {max_rows}",
        ),
    )
    await db.commit()
    return n_exp + n_size


async def gc_attachments(
    db: aiosqlite.Connection, *, base_dir: Path, older_than_iso: str, pipeline: str | None = None
) -> int:
    sel_sql = "SELECT url_hash, local_path FROM attachment_mirror WHERE last_referenced_at < ?"
    args: list = [older_than_iso]
    if pipeline:
        sel_sql += " AND pipeline_id = ?"
        args.append(pipeline)
    cur = await db.execute(sel_sql, args)
    rows = await cur.fetchall()
    n = len(rows)
    criteria = f"last_referenced_at < {older_than_iso}" + (
        f" AND pipeline_id={pipeline}" if pipeline else ""
    )
    if n == 0:
        await db.execute(
            "INSERT INTO gc_log (ran_at, table_name, rows_deleted, criteria) VALUES (?, ?, ?, ?)",
            (_now_iso(), "attachment_mirror", 0, criteria),
        )
        await db.commit()
        await orphan_sweep(db, base_dir)
        return 0

    await db.execute("BEGIN")
    try:
        for url_hash, _ in rows:
            await db.execute(
                "DELETE FROM attachment_mirror WHERE url_hash = ?",
                (url_hash,),
            )
        await db.execute(
            "INSERT INTO gc_log (ran_at, table_name, rows_deleted, criteria) VALUES (?, ?, ?, ?)",
            (_now_iso(), "attachment_mirror", n, criteria),
        )
        await db.commit()
    except Exception:
        await db.rollback()
        raise

    for _, local_path in rows:
        p = base_dir / local_path
        try:
            if p.is_file():
                p.unlink()
        except OSError:
            pass
    await orphan_sweep(db, base_dir)
    return n


async def orphan_sweep(db: aiosqlite.Connection, base_dir: Path) -> int:
    cur = await db.execute("SELECT local_path FROM attachment_mirror")
    known = {r[0] for r in await cur.fetchall()}
    n = 0
    if base_dir.exists():
        for year_dir in base_dir.iterdir():
            if not year_dir.is_dir():
                continue
            for month_dir in year_dir.iterdir():
                if not month_dir.is_dir():
                    continue
                for shard_dir in month_dir.iterdir():
                    if not shard_dir.is_dir():
                        continue
                    for f in shard_dir.iterdir():
                        if not f.is_file():
                            continue
                        rel = str(f.relative_to(base_dir)).replace("\\", "/")
                        if rel not in known:
                            try:
                                f.unlink()
                                n += 1
                            except OSError:
                                pass
    await db.execute(
        "INSERT INTO gc_log (ran_at, table_name, rows_deleted, criteria) VALUES (?, ?, ?, ?)",
        (
            _now_iso(),
            "attachment_orphans",
            n,
            "files in attachment_dir not referenced by any attachment_mirror.local_path",
        ),
    )
    await db.commit()
    return n
