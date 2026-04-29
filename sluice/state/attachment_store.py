import hashlib
import mimetypes
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path

import aiofiles
import aiosqlite

from sluice.url_canon import canonical_url


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ext_from_mime(mime: str | None) -> str:
    if not mime:
        return "bin"
    mime = mime.split(";", 1)[0].strip().lower()
    overrides = {
        "image/jpeg": "jpg",
        "image/svg+xml": "svg",
        "text/plain": "txt",
    }
    ext = overrides.get(mime) or mimetypes.guess_extension(mime)
    return ext.lstrip(".") if ext else "bin"


class AttachmentStore:
    def __init__(self, db: aiosqlite.Connection, base_dir: Path):
        self._db = db
        self._base = Path(base_dir)

    async def put_bytes(
        self, *, url: str, mime_type: str | None, data: bytes, pipeline_id: str
    ) -> str:
        return await self._put_bytes_once(
            url=url,
            mime_type=mime_type,
            data=data,
            pipeline_id=pipeline_id,
            retry=0,
        )

    async def _put_bytes_once(self, *, url, mime_type, data, pipeline_id, retry: int) -> str:
        if retry > 1:
            raise RuntimeError(
                f"attachment mirror retry exhausted for {url!r} "
                f"(race or repeated file-missing condition)"
            )
        canon = canonical_url(url)
        url_hash = hashlib.sha256(canon.encode("utf-8")).hexdigest()
        ext = _ext_from_mime(mime_type)
        rel_dir = f"{datetime.now(timezone.utc).strftime('%Y/%m')}/{url_hash[:2]}"
        rel_path = f"{rel_dir}/{url_hash}.{ext}"
        abs_dir = self._base / rel_dir
        abs_dir.mkdir(parents=True, exist_ok=True)
        final_path = self._base / rel_path
        tmp_path = abs_dir / f".{url_hash}.tmp.{secrets.token_hex(4)}"

        async with aiofiles.open(tmp_path, "wb") as f:
            await f.write(data)

        try:
            await self._db.execute("BEGIN")
            cur = await self._db.execute(
                "INSERT OR IGNORE INTO attachment_mirror "
                "(url_hash, url, local_path, mime_type, byte_size, "
                " fetched_at, last_referenced_at, pipeline_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    url_hash,
                    canon,
                    rel_path,
                    mime_type,
                    len(data),
                    _now_iso(),
                    _now_iso(),
                    pipeline_id,
                ),
            )
            won = cur.rowcount == 1
            if won:
                try:
                    os.replace(tmp_path, final_path)
                except OSError:
                    await self._db.rollback()
                    if tmp_path.exists():
                        tmp_path.unlink()
                    raise
                try:
                    await self._db.commit()
                except Exception:
                    from loguru import logger as _log

                    cur = await self._db.execute(
                        "SELECT 1 FROM attachment_mirror WHERE url_hash = ?",
                        (url_hash,),
                    )
                    persisted = await cur.fetchone()
                    if not persisted:
                        _log.error(
                            f"attachment mirror commit failed but file "
                            f"already moved to {final_path}; left as orphan "
                            f"for next gc orphan_sweep"
                        )
                    raise
                return rel_path

            await self._db.rollback()
            if tmp_path.exists():
                tmp_path.unlink()
            cur = await self._db.execute(
                "SELECT local_path FROM attachment_mirror WHERE url_hash = ?",
                (url_hash,),
            )
            row = await cur.fetchone()
            if row is None:
                return await self._put_bytes_once(
                    url=url,
                    mime_type=mime_type,
                    data=data,
                    pipeline_id=pipeline_id,
                    retry=retry + 1,
                )
            existing_path = row[0]
            if not (self._base / existing_path).is_file():
                await self._db.execute(
                    "DELETE FROM attachment_mirror WHERE url_hash = ?",
                    (url_hash,),
                )
                await self._db.commit()
                return await self._put_bytes_once(
                    url=url,
                    mime_type=mime_type,
                    data=data,
                    pipeline_id=pipeline_id,
                    retry=retry + 1,
                )
            await self._db.execute(
                "UPDATE attachment_mirror SET last_referenced_at = ? WHERE url_hash = ?",
                (_now_iso(), url_hash),
            )
            await self._db.commit()
            return existing_path
        except BaseException:
            try:
                await self._db.rollback()
            except Exception:
                pass
            if tmp_path.exists():
                tmp_path.unlink()
            raise

    async def lookup(self, url: str) -> str | None:
        canon = canonical_url(url)
        url_hash = hashlib.sha256(canon.encode("utf-8")).hexdigest()
        cur = await self._db.execute(
            "SELECT local_path FROM attachment_mirror WHERE url_hash = ?",
            (url_hash,),
        )
        row = await cur.fetchone()
        return row[0] if row else None
