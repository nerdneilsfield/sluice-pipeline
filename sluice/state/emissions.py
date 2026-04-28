from dataclasses import dataclass
from datetime import datetime, timezone
import aiosqlite

@dataclass
class Emission:
    sink_id: str
    sink_type: str
    external_id: str
    emitted_at: str

class EmissionStore:
    def __init__(self, db: aiosqlite.Connection): self.db = db

    async def lookup(self, pipeline_id, run_key, sink_id) -> Emission | None:
        async with self.db.execute(
            "SELECT sink_id, sink_type, external_id, emitted_at "
            "FROM sink_emissions "
            "WHERE pipeline_id=? AND run_key=? AND sink_id=?",
            (pipeline_id, run_key, sink_id),
        ) as cur:
            row = await cur.fetchone()
        return Emission(*row) if row else None

    async def insert(self, pipeline_id, run_key, sink_id, sink_type, external_id):
        await self.db.execute(
            "INSERT OR REPLACE INTO sink_emissions "
            "(pipeline_id, run_key, sink_id, sink_type, external_id, emitted_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (pipeline_id, run_key, sink_id, sink_type, external_id,
             datetime.now(timezone.utc).isoformat()),
        )
        await self.db.commit()
