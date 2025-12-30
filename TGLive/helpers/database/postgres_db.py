import time
import asyncio
import asyncpg
from typing import Optional, List


class PostgresPlaylistStore:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.pool: asyncpg.Pool | None = None
        self._lock = asyncio.Lock()

    async def connect(self):
        async with self._lock:
            if self.pool and not self.pool._closed:
                return

            for _ in range(3):
                try:
                    self.pool = await asyncpg.create_pool(
                        self.db_url,
                        min_size=1,
                        max_size=5,
                        timeout=10,
                    )
                    return
                except Exception:
                    await asyncio.sleep(2)

            raise RuntimeError("PostgreSQL connection failed")

    async def _acquire(self):
        await self.connect()
        return self.pool.acquire()

    async def load(self, chat_id: int | str) -> Optional[dict]:
        async with await self._acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT playlist, last_started_id, last_completed_id
                FROM playlists
                WHERE chat_id=$1
                """,
                str(chat_id),
            )

            if not row:
                return None

            return {
                "playlist": list(row["playlist"]),
                "last_started_id": row["last_started_id"],
                "last_completed_id": row["last_completed_id"],
            }

    async def append_new(self, chat_id, new_ids: List[int]):
        if not new_ids:
            return

        async with await self._acquire() as conn:
            await conn.execute(
                """
                INSERT INTO playlists (chat_id, playlist, updated_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (chat_id)
                DO UPDATE SET
                    playlist = (
                        SELECT ARRAY(
                            SELECT DISTINCT e
                            FROM unnest(playlists.playlist || EXCLUDED.playlist) AS e
                        )
                    ),
                    updated_at = EXCLUDED.updated_at
                """,
                str(chat_id),
                new_ids,
                int(time.time()),
            )

    async def remove_video(self, chat_id, message_id: int):
        async with await self._acquire() as conn:
            await conn.execute(
                """
                UPDATE playlists
                SET
                    playlist = array_remove(playlist, $2),
                    last_started_id = CASE
                        WHEN last_started_id = $2 THEN NULL
                        ELSE last_started_id
                    END,
                    last_completed_id = CASE
                        WHEN last_completed_id = $2 THEN NULL
                        ELSE last_completed_id
                    END,
                    updated_at = $3
                WHERE chat_id = $1
                """,
                str(chat_id),
                message_id,
                int(time.time()),
            )

    async def set_last_started(self, chat_id, message_id: int):
        async with await self._acquire() as conn:
            await conn.execute(
                """
                INSERT INTO playlists (chat_id, last_started_id, updated_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (chat_id)
                DO UPDATE SET
                    last_started_id = EXCLUDED.last_started_id,
                    updated_at = EXCLUDED.updated_at
                """,
                str(chat_id),
                message_id,
                int(time.time()),
            )

    async def set_last_completed(self, chat_id, message_id: int):
        async with await self._acquire() as conn:
            await conn.execute(
                """
                INSERT INTO playlists (chat_id, last_completed_id, updated_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (chat_id)
                DO UPDATE SET
                    last_completed_id = EXCLUDED.last_completed_id,
                    updated_at = EXCLUDED.updated_at
                """,
                str(chat_id),
                message_id,
                int(time.time()),
            )