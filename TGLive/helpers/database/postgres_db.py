import time
import asyncio
import asyncpg
from typing import Optional, List
from TGLive import get_logger

log = get_logger(__name__)


class PostgresPlaylistStore:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.pool: asyncpg.Pool | None = None
        self._lock = asyncio.Lock()

    async def connect(self):
        async with self._lock:
            if self.pool and not self.pool._closed:
                return

            for attempt in range(3):
                try:
                    log.info("connecting (attempt %s)", attempt + 1)
                    self.pool = await asyncpg.create_pool(
                        self.db_url,
                        min_size=1,
                        max_size=5,
                        timeout=10,
                    )
                    log.info("connected")
                    return
                except Exception as e:
                    log.warning("connect failed: %s", e)
                    await asyncio.sleep(2)

            log.error("connection failed permanently")
            raise RuntimeError("PostgreSQL connection failed")

    async def _acquire(self):
        await self.connect()
        return self.pool.acquire()

    async def load(self, chat_id: int | str) -> Optional[dict]:
        async with await self._acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT playlist, reverse, last_started_id, last_completed_id
                FROM playlists
                WHERE chat_id=$1
                """,
                str(chat_id),
            )

            if not row:
                log.debug("load: no data for %s", chat_id)
                return None

            log.debug("load: %s items for %s", len(row["playlist"]), chat_id)
            return {
                "playlist": list(row["playlist"]),
                "reverse": row["reverse"],
                "last_started_id": row["last_started_id"],
                "last_completed_id": row["last_completed_id"],
            }

    async def append_new(
        self,
        chat_id: int | str,
        new_ids: List[int],
        reverse: bool = False,
    ):
        if not new_ids:
            return

        new_ids = sorted(set(new_ids))

        async with await self._acquire() as conn:
            await conn.execute(
                """
                INSERT INTO playlists (chat_id, playlist, reverse, updated_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (chat_id)
                DO UPDATE SET
                    playlist = playlists.playlist ||
                        ARRAY(
                            SELECT e
                            FROM unnest(EXCLUDED.playlist) AS e
                            WHERE NOT e = ANY(playlists.playlist)
                        ),
                    reverse = EXCLUDED.reverse,
                    updated_at = EXCLUDED.updated_at
                """,
                str(chat_id),
                new_ids,
                reverse,
                int(time.time()),
            )

        log.info("append: %s ids to %s (reverse=%s)", len(new_ids), chat_id, reverse)

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

        log.info("remove: %s from %s", message_id, chat_id)

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

        log.info("started: %s -> %s", chat_id, message_id)

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

        log.info("completed: %s -> %s", chat_id, message_id)

    async def get_playlist(self, chat_id: int | str) -> List[int]:
        row = await self.load(chat_id)
        if not row:
            return []

        playlist = row["playlist"]

        if row.get("reverse"):
            playlist = playlist[::-1]

        log.debug(
            "get playlist: %s items for %s (reverse=%s)",
            len(playlist),
            chat_id,
            row.get("reverse"),
        )

        return playlist
