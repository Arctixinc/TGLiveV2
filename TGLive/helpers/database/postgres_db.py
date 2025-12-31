import time
import asyncio
import asyncpg
from typing import Optional, List, Dict
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

    async def load(self, chat_id: int | str) -> Optional[Dict]:
        async with await self._acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    playlist,
                    latest_id,
                    reverse,
                    last_started_id,
                    last_completed_id
                FROM playlists
                WHERE chat_id = $1
                """,
                str(chat_id),
            )

            if not row:
                log.debug("load: no data for %s", chat_id)
                return None

            data = {
                "playlist": list(row["playlist"]),
                "latest_id": row["latest_id"] or 0,
                "reverse": row["reverse"],
                "last_started_id": row["last_started_id"],
                "last_completed_id": row["last_completed_id"],
            }

            log.debug(
                "load: chat=%s items=%s latest_id=%s reverse=%s",
                chat_id,
                len(data["playlist"]),
                data["latest_id"],
                data["reverse"],
            )

            return data

    async def append_new(
        self,
        chat_id: int | str,
        new_ids: List[int],
        reverse: bool = False,
    ):
        if not new_ids:
            return

        new_ids = sorted(set(new_ids))
        latest = max(new_ids)

        async with await self._acquire() as conn:
            await conn.execute(
                """
                INSERT INTO playlists
                    (chat_id, playlist, latest_id, reverse, updated_at)
                VALUES
                    ($1, $2, $3, $4, $5)
                ON CONFLICT (chat_id)
                DO UPDATE SET
                    playlist = playlists.playlist ||
                        ARRAY(
                            SELECT e
                            FROM unnest(EXCLUDED.playlist) AS e
                            WHERE NOT e = ANY(playlists.playlist)
                        ),
                    latest_id = GREATEST(playlists.latest_id, EXCLUDED.latest_id),
                    reverse = EXCLUDED.reverse,
                    updated_at = EXCLUDED.updated_at
                """,
                str(chat_id),
                new_ids,
                latest,
                reverse,
                int(time.time()),
            )

        log.info(
            "append: chat=%s added=%s latest_id=%s reverse=%s",
            chat_id,
            len(new_ids),
            latest,
            reverse,
        )

    async def remove_video(self, chat_id: int | str, message_id: int):
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

        log.info("remove: chat=%s video=%s", chat_id, message_id)

    async def set_last_started(self, chat_id: int | str, message_id: int):
        async with await self._acquire() as conn:
            await conn.execute(
                """
                INSERT INTO playlists
                    (chat_id, last_started_id, updated_at)
                VALUES
                    ($1, $2, $3)
                ON CONFLICT (chat_id)
                DO UPDATE SET
                    last_started_id = EXCLUDED.last_started_id,
                    updated_at = EXCLUDED.updated_at
                """,
                str(chat_id),
                message_id,
                int(time.time()),
            )

        log.info("started: chat=%s video=%s", chat_id, message_id)

    async def set_last_completed(self, chat_id: int | str, message_id: int):
        async with await self._acquire() as conn:
            await conn.execute(
                """
                INSERT INTO playlists
                    (chat_id, last_completed_id, updated_at)
                VALUES
                    ($1, $2, $3)
                ON CONFLICT (chat_id)
                DO UPDATE SET
                    last_completed_id = EXCLUDED.last_completed_id,
                    updated_at = EXCLUDED.updated_at
                """,
                str(chat_id),
                message_id,
                int(time.time()),
            )

        log.info("completed: chat=%s video=%s", chat_id, message_id)

    async def get_playlist(self, chat_id: int | str) -> List[int]:
        row = await self.load(chat_id)
        if not row:
            return []

        playlist = row["playlist"]

        if row.get("reverse"):
            playlist = playlist[::-1]

        log.debug(
            "get playlist: chat=%s items=%s latest_id=%s reverse=%s",
            chat_id,
            len(playlist),
            row["latest_id"],
            row["reverse"],
        )

        return playlist
