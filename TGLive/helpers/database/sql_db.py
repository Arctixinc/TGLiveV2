from typing import List, Optional, Tuple
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, BigInteger, Boolean, Text, select
from TGLive import get_logger

log = get_logger(__name__)

Base = declarative_base()


class PlaylistTable(Base):
    __tablename__ = "playlists"

    chat_id = Column(BigInteger, primary_key=True)
    playlist = Column(Text, nullable=False, default="")

    last_started_id = Column(BigInteger, nullable=True)
    last_completed_id = Column(BigInteger, nullable=True)

    reverse = Column(Boolean, default=False)
    channel_name = Column(Text, nullable=True)


class SQLPlaylistStore:
    def __init__(self, db_url: str):
        self.engine = create_async_engine(db_url, echo=False)
        self.Session = sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def init(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        log.info("tables ready")

    @staticmethod
    def _encode_playlist(playlist: List[int]) -> str:
        return ",".join(map(str, playlist))

    @staticmethod
    def _decode_playlist(raw: str) -> List[int]:
        if not raw:
            return []
        return [int(x) for x in raw.split(",") if x]

    async def load(
        self, chat_id: int
    ) -> Tuple[List[int], Optional[int], Optional[int], bool, Optional[str]]:
        async with self.Session() as session:
            result = await session.execute(
                select(PlaylistTable).where(PlaylistTable.chat_id == chat_id)
            )
            row = result.scalar_one_or_none()

            if not row:
                log.debug("load: no data for %s", chat_id)
                return [], None, None, False, None

            log.debug("load: %s items for %s", len(row.playlist.split(",")) if row.playlist else 0, chat_id)

            return (
                self._decode_playlist(row.playlist),
                row.last_started_id,
                row.last_completed_id,
                row.reverse,
                row.channel_name,
            )

    async def append_new(
        self,
        chat_id: int,
        new_ids: List[int],
        reverse: bool = False,
        channel_name: Optional[str] = None,
    ):
        if not new_ids:
            return

        # Mongo-style: sort + dedupe before append
        new_ids = sorted(set(new_ids))

        async with self.Session() as session:
            row = await session.get(PlaylistTable, chat_id)

            if row:
                playlist = self._decode_playlist(row.playlist)
            else:
                playlist = []

            seen = set(playlist)
            added = 0
            for vid in new_ids:
                if vid not in seen:
                    playlist.append(vid)
                    seen.add(vid)
                    added += 1

            if row:
                row.playlist = self._encode_playlist(playlist)
                row.reverse = reverse
                if channel_name:
                    row.channel_name = channel_name
            else:
                session.add(
                    PlaylistTable(
                        chat_id=chat_id,
                        playlist=self._encode_playlist(playlist),
                        reverse=reverse,
                        channel_name=channel_name,
                    )
                )

            await session.commit()

        log.info("append: %s ids to %s (added=%s, reverse=%s)", len(new_ids), chat_id, added, reverse)

    async def remove_video(self, chat_id: int, video_id: int):
        async with self.Session() as session:
            row = await session.get(PlaylistTable, chat_id)
            if not row:
                return

            playlist = self._decode_playlist(row.playlist)
            if video_id not in playlist:
                return

            playlist.remove(video_id)

            if row.last_started_id == video_id:
                row.last_started_id = None
            if row.last_completed_id == video_id:
                row.last_completed_id = None

            row.playlist = self._encode_playlist(playlist)
            await session.commit()

        log.info("remove: %s from %s", video_id, chat_id)

    async def set_last_started(self, chat_id: int, video_id: int):
        async with self.Session() as session:
            row = await session.get(PlaylistTable, chat_id)

            if row:
                row.last_started_id = video_id
            else:
                session.add(
                    PlaylistTable(
                        chat_id=chat_id,
                        playlist="",
                        last_started_id=video_id,
                    )
                )

            await session.commit()

        log.info("started: %s -> %s", chat_id, video_id)

    async def set_last_completed(self, chat_id: int, video_id: int):
        async with self.Session() as session:
            row = await session.get(PlaylistTable, chat_id)

            if row:
                row.last_completed_id = video_id
            else:
                session.add(
                    PlaylistTable(
                        chat_id=chat_id,
                        playlist="",
                        last_completed_id=video_id,
                    )
                )

            await session.commit()

        log.info("completed: %s -> %s", chat_id, video_id)

    async def get_playlist(self, chat_id: int) -> List[int]:
        playlist, _, _, reverse, _ = await self.load(chat_id)

        if reverse:
            playlist = playlist[::-1]

        log.debug(
            "get_playlist: %s items for %s (reverse=%s)",
            len(playlist),
            chat_id,
            reverse,
        )

        return playlist
