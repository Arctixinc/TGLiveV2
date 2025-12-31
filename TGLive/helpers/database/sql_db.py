from typing import List, Optional, Dict
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

    latest_id = Column(BigInteger, default=0)

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

    async def load(self, chat_id: int | str) -> Optional[Dict]:
        async with self.Session() as session:
            result = await session.execute(
                select(PlaylistTable).where(
                    PlaylistTable.chat_id == int(chat_id)
                )
            )
            row = result.scalar_one_or_none()

            if not row:
                log.debug("load: no data for %s", chat_id)
                return None

            data = {
                "playlist": self._decode_playlist(row.playlist),
                "latest_id": row.latest_id or 0,
                "last_started_id": row.last_started_id,
                "last_completed_id": row.last_completed_id,
                "reverse": row.reverse,
                "channel_name": row.channel_name,
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
        channel_name: Optional[str] = None,
    ):
        if not new_ids:
            return

        new_ids = sorted(set(new_ids))
        new_latest = max(new_ids)

        async with self.Session() as session:
            row = await session.get(PlaylistTable, int(chat_id))

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
                row.latest_id = max(row.latest_id or 0, new_latest)
                row.reverse = reverse
                if channel_name:
                    row.channel_name = channel_name
            else:
                session.add(
                    PlaylistTable(
                        chat_id=int(chat_id),
                        playlist=self._encode_playlist(playlist),
                        latest_id=new_latest,
                        reverse=reverse,
                        channel_name=channel_name,
                    )
                )

            await session.commit()

        log.info(
            "append: chat=%s added=%s latest_id=%s reverse=%s",
            chat_id,
            added,
            new_latest,
            reverse,
        )

    async def remove_video(self, chat_id: int | str, video_id: int):
        async with self.Session() as session:
            row = await session.get(PlaylistTable, int(chat_id))
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

        log.info("remove: chat=%s video=%s", chat_id, video_id)

    async def set_last_started(self, chat_id: int | str, video_id: int):
        async with self.Session() as session:
            row = await session.get(PlaylistTable, int(chat_id))

            if row:
                row.last_started_id = video_id
            else:
                session.add(
                    PlaylistTable(
                        chat_id=int(chat_id),
                        playlist="",
                        latest_id=0,
                        last_started_id=video_id,
                    )
                )

            await session.commit()

        log.info("started: chat=%s video=%s", chat_id, video_id)

    async def set_last_completed(self, chat_id: int | str, video_id: int):
        async with self.Session() as session:
            row = await session.get(PlaylistTable, int(chat_id))

            if row:
                row.last_completed_id = video_id
            else:
                session.add(
                    PlaylistTable(
                        chat_id=int(chat_id),
                        playlist="",
                        latest_id=0,
                        last_completed_id=video_id,
                    )
                )

            await session.commit()

        log.info("completed: chat=%s video=%s", chat_id, video_id)

    async def get_playlist(self, chat_id: int | str) -> List[int]:
        data = await self.load(chat_id)
        if not data:
            return []

        playlist = data["playlist"]

        if data.get("reverse"):
            playlist = playlist[::-1]

        log.debug(
            "get_playlist: chat=%s items=%s latest_id=%s reverse=%s",
            chat_id,
            len(playlist),
            data["latest_id"],
            data["reverse"],
        )

        return playlist
