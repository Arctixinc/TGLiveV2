from typing import List, Optional
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, BigInteger, Boolean, Text, select

Base = declarative_base()


class PlaylistTable(Base):
    __tablename__ = "playlists"

    chat_id = Column(BigInteger, primary_key=True)
    playlist = Column(Text)
    latest_id = Column(BigInteger)
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

    async def load(self, chat_id: int):
        async with self.Session() as session:
            result = await session.execute(
                select(PlaylistTable).where(PlaylistTable.chat_id == chat_id)
            )
            row = result.scalar_one_or_none()

            if not row:
                return [], None, False, None

            return (
                row.playlist.split(",") if row.playlist else [],
                row.latest_id,
                row.reverse,
                row.channel_name,
            )

    async def save(
        self,
        chat_id: int,
        playlist: List[int],
        latest_id: int,
        reverse: bool,
        channel_name: Optional[str] = None,
    ):
        async with self.Session() as session:
            row = await session.get(PlaylistTable, chat_id)
            data = {
                "playlist": ",".join(map(str, playlist)),
                "latest_id": latest_id,
                "reverse": reverse,
                "channel_name": channel_name,
            }

            if row:
                for k, v in data.items():
                    setattr(row, k, v)
            else:
                session.add(PlaylistTable(chat_id=chat_id, **data))

            await session.commit()

    async def append_new(self, chat_id, new_ids, latest_id=None, reverse=False):
        playlist, _, _, channel_name = await self.load(chat_id)
        playlist.extend(new_ids)
        await self.save(chat_id, playlist, latest_id, reverse, channel_name)

    async def remove_video(self, chat_id, video_id):
        playlist, latest_id, reverse, channel_name = await self.load(chat_id)
        if video_id in playlist:
            playlist.remove(video_id)
            await self.save(chat_id, playlist, latest_id, reverse, channel_name)

    async def set_last_started(self, chat_id, video_id):
        playlist, _, reverse, channel_name = await self.load(chat_id)
        await self.save(chat_id, playlist, video_id, reverse, channel_name)

    async def set_last_completed(self, chat_id, video_id):
        await self.set_last_started(chat_id, video_id)
