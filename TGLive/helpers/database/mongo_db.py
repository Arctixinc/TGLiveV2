import time
from typing import Optional, Tuple, List
from motor.motor_asyncio import AsyncIOMotorClient


class MongoPlaylistStore:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = AsyncIOMotorClient(mongo_uri)
        self.db = self.client[db_name]
        self.col = self.db.playlists

        self.col.create_index("_id", unique=True)

    async def load(
        self, chat_id: int
    ) -> Tuple[List[int], Optional[int], Optional[int], bool, Optional[str]]:
        row = await self.col.find_one({"_id": chat_id})
        if not row:
            return [], None, None, False, None

        return (
            row.get("playlist", []),
            row.get("last_started_id"),
            row.get("last_completed_id"),
            row.get("reverse", False),
            row.get("channel_name"),
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

        update = {
            "$addToSet": {"playlist": {"$each": new_ids}},
            "$set": {
                "reverse": reverse,
                "updated_at": int(time.time()),
            },
            "$setOnInsert": {
                "last_started_id": None,
                "last_completed_id": None,
            },
        }

        if channel_name:
            update["$set"]["channel_name"] = channel_name

        await self.col.update_one(
            {"_id": chat_id},
            update,
            upsert=True,
        )

    async def remove_video(self, chat_id: int, video_id: int):
        await self.col.update_one(
            {"_id": chat_id},
            {
                "$pull": {"playlist": video_id},
                "$set": {"updated_at": int(time.time())},
            },
        )

    async def set_last_started(self, chat_id: int, video_id: int):
        await self.col.update_one(
            {"_id": chat_id},
            {
                "$set": {
                    "last_started_id": video_id,
                    "updated_at": int(time.time()),
                },
                "$setOnInsert": {
                    "playlist": [],
                    "last_completed_id": None,
                },
            },
            upsert=True,
        )

    async def set_last_completed(self, chat_id: int, video_id: int):
        await self.col.update_one(
            {"_id": chat_id},
            {
                "$set": {
                    "last_completed_id": video_id,
                    "updated_at": int(time.time()),
                },
                "$setOnInsert": {
                    "playlist": [],
                },
            },
            upsert=True,
        )