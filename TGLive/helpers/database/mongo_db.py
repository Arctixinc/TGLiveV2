from motor.motor_asyncio import AsyncIOMotorClient


class MongoPlaylistStore:
    def __init__(self, mongo_uri: str, db_name: str):
        self.client = AsyncIOMotorClient(mongo_uri)
        self.db = self.client[db_name]
        self.col = self.db.playlists

    async def load(self, chat_id: int):
        row = await self.col.find_one({"_id": chat_id})
        if not row:
            return [], None, False, None

        return (
            row.get("playlist", []),
            row.get("latest_id"),
            row.get("reverse", False),
            row.get("channel_name"),
        )

    async def save(self, chat_id, playlist, latest_id, reverse, channel_name=None):
        await self.col.update_one(
            {"_id": chat_id},
            {
                "$set": {
                    "playlist": playlist,
                    "latest_id": latest_id,
                    "reverse": reverse,
                    "channel_name": channel_name,
                }
            },
            upsert=True,
        )

    async def append_new(self, chat_id, new_ids, latest_id=None, reverse=False):
        await self.col.update_one(
            {"_id": chat_id},
            {"$push": {"playlist": {"$each": new_ids}}},
            upsert=True,
        )

    async def remove_video(self, chat_id, video_id):
        await self.col.update_one(
            {"_id": chat_id},
            {"$pull": {"playlist": video_id}},
        )

    async def set_last_started(self, chat_id, video_id):
        await self.col.update_one(
            {"_id": chat_id},
            {"$set": {"latest_id": video_id}},
            upsert=True,
        )

    async def set_last_completed(self, chat_id, video_id):
        await self.set_last_started(chat_id, video_id)
