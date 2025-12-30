import json
from pathlib import Path
from asyncio import Lock


class JsonPlaylistStore:
    def __init__(self, file_path: str):
        self.path = Path(file_path)
        self.lock = Lock()

        if not self.path.exists():
            self.path.write_text(json.dumps({}))

    async def _read(self):
        return json.loads(self.path.read_text())

    async def _write(self, data):
        self.path.write_text(json.dumps(data, indent=2))

    async def load(self, chat_id: int):
        async with self.lock:
            data = await self._read()
            row = data.get(str(chat_id), {})
            return (
                row.get("playlist", []),
                row.get("latest_id"),
                row.get("reverse", False),
                row.get("channel_name"),
            )

    async def save(self, chat_id, playlist, latest_id, reverse, channel_name=None):
        async with self.lock:
            data = await self._read()
            data[str(chat_id)] = {
                "playlist": playlist,
                "latest_id": latest_id,
                "reverse": reverse,
                "channel_name": channel_name,
            }
            await self._write(data)

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
        playlist, _, reverse, channel_name = await self.load(chat_id)
        await self.save(chat_id, playlist, video_id, reverse, channel_name)
