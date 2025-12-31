import os
import json
import time
import asyncio
from typing import Optional, List


class JsonPlaylistStore:
    def __init__(self, file_path: str = "playlists.json"):
        self.file_path = file_path
        self._lock = asyncio.Lock()

        if not os.path.exists(self.file_path):
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump({}, f)

    def _key(self, chat_id: int | str) -> str:
        return f"channel_{chat_id}"

    async def _load_all(self) -> dict:
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    async def _save_all(self, data: dict):
        tmp = self.file_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.file_path)

    async def load(self, chat_id: int | str) -> Optional[dict]:
        async with self._lock:
            data = await self._load_all()
            return data.get(self._key(chat_id))

    async def append_new(
        self,
        chat_id: int | str,
        new_ids: List[int],
        reverse: bool = False,
        channel_name: Optional[str] = None,
    ):
        if not new_ids:
            return

        async with self._lock:
            data = await self._load_all()
            key = self._key(chat_id)

            entry = data.get(key, {
                "chat_id": chat_id,
                "playlist": [],
                "last_started_id": None,
                "last_completed_id": None,
            })

            playlist = entry["playlist"]

            seen = set(playlist)
            for vid in sorted(set(new_ids)):
                if vid not in seen:
                    playlist.append(vid)
                    seen.add(vid)

            entry["playlist"] = playlist
            entry["reverse"] = reverse
            entry["updated_at"] = int(time.time())

            if channel_name:
                entry["channel_name"] = channel_name

            data[key] = entry
            await self._save_all(data)

    async def remove_video(self, chat_id: int | str, video_id: int):
        async with self._lock:
            data = await self._load_all()
            key = self._key(chat_id)

            entry = data.get(key)
            if not entry:
                return

            entry["playlist"] = [
                x for x in entry.get("playlist", []) if x != video_id
            ]

            if entry.get("last_started_id") == video_id:
                entry["last_started_id"] = None
            if entry.get("last_completed_id") == video_id:
                entry["last_completed_id"] = None

            entry["updated_at"] = int(time.time())
            await self._save_all(data)

    async def set_last_started(self, chat_id: int | str, video_id: int):
        async with self._lock:
            data = await self._load_all()
            key = self._key(chat_id)

            entry = data.setdefault(key, {
                "chat_id": chat_id,
                "playlist": [],
                "last_completed_id": None,
            })

            entry["last_started_id"] = video_id
            entry["updated_at"] = int(time.time())

            await self._save_all(data)

    async def set_last_completed(self, chat_id: int | str, video_id: int):
        async with self._lock:
            data = await self._load_all()
            key = self._key(chat_id)

            entry = data.setdefault(key, {
                "chat_id": chat_id,
                "playlist": [],
            })

            entry["last_completed_id"] = video_id
            entry["updated_at"] = int(time.time())

            await self._save_all(data)

    async def get_playlist(self, chat_id: int | str) -> List[int]:
        async with self._lock:
            data = await self._load_all()
            entry = data.get(self._key(chat_id))
            if not entry:
                return []

            playlist = entry.get("playlist", [])

            if entry.get("reverse", False):
                return playlist[::-1]

            return playlist
