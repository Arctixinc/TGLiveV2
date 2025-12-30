import os
import json
import time
import asyncio
from typing import Optional


class JsonPlaylistStore:
    def __init__(self, file_path: str = "playlists.json"):
        self.file_path = file_path
        self._lock = asyncio.Lock()

        if not os.path.exists(self.file_path):
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump({}, f)

    def _key(self, chat_id: int | str) -> str:
        return f"channel_{chat_id}"

    async def _load_all_unlocked(self) -> dict:
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    async def _save_all_unlocked(self, data: dict):
        tmp = self.file_path + ".tmp"

        def format_entry(entry: dict, indent: int = 2) -> str:
            lines = ["{"]
            pad = " " * indent

            items = list(entry.items())
            for i, (k, v) in enumerate(items):
                comma = "," if i < len(items) - 1 else ""

                if k == "playlist":
                    line = f'{pad}"{k}": {json.dumps(v, separators=(",", ":"))}{comma}'
                else:
                    line = f'{pad}"{k}": {json.dumps(v, ensure_ascii=False)}{comma}'

                lines.append(line)

            lines.append("}")
            return "\n".join(lines)

        with open(tmp, "w", encoding="utf-8") as f:
            f.write("{\n")
            keys = list(data.keys())

            for i, key in enumerate(keys):
                comma = "," if i < len(keys) - 1 else ""
                entry = format_entry(data[key], indent=4)
                f.write(f'  "{key}": {entry}{comma}\n')

            f.write("}")

        os.replace(tmp, self.file_path)

    async def load(self, chat_id: int | str) -> Optional[dict]:
        async with self._lock:
            data = await self._load_all_unlocked()
            return data.get(self._key(chat_id))

    async def append_new(
        self,
        chat_id,
        new_ids,
        reverse: bool,
        channel_name: str | None = None,
    ):
        if not new_ids:
            return

        async with self._lock:
            data = await self._load_all_unlocked()
            key = self._key(chat_id)

            entry = data.get(key, {
                "chat_id": chat_id,
                "playlist": [],
                "last_started_id": None,
                "last_completed_id": None,
            })

            playlist = entry["playlist"]

            seen = set(playlist)
            for vid in new_ids:
                if vid not in seen:
                    playlist.append(vid)
                    seen.add(vid)

            entry.update({
                "playlist": playlist,
                "reverse": reverse,
                "updated_at": int(time.time()),
            })

            if channel_name:
                entry["channel_name"] = channel_name

            data[key] = entry
            await self._save_all_unlocked(data)

    async def remove_video(self, chat_id, message_id: int):
        async with self._lock:
            data = await self._load_all_unlocked()
            key = self._key(chat_id)

            entry = data.get(key)
            if not entry:
                return

            entry["playlist"] = [
                x for x in entry.get("playlist", []) if x != message_id
            ]

            if entry.get("last_started_id") == message_id:
                entry["last_started_id"] = None
            if entry.get("last_completed_id") == message_id:
                entry["last_completed_id"] = None

            entry["updated_at"] = int(time.time())
            await self._save_all_unlocked(data)

    async def set_last_started(self, chat_id, message_id: int):
        async with self._lock:
            data = await self._load_all_unlocked()
            key = self._key(chat_id)

            entry = data.setdefault(key, {
                "chat_id": chat_id,
                "playlist": [],
                "last_completed_id": None,
            })

            entry["last_started_id"] = message_id
            entry["updated_at"] = int(time.time())

            await self._save_all_unlocked(data)

    async def set_last_completed(self, chat_id, message_id: int):
        async with self._lock:
            data = await self._load_all_unlocked()
            key = self._key(chat_id)

            entry = data.setdefault(key, {
                "chat_id": chat_id,
                "playlist": [],
            })

            entry["last_completed_id"] = message_id
            entry["updated_at"] = int(time.time())

            await self._save_all_unlocked(data)

