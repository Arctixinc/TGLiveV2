import os
import json
import time
import asyncio
from typing import Optional, List
from TGLive import get_logger

log = get_logger(__name__)


class JsonPlaylistStore:
    def __init__(self, file_path: str = "playlists.json"):
        self.file_path = file_path
        self._lock = asyncio.Lock()

        if not os.path.exists(self.file_path):
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump({}, f)
            log.info("created playlist file: %s", self.file_path)

    def _key(self, chat_id: int | str) -> str:
        return f"channel_{chat_id}"

    async def _load_all(self) -> dict:
        try:
            if os.path.getsize(self.file_path) == 0:
                return {}
            with open(self.file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.warning("load failed: %s", e)
            return {}

    async def _save_all(self, data: dict):
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
        log.debug("saved playlist file")

    async def load(self, chat_id: int | str) -> Optional[dict]:
        async with self._lock:
            data = await self._load_all()
            entry = data.get(self._key(chat_id))
            log.debug("load: chat=%s found=%s", chat_id, bool(entry))
            return entry

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
                "latest_id": 0,
                "last_started_id": None,
                "last_completed_id": None,
            })

            playlist = entry["playlist"]
            seen = set(playlist)

            added = 0
            for vid in sorted(set(new_ids)):
                if vid not in seen:
                    playlist.append(vid)
                    seen.add(vid)
                    added += 1

            entry["playlist"] = playlist
            entry["latest_id"] = max(
                entry.get("latest_id", 0),
                max(new_ids),
            )
            entry["reverse"] = reverse
            entry["updated_at"] = int(time.time())

            if channel_name:
                entry["channel_name"] = channel_name

            data[key] = entry
            await self._save_all(data)

        log.info(
            "append: chat=%s added=%s total=%s latest_id=%s",
            chat_id,
            added,
            len(playlist),
            entry["latest_id"],
        )

    async def remove_video(self, chat_id: int | str, video_id: int):
        async with self._lock:
            data = await self._load_all()
            key = self._key(chat_id)

            entry = data.get(key)
            if not entry:
                return

            entry["playlist"] = [x for x in entry["playlist"] if x != video_id]

            if entry.get("last_started_id") == video_id:
                entry["last_started_id"] = None
            if entry.get("last_completed_id") == video_id:
                entry["last_completed_id"] = None

            entry["updated_at"] = int(time.time())
            await self._save_all(data)

        log.info("remove: chat=%s video=%s", chat_id, video_id)

    async def set_last_started(self, chat_id: int | str, video_id: int):
        async with self._lock:
            data = await self._load_all()
            key = self._key(chat_id)

            entry = data.setdefault(key, {
                "chat_id": chat_id,
                "playlist": [],
                "latest_id": 0,
                "last_completed_id": None,
            })

            entry["last_started_id"] = video_id
            entry["updated_at"] = int(time.time())
            await self._save_all(data)

        log.info("started: chat=%s video=%s", chat_id, video_id)

    async def set_last_completed(self, chat_id: int | str, video_id: int):
        async with self._lock:
            data = await self._load_all()
            key = self._key(chat_id)

            entry = data.setdefault(key, {
                "chat_id": chat_id,
                "playlist": [],
                "latest_id": 0,
            })

            entry["last_completed_id"] = video_id
            entry["updated_at"] = int(time.time())
            await self._save_all(data)

        log.info("completed: chat=%s video=%s", chat_id, video_id)

    async def get_playlist(self, chat_id: int | str) -> List[int]:
        async with self._lock:
            data = await self._load_all()
            entry = data.get(self._key(chat_id))
            if not entry:
                return []

            playlist = entry.get("playlist", [])

            if entry.get("reverse", False):
                playlist = playlist[::-1]

            log.debug(
                "get_playlist: chat=%s count=%s latest_id=%s",
                chat_id,
                len(playlist),
                entry.get("latest_id", 0),
            )

            return playlist
