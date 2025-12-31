import asyncio
from typing import List, Optional
from pyrogram.errors import FloodWait

from TGLive import get_logger
from .base import PlaylistStore

log = get_logger(__name__)


class VideoPlaylistManager:
    """
    Playlist Manager (DB-agnostic)

    Storage order (DB):
        OLD → NEW

    Playback:
        reverse = False → OLD → NEW
        reverse = True  → NEW → OLD
    """

    def __init__(
        self,
        client,
        chat_id: int | str,
        store: PlaylistStore,
        auto_checker: bool = True,
        check_interval: int = 120,
        preloaded_playlist: list[int] | None = None,
        reverse: bool = False,
    ):
        self.client = client
        self.chat_id = chat_id
        self.store = store

        self.auto_checker = auto_checker
        self.check_interval = check_interval
        self.reverse = reverse

        self.playlist: List[int] = []
        self.latest_id: int = 0

        self.last_started_id: Optional[int] = None
        self.last_completed_id: Optional[int] = None

        self.running = False
        self.lock = asyncio.Lock()

        self.preloaded_playlist = preloaded_playlist
        self.channel_name: Optional[str] = None

    def stop(self):
        self.running = False
        log.debug("auto-checker stopped")

    async def safe_iter_messages(self, limit: int, offset_id: int = 0):
        while True:
            try:
                async for msg in self.client.iter_messages(
                    self.chat_id,
                    limit=limit,
                    offset=offset_id,
                ):
                    yield msg
                break

            except FloodWait as e:
                log.warning("FloodWait %ss", e.value)
                await asyncio.sleep(e.value)

            except Exception as e:
                log.error("iter_messages error: %s", e)
                break

    async def build(self):
        """
        Build playlist:
        - preload OR
        - load from DB OR
        - first Telegram scan (once)
        """

        # fetch channel name once
        try:
            chat = await self.client.get_chat(self.chat_id)
            self.channel_name = chat.title or chat.username or str(self.chat_id)
        except Exception:
            self.channel_name = str(self.chat_id)

        # 1️⃣ PRELOADED PLAYLIST
        if self.preloaded_playlist:
            async with self.lock:
                self.playlist = list(self.preloaded_playlist)
                self.latest_id = max(self.playlist) if self.playlist else 0

            log.warning("using preloaded playlist (%s items)", len(self.playlist))
            return

        # 2️⃣ LOAD FROM DB (SOURCE OF TRUTH)
        data = await self.store.load(self.chat_id)

        if data:
            async with self.lock:
                self.playlist = data.get("playlist", [])
                self.latest_id = data.get("latest_id", 0)
                self.reverse = data.get("reverse", self.reverse)
                self.last_started_id = data.get("last_started_id")
                self.last_completed_id = data.get("last_completed_id")

            log.info(
                "playlist loaded (%s items, latest_id=%s)",
                len(self.playlist),
                self.latest_id,
            )

            if self.auto_checker:
                await self.start_auto_update()
            return

        # 3️⃣ FIRST TELEGRAM SCAN (ONLY ONCE)
        log.info("building playlist from Telegram (first run)…")

        temp: List[int] = []
        latest = 0

        async for msg in self.safe_iter_messages(limit=2000):
            if msg.video or (
                msg.document
                and msg.document.mime_type
                and msg.document.mime_type.startswith("video/")
            ):
                temp.append(msg.id)
                latest = max(latest, msg.id)

        async with self.lock:
            self.playlist = temp
            self.latest_id = latest

        await self.store.append_new(
            self.chat_id,
            temp,
            reverse=self.reverse,
            channel_name=self.channel_name,
        )

        log.info(
            "playlist built (%s items, latest_id=%s)",
            len(self.playlist),
            self.latest_id,
        )

        if self.auto_checker:
            await self.start_auto_update()

    async def start_auto_update(self):
        if self.running or self.preloaded_playlist:
            return

        self.running = True
        log.info("auto-checker started (%ss)", self.check_interval)
        asyncio.create_task(self._auto_loop())

    async def _auto_loop(self):
        while self.running:
            try:
                await self.check_for_updates()
            except Exception as e:
                log.error("auto-update error: %s", e)
            await asyncio.sleep(self.check_interval)

    async def check_for_updates(self):
        """
        Incremental update:
        - start from latest_id + 1
        - scan max 500 messages
        """

        new_ids: List[int] = []
        start = self.latest_id + 1
        end = start + 500
        local_latest = self.latest_id

        async for msg in self.safe_iter_messages(limit=end, offset_id=start):
            if msg.id <= self.latest_id:
                continue

            if msg.video or (
                msg.document
                and msg.document.mime_type
                and msg.document.mime_type.startswith("video/")
            ):
                new_ids.append(msg.id)
                local_latest = max(local_latest, msg.id)

        if not new_ids:
            return

        new_ids = sorted(set(new_ids))

        async with self.lock:
            new_ids = [i for i in new_ids if i not in self.playlist]
            if not new_ids:
                return

            self.playlist.extend(new_ids)
            self.latest_id = local_latest

        await self.store.append_new(
            self.chat_id,
            new_ids,
            reverse=self.reverse,
            channel_name=self.channel_name,
        )

        log.info(
            "added %s new videos (total=%s, latest_id=%s)",
            len(new_ids),
            len(self.playlist),
            self.latest_id,
        )
        
    async def manual_update(self):
        if self.preloaded_playlist:
            return
        log.info("manual update triggered")

        try:
            await self.check_for_updates()
            log.info("manual update finished")
        except Exception as e:
            log.error("manual update failed: %s", e)

    async def remove_video(self, message_id: int):
        async with self.lock:
            if message_id in self.playlist:
                self.playlist.remove(message_id)

            if self.last_started_id == message_id:
                self.last_started_id = None
            if self.last_completed_id == message_id:
                self.last_completed_id = None

        await self.store.remove_video(self.chat_id, message_id)
        log.warning("removed video %s", message_id)

    async def next_video(self, current_id: Optional[int]) -> Optional[int]:
        async with self.lock:
            if not self.playlist:
                return None
            
            size = len(self.playlist)

            if current_id is None:
                if self.last_started_id in self.playlist:
                    return self.last_started_id
                if self.last_completed_id in self.playlist:
                    idx = self.playlist.index(self.last_completed_id)
                    return self.playlist[(idx + 1) % size]
                return self.playlist[0]

            try:
                idx = self.playlist.index(current_id)
                return self.playlist[(idx + 1) % len(self.playlist)]
            except ValueError:
                return self.playlist[0]

    def get_playlist(self) -> List[int]:
        if self.reverse:
            return self.playlist[::-1]
        return self.playlist
