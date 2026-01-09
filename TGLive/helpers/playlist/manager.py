import asyncio
from typing import List, Optional
from pyrogram.errors import FloodWait

from TGLive import get_logger
from .base import PlaylistStore

log = get_logger(__name__)

# ============================================================
# GLOBAL SCAN LOCK
# ============================================================
SCAN_SEMAPHORE = asyncio.Semaphore(1)


class VideoPlaylistManager:
    """
    Playlist Manager (DB-agnostic)
    FloodWait-safe
    Restart-safe
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

        # 🔥 task tracking (FIX)
        self._auto_task: Optional[asyncio.Task] = None
        self._delayed_task: Optional[asyncio.Task] = None

    # ============================================================
    # STOP (CRITICAL FOR RESTARTS)
    # ============================================================
    async def stop(self):
        self.running = False

        for task in (self._auto_task, self._delayed_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._auto_task = None
        self._delayed_task = None

        log.debug("auto-checker fully stopped")

    # ============================================================
    # SAFE ITERATOR (THROTTLED)
    # ============================================================
    async def safe_iter_messages(self, limit: int, offset_id: int = 0):
        async with SCAN_SEMAPHORE:
            while True:
                try:
                    async for msg in self.client.iter_messages(
                        self.chat_id,
                        limit=limit,
                        offset=offset_id,
                    ):
                        yield msg
                        await asyncio.sleep(0.02)
                    break

                except FloodWait as e:
                    log.warning("FloodWait %ss", e.value)
                    await asyncio.sleep(e.value + 1)

                except Exception as e:
                    log.error("iter_messages error: %s", e)
                    break

    # ============================================================
    # BUILD PLAYLIST
    # ============================================================
    async def build(self):
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

            log.warning(
                "using preloaded playlist (%s items)",
                len(self.playlist),
            )
            return

        # 2️⃣ LOAD FROM DB
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
                self._delayed_task = asyncio.create_task(
                    self._delayed_auto_start()
                )
            return

        # 3️⃣ FIRST TELEGRAM SCAN (CHUNKED)
        log.info("building playlist from Telegram (first run)…")

        temp: List[int] = []
        latest = 0
        scanned = 0

        async for msg in self.safe_iter_messages(limit=2000):
            scanned += 1

            if msg.video or (
                msg.document
                and msg.document.mime_type
                and msg.document.mime_type.startswith("video/")
            ):
                temp.append(msg.id)
                latest = max(latest, msg.id)

            if scanned % 200 == 0:
                await asyncio.sleep(1)

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
            self._delayed_task = asyncio.create_task(
                self._delayed_auto_start()
            )

    # ============================================================
    # AUTO CHECKER
    # ============================================================
    async def _delayed_auto_start(self):
        await asyncio.sleep(30)
        await self.start_auto_update()

    async def start_auto_update(self):
        if self.running or self.preloaded_playlist:
            return

        self.running = True
        log.info("auto-checker started (%ss)", self.check_interval)

        self._auto_task = asyncio.create_task(self._auto_loop())

    async def _auto_loop(self):
        try:
            while self.running:
                await self.check_for_updates()
                await asyncio.sleep(self.check_interval)
        except asyncio.CancelledError:
            pass
        finally:
            log.debug("auto-checker loop exited")

    # ============================================================
    # INCREMENTAL UPDATE
    # ============================================================
    async def check_for_updates(self):
        new_ids: List[int] = []
        start = self.latest_id + 1
        end = start + 500
        local_latest = self.latest_id

        async for msg in self.safe_iter_messages(
            limit=end,
            offset_id=start,
        ):
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

        async with self.lock:
            new_ids = [i for i in set(new_ids) if i not in self.playlist]
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
            "added %s new videos (total=%s)",
            len(new_ids),
            len(self.playlist),
        )

    # ============================================================
    # PLAYBACK HELPERS (UNCHANGED)
    # ============================================================
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
                return self.playlist[(idx + 1) % size]
            except ValueError:
                return self.playlist[0]

    async def get_playlist(self) -> List[int]:
        return self.playlist[::-1] if self.reverse else self.playlist
