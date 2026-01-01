import asyncio
from typing import AsyncGenerator
import pytz

from TGLive.helpers.ext_utils import FIleNotFound
from TGLive import get_logger

LOGGER = get_logger(__name__)

# ============================================================
# TIMEZONE (CHANGE HERE IF NEEDED)
# ============================================================
LOCAL_TZ = pytz.timezone("Asia/Kolkata")


class PlaylistStreamGenerator:
    def __init__(self, playlist_manager, multi_streamer, stream_name: str):
        self.pm = playlist_manager
        self.ms = multi_streamer
        self.stream_name = stream_name

        LOGGER.debug(
            "[%s] PlaylistStreamGenerator initialized | chat_id=%s",
            stream_name,
            getattr(playlist_manager, "chat_id", None),
        )

    async def generator(self) -> AsyncGenerator[bytes, None]:
        LOGGER.debug("[%s] generator ENTER", self.stream_name)

        current_id = None

        while True:
            # --------------------------------------------------
            # GET NEXT VIDEO
            # --------------------------------------------------
            try:
                next_id = await self.pm.next_video(current_id)
            except Exception as e:
                LOGGER.error(
                    "[%s] next_video failed: %s",
                    self.stream_name,
                    e,
                )
                await asyncio.sleep(1)
                continue

            if not next_id:
                await asyncio.sleep(1)
                continue

            # --------------------------------------------------
            # START VIDEO (STATE UPDATE)
            # --------------------------------------------------
            self.pm.last_started_id = next_id
            current_id = next_id

            LOGGER.info(
                "[%s] Starting video %s",
                self.stream_name,
                next_id,
            )

            try:
                await self.pm.store.set_last_started(
                    self.pm.chat_id, next_id
                )
            except Exception as e:
                LOGGER.warning(
                    "[%s] store start update failed: %s",
                    self.stream_name,
                    e,
                )

            # --------------------------------------------------
            # STREAM VIDEO
            # --------------------------------------------------
            try:
                async for ts_chunk in self.ms.stream_video(
                    self.pm.chat_id,
                    next_id,
                    stream_name=self.stream_name,
                ):
                    yield ts_chunk

                # --------------------------------------------------
                # FINISHED NORMALLY
                # --------------------------------------------------
                self.pm.last_completed_id = next_id

                try:
                    await self.pm.store.set_last_completed(
                        self.pm.chat_id, next_id
                    )
                except Exception as e:
                    LOGGER.warning(
                        "[%s] store complete update failed: %s",
                        self.stream_name,
                        e,
                    )

                LOGGER.info(
                    "[%s] Finished video %s",
                    self.stream_name,
                    next_id,
                )

            # --------------------------------------------------
            # FILE DELETED / NOT FOUND
            # --------------------------------------------------
            except FIleNotFound:
                LOGGER.warning(
                    "[%s] Video %s not found – removing from playlist",
                    self.stream_name,
                    next_id,
                )

                try:
                    await self.pm.remove_video(next_id)
                except Exception:
                    pass

                current_id = None
                continue

            # --------------------------------------------------
            # STREAM ERROR
            # --------------------------------------------------
            except Exception as e:
                LOGGER.error(
                    "[%s] Stream error %s: %s",
                    self.stream_name,
                    next_id,
                    e,
                )
                current_id = None
                continue