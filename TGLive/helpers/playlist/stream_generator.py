import asyncio
from typing import AsyncGenerator
import pytz

from TGLive import get_logger
from TGLive.helpers.ext_utils import FIleNotFound
from TGLive.helpers.encoding.cleaner import ffmpeg_cleaner

LOGGER = get_logger(__name__)

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

    async def iter_videos(self) -> AsyncGenerator[tuple[int, AsyncGenerator[bytes, None]], None]:
        """
        Yields:
            (video_id, ts_source)
        """

        current_id = None

        while True:
            # --------------------------------------------------
            # GET NEXT VIDEO
            # --------------------------------------------------
            try:
                next_id = await self.pm.next_video(current_id)
            except Exception as e:
                LOGGER.error("[%s] next_video failed: %s", self.stream_name, e)
                await asyncio.sleep(1)
                continue

            if not next_id:
                await asyncio.sleep(1)
                continue

            # --------------------------------------------------
            # START VIDEO
            # --------------------------------------------------
            self.pm.last_started_id = next_id
            current_id = next_id

            LOGGER.info("[%s] Starting video %s", self.stream_name, next_id)

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
            # PREPARE STREAM (DO NOT ITERATE HERE)
            # --------------------------------------------------
            try:
                raw_source = self.ms.stream_video(
                    chat_id=self.pm.chat_id,
                    message_id=next_id,
                    stream_name=self.stream_name,
                )

                ts_source = ffmpeg_cleaner(
                    raw_source,
                    self.stream_name,
                )

                # ✅ THIS IS THE KEY FIX
                yield next_id, ts_source

                # --------------------------------------------------
                # MARK COMPLETED
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

            except Exception as e:
                LOGGER.error(
                    "[%s] Stream error %s: %s",
                    self.stream_name,
                    next_id,
                    e,
                )
                current_id = None
                continue
