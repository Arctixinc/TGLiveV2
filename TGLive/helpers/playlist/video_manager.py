import asyncio
from typing import AsyncGenerator
from datetime import datetime, timedelta
import pytz


from TGLive.helpers.ext_utils import get_readable_time, FIleNotFound

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
            LOGGER.debug(
                "[%s] requesting next video | current_id=%s",
                self.stream_name,
                current_id,
            )

            try:
                next_id = await self.pm.next_video(current_id)
                LOGGER.debug(
                    "[%s] next_video returned next_id=%s",
                    self.stream_name,
                    next_id,
                )
            except Exception as e:
                LOGGER.error(
                    f"[{self.stream_name}] next_video failed: {e}"
                )
                await asyncio.sleep(1)
                continue

            if not next_id:
                LOGGER.debug(
                    "[%s] no next video available, sleeping",
                    self.stream_name,
                )
                await asyncio.sleep(1)
                continue

            # --------------------------------------------------
            # START VIDEO (STATE UPDATE)
            # --------------------------------------------------
            self.pm.last_started_id = next_id
            current_id = next_id

            LOGGER.debug(
                "[%s] starting video | id=%s",
                self.stream_name,
                next_id,
            )

            try:
                await self.pm.mongo.set_last_started(
                    self.pm.chat_id, next_id
                )
                LOGGER.debug(
                    "[%s] mongo last_started updated | id=%s",
                    self.stream_name,
                    next_id,
                )
            except Exception as e:
                LOGGER.error(
                    f"[{self.stream_name}] Mongo start update failed: {e}"
                )

            # --------------------------------------------------
            # DURATION + ETA LOG
            # --------------------------------------------------
            LOGGER.debug(
                "[%s] fetching duration | id=%s",
                self.stream_name,
                next_id,
            )

            duration = await self.pm.get_duration(next_id)

            if duration:
                now = datetime.now(LOCAL_TZ)
                expected_end = now + timedelta(seconds=duration)

                LOGGER.debug(
                    "[%s] duration=%s seconds | now=%s | expected_end=%s",
                    self.stream_name,
                    duration,
                    now.isoformat(),
                    expected_end.isoformat(),
                )

                LOGGER.info(
                    f"[{self.stream_name}] Starting video {next_id} | "
                    f"duration={get_readable_time(duration)} | "
                    f"Expected end at {expected_end.strftime('%Y-%m-%d %I:%M:%S %p')}"
                )

            else:
                LOGGER.debug(
                    "[%s] duration unknown | id=%s",
                    self.stream_name,
                    next_id,
                )

                LOGGER.info(
                    f"[{self.stream_name}] Starting video {next_id} | "
                    f"duration=unknown"
                )

            # --------------------------------------------------
            # STREAM VIDEO
            # --------------------------------------------------
            LOGGER.debug(
                "[%s] entering stream_video | id=%s",
                self.stream_name,
                next_id,
            )

            try:
                async for ts_chunk in self.ms.stream_video(
                    self.pm.chat_id,
                    next_id,
                    stream_name=self.stream_name
                ):
                    yield ts_chunk

                # --------------------------------------------------
                # FINISHED NORMALLY
                # --------------------------------------------------
                self.pm.last_completed_id = next_id

                LOGGER.debug(
                    "[%s] stream completed normally | id=%s",
                    self.stream_name,
                    next_id,
                )

                try:
                    await self.pm.mongo.set_last_completed(
                        self.pm.chat_id, next_id
                    )
                    LOGGER.debug(
                        "[%s] mongo last_completed updated | id=%s",
                        self.stream_name,
                        next_id,
                    )
                except Exception as e:
                    LOGGER.error(
                        f"[{self.stream_name}] Mongo complete update failed: {e}"
                    )

                if duration:
                    LOGGER.info(
                        f"[{self.stream_name}] Finished video {next_id} | "
                        f"played={get_readable_time(duration)}"
                    )
                else:
                    LOGGER.info(
                        f"[{self.stream_name}] Finished video {next_id}"
                    )

            # --------------------------------------------------
            # FILE DELETED / NOT FOUND
            # --------------------------------------------------
            except FIleNotFound:
                LOGGER.warning(
                    f"[{self.stream_name}] Video {next_id} not found – skipping"
                )

                try:
                    await self.pm.remove_video(next_id)
                    LOGGER.debug(
                        "[%s] removed missing video from playlist | id=%s",
                        self.stream_name,
                        next_id,
                    )
                except Exception:
                    pass

                current_id = None
                continue

            # --------------------------------------------------
            # STREAM ERROR
            # --------------------------------------------------
            except Exception as e:
                LOGGER.error(
                    f"[{self.stream_name}] Stream error {next_id}: {e}"
                )
                LOGGER.debug(
                    "[%s] resetting current_id after stream error",
                    self.stream_name,
                )
                current_id = None
                continue
