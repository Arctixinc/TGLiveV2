# byte_streamer.py (debug-enhanced, logic unchanged)

import asyncio
from typing import Dict, Union

from pyrogram import Client, raw
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType
from pyrogram.session import Session, Auth

from TGLive.helpers.ext_utils.utils import get_file_ids
from TGLive.helpers.ext_utils.exception import FIleNotFound


from TGLive import get_logger

LOGGER = get_logger(__name__)

class ByteStreamer:
    def __init__(self, client: Client):
        LOGGER.debug("[ByteStreamer.__init__] Initializing ByteStreamer")

        self.clean_timer = 30 * 60
        self.client: Client = client
        self.__cached_file_ids: Dict[int, FileId] = {}
        
        self._clean_task: asyncio.Task | None = None
        self._running = True

        LOGGER.debug(
            "[ByteStreamer.__init__] clean_timer=%s client_id=%s",
            self.clean_timer,
            getattr(client, "name", None),
        )

        self._clean_task = asyncio.create_task(self.clean_cache())
        LOGGER.debug("[ByteStreamer.__init__] Cache cleaner task started")

    # ---------------------------------------------------------
    # FILE METADATA
    # ---------------------------------------------------------
    async def get_file_properties(self, chat_id: int, message_id: int) -> FileId:
        LOGGER.debug(
            "[get_file_properties] chat_id=%s message_id=%s",
            chat_id,
            message_id,
        )

        if message_id not in self.__cached_file_ids:
            LOGGER.debug(
                "[get_file_properties] cache miss for message_id=%s",
                message_id,
            )

            file_id = await get_file_ids(
                self.client,
                int(chat_id),
                int(message_id),
            )

            LOGGER.debug(
                "[get_file_properties] get_file_ids returned=%s",
                bool(file_id),
            )

            if not file_id:
                LOGGER.info("Message with ID %s not found!", message_id)
                raise FIleNotFound

            LOGGER.debug(
                "[get_file_properties] file_type=%s dc_id=%s",
                file_id.file_type,
                file_id.dc_id,
            )

            if file_id.file_type not in {FileType.VIDEO, FileType.DOCUMENT}:
                LOGGER.debug(
                    "[get_file_properties] unsupported file_type=%s",
                    file_id.file_type,
                )
                raise ValueError("Only video files are supported.")

            self.__cached_file_ids[message_id] = file_id
            LOGGER.debug(
                "[get_file_properties] cached file_id for message_id=%s",
                message_id,
            )
        else:
            LOGGER.debug(
                "[get_file_properties] cache hit for message_id=%s",
                message_id,
            )

        return self.__cached_file_ids[message_id]

    # ---------------------------------------------------------
    # FILE STREAMING
    # ---------------------------------------------------------
    async def yield_file(
        self,
        file_id: FileId,
        index: int,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ):
        client = self.client

        LOGGER.debug(
            "[yield_file] START index=%s offset=%s chunk_size=%s part_count=%s",
            index,
            offset,
            chunk_size,
            part_count,
        )

        LOGGER.debug(
            "[yield_file] first_part_cut=%s last_part_cut=%s",
            first_part_cut,
            last_part_cut,
        )

        media_session = await self.generate_media_session(file_id)
        LOGGER.debug("[yield_file] media_session acquired")

        location = await self.get_location(file_id)
        LOGGER.debug("[yield_file] file location prepared")

        current_part = 1

        try:
            LOGGER.debug(
                "[yield_file] sending GetFile offset=%s limit=%s",
                offset,
                chunk_size,
            )

            r = await media_session.send(
                raw.functions.upload.GetFile(
                    location=location,
                    offset=offset,
                    limit=chunk_size,
                )
            )

            LOGGER.debug(
                "[yield_file] first GetFile response type=%s",
                type(r),
            )

            if isinstance(r, raw.types.upload.File):
                while True:
                    chunk = r.bytes

                    chunk_len = len(chunk) if chunk else 0
                    chunk_kb = chunk_len / 1024
                    chunk_mb = chunk_len / (1024 * 1024)
                    progress_pct = (current_part / part_count) * 100 if part_count else 0

                    LOGGER.debug(
                        "[yield_file] chunk part=%s/%s | %s bytes | %.2f KB | %.2f MB | %.2f%%",
                        current_part,
                        part_count,
                        chunk_len,
                        chunk_kb,
                        chunk_mb,
                        progress_pct,
                    )

                    if not chunk:
                        LOGGER.debug("[yield_file] empty chunk received, breaking")
                        break

                    if part_count == 1:
                        LOGGER.debug("[yield_file] single-part cut")
                        yield chunk[first_part_cut:last_part_cut]

                    elif current_part == 1:
                        LOGGER.debug("[yield_file] first-part cut")
                        yield chunk[first_part_cut:]

                    elif current_part == part_count:
                        LOGGER.debug("[yield_file] last-part cut")
                        yield chunk[:last_part_cut]

                    else:
                        yield chunk

                    current_part += 1
                    offset += chunk_size

                    LOGGER.debug(
                        "[yield_file] advanced to part=%s next_offset=%s",
                        current_part,
                        offset,
                    )

                    if current_part > part_count:
                        LOGGER.debug("[yield_file] reached final part, stopping")
                        break

                    r = await media_session.send(
                        raw.functions.upload.GetFile(
                            location=location,
                            offset=offset,
                            limit=chunk_size,
                        )
                    )

        except (TimeoutError, AttributeError) as e:
            LOGGER.debug(
                "[yield_file] streaming exception=%s",
                repr(e),
                exc_info=True,
            )

        finally:
            LOGGER.debug(
                "[yield_file] FINISH index=%s total_parts_sent=%s",
                index,
                current_part - 1,
            )

    # ---------------------------------------------------------
    # MEDIA SESSION
    # ---------------------------------------------------------
    async def generate_media_session(self, file_id: FileId) -> Session:
        LOGGER.debug(
            "[generate_media_session] called file_dc=%s",
            file_id.dc_id,
        )

        media_sessions = self.client.media_sessions
        client_dc = self.client.session.dc_id

        LOGGER.debug(
            "[generate_media_session] client_dc=%s known_media_sessions=%s",
            client_dc,
            list(media_sessions.keys()),
        )

        if file_id.dc_id == client_dc:
            LOGGER.debug(
                "[generate_media_session] SAME DC detected, using main session"
            )
            return self.client.session

        if file_id.dc_id in media_sessions:
            LOGGER.debug(
                "[generate_media_session] reusing cached media session for dc=%s",
                file_id.dc_id,
            )
            return media_sessions[file_id.dc_id]

        LOGGER.debug(
            "[generate_media_session] creating NEW media session for dc=%s",
            file_id.dc_id,
        )

        media_session = Session(
            self.client,
            file_id.dc_id,
            await Auth(
                self.client,
                file_id.dc_id,
                await self.client.storage.test_mode(),
            ).create(),
            await self.client.storage.test_mode(),
            is_media=True,
        )

        await media_session.start()
        LOGGER.debug("[generate_media_session] media session started")

        exported = await self.client.invoke(
            raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
        )

        LOGGER.debug(
            "[generate_media_session] auth exported id=%s bytes_len=%s",
            exported.id,
            len(exported.bytes),
        )

        await media_session.send(
            raw.functions.auth.ImportAuthorization(
                id=exported.id,
                bytes=exported.bytes,
            )
        )

        media_sessions[file_id.dc_id] = media_session

        LOGGER.debug(
            "[generate_media_session] media session READY for dc=%s",
            file_id.dc_id,
        )

        return media_session

    # ---------------------------------------------------------
    # FILE LOCATION
    # ---------------------------------------------------------
    @staticmethod
    async def get_location(file_id: FileId) -> raw.types.InputDocumentFileLocation:
        LOGGER.debug(
            "[get_location] media_id=%s access_hash=%s thumb=%s",
            file_id.media_id,
            file_id.access_hash,
            file_id.thumbnail_size,
        )

        return raw.types.InputDocumentFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=file_id.file_reference,
            thumb_size=file_id.thumbnail_size,
        )

    # ---------------------------------------------------------
    # CACHE CLEANER
    # ---------------------------------------------------------
    async def clean_cache(self):
        LOGGER.debug("[clean_cache] started")

        try:
            while self._running:
                await asyncio.sleep(self.clean_timer)

                size_before = len(self.__cached_file_ids)
                self.__cached_file_ids.clear()

                LOGGER.debug(
                    "[clean_cache] cleared cache size=%s",
                    size_before,
                )

        except asyncio.CancelledError:
            LOGGER.debug("[clean_cache] cancelled")
            raise

    # ---------------------------------------------------------
    # STOP
    # ---------------------------------------------------------
    async def stop(self):
        self._running = False

        if self._clean_task:
            self._clean_task.cancel()
            try:
                await self._clean_task
            except asyncio.CancelledError:
                pass

            self._clean_task = None

        LOGGER.debug("[ByteStreamer] stopped")

