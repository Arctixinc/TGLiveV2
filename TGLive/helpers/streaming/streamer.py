import math
from typing import AsyncGenerator, Dict

from TGLive import get_logger
from TGLive.helpers.client import ClientManager
from TGLive.helpers.ext_utils import ByteStreamer

LOGGER = get_logger(__name__)

_rr_pointer = 0


class MultiClientStreamer:
    """
    STRICT RESPONSIBILITY:
    - Select Telegram client
    - Stream RAW BYTES only

    ❌ NO FFmpeg
    ❌ NO subprocess
    ❌ NO registry
    """

    def __init__(self):
        self._bs_cache: Dict[int, ByteStreamer] = {}

    # --------------------------------------------------
    # CLIENT SELECTION (round-robin + least load)
    # --------------------------------------------------
    def _choose_client(self) -> int:
        global _rr_pointer

        if not ClientManager.work_loads:
            return next(iter(ClientManager.multi_clients.keys()))

        min_load = min(ClientManager.work_loads.values())
        candidates = [
            i for i, v in ClientManager.work_loads.items()
            if v == min_load
        ]

        chosen = candidates[_rr_pointer % len(candidates)]
        _rr_pointer += 1
        return chosen

    # --------------------------------------------------
    # BYTE STREAMER
    # --------------------------------------------------
    def _get_bs(self, index: int) -> ByteStreamer:
        if index not in self._bs_cache:
            self._bs_cache[index] = ByteStreamer(
                ClientManager.multi_clients[index]
            )
        return self._bs_cache[index]

    # --------------------------------------------------
    # RAW STREAM (NO FFmpeg here)
    # --------------------------------------------------
    async def stream_video(
        self,
        chat_id: int,
        message_id: int,
        stream_name: str,
        start_offset: int = 0,
    ) -> AsyncGenerator[bytes, None]:

        index = self._choose_client()
        bs = self._get_bs(index)

        ClientManager.work_loads[index] = (
            ClientManager.work_loads.get(index, 0) + 1
        )

        try:
            file_id = await bs.get_file_properties(chat_id, message_id)
            file_size = file_id.file_size or 0

            chunk_size = 512 * 1024
            part_count = max(1, math.ceil(file_size / chunk_size))
            last_part_cut = (file_size % chunk_size) or chunk_size

            LOGGER.info(
                "[%s] streaming message=%s via client=%s size=%.2fMB",
                stream_name,
                message_id,
                index,
                file_size / (1024 * 1024),
            )

            async for chunk in bs.yield_file(
                file_id=file_id,
                index=index,
                offset=start_offset,
                first_part_cut=0,
                last_part_cut=last_part_cut,
                part_count=part_count,
                chunk_size=chunk_size,
            ):
                yield chunk

        finally:
            ClientManager.work_loads[index] = max(
                0,
                ClientManager.work_loads.get(index, 1) - 1,
            )

    # --------------------------------------------------
    # STOP (ByteStreamer cleanup)
    # --------------------------------------------------
    async def stop(self):
        LOGGER.warning("MultiClientStreamer stopping ByteStreamers")

        for index, bs in self._bs_cache.items():
            try:
                await bs.stop()
                LOGGER.debug("ByteStreamer stopped (client=%s)", index)
            except Exception as e:
                LOGGER.warning(
                    "Failed to stop ByteStreamer (client=%s): %s",
                    index,
                    e,
                )

        self._bs_cache.clear()
