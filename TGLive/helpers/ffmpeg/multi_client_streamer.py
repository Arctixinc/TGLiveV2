import asyncio
import math
from typing import AsyncGenerator, Dict

from TGLive import get_logger
from TGLive.helpers.client import ClientManager
from TGLive.helpers.ext_utils import ByteStreamer

from .ffmpeg_registry import FFMPEG_PROCS

LOGGER = get_logger(__name__)

_rr_pointer = 0


class MultiClientStreamer:
    def __init__(self):
        self._bs_cache: Dict[int, ByteStreamer] = {}

    def _choose_client(self) -> int:
        global _rr_pointer
        if not ClientManager.work_loads:
            return next(iter(ClientManager.multi_clients.keys()))

        min_load = min(ClientManager.work_loads.values())
        candidates = [i for i, v in ClientManager.work_loads.items() if v == min_load]
        chosen = candidates[_rr_pointer % len(candidates)]
        _rr_pointer += 1
        return chosen

    def _get_bs(self, index: int) -> ByteStreamer:
        if index not in self._bs_cache:
            self._bs_cache[index] = ByteStreamer(ClientManager.multi_clients[index])
        return self._bs_cache[index]

    async def _drain_stderr(self, proc, stream_name: str):
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                LOGGER.warning(
                    "[%s] ffmpeg stderr: %s",
                    stream_name,
                    line.decode(errors="ignore").strip(),
                )
        except asyncio.CancelledError:
            pass

    async def stream_video(
        self,
        chat_id: int,
        message_id: int,
        stream_name: str,
        start_offset: int = 0,
    ) -> AsyncGenerator[bytes, None]:

        index = self._choose_client()
        bs = self._get_bs(index)
        ClientManager.work_loads[index] = ClientManager.work_loads.get(index, 0) + 1

        ffmpeg = None
        pump_task = None
        stderr_task = None

        try:
            file_id = await bs.get_file_properties(chat_id, message_id)
            file_size = file_id.file_size or 0

            chunk_size = 512 * 1024
            part_count = max(1, math.ceil(file_size / chunk_size))
            last_part_cut = (file_size % chunk_size) or chunk_size

            ffmpeg = await asyncio.create_subprocess_exec(
                "ffmpeg",
                "-threads", "1",
                "-loglevel", "error",
                "-fflags", "+genpts",
                "-i", "pipe:0",
                "-map", "0:v:0",
                "-map", "0:a?",
                "-c:v", "copy",
                "-c:a", "aac",
                "-b:a", "128k",
                "-f", "mpegts",
                "pipe:1",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            FFMPEG_PROCS.add(ffmpeg)
            LOGGER.info("[%s] clean ffmpeg started (pid=%s)", stream_name, ffmpeg.pid)

            stderr_task = asyncio.create_task(
                self._drain_stderr(ffmpeg, stream_name)
            )

            async def pump():
                try:
                    async for chunk in bs.yield_file(
                        file_id=file_id,
                        index=index,
                        offset=start_offset,
                        first_part_cut=0,
                        last_part_cut=last_part_cut,
                        part_count=part_count,
                        chunk_size=chunk_size,
                    ):
                        if ffmpeg.stdin.is_closing():
                            break

                        try:
                            ffmpeg.stdin.write(chunk)
                            await ffmpeg.stdin.drain()
                        except (BrokenPipeError, ConnectionResetError):
                            LOGGER.warning(
                                "[%s] ffmpeg stdin closed, stopping pump",
                                stream_name,
                            )
                            break

                except asyncio.CancelledError:
                    LOGGER.debug("[%s] pump cancelled", stream_name)

                except Exception as e:
                    LOGGER.error("[%s] pump error: %s", stream_name, e)

                finally:
                    try:
                        if ffmpeg.stdin and not ffmpeg.stdin.is_closing():
                            ffmpeg.stdin.close()
                    except Exception:
                        pass

            pump_task = asyncio.create_task(pump())

            while True:
                data = await ffmpeg.stdout.read(188 * 64)
                if not data:
                    break
                yield data

        finally:
            # Cancel pump safely
            if pump_task:
                pump_task.cancel()
                try:
                    await pump_task
                except asyncio.CancelledError:
                    pass

            # Wait for ffmpeg exit
            if ffmpeg:
                try:
                    await ffmpeg.wait()
                except Exception:
                    pass
                FFMPEG_PROCS.discard(ffmpeg)

            if stderr_task:
                stderr_task.cancel()

            ClientManager.work_loads[index] = max(
                0, ClientManager.work_loads.get(index, 1) - 1
            )

            LOGGER.info("[%s] clean ffmpeg exited", stream_name)
