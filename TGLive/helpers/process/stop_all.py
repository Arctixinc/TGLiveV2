import asyncio

from TGLive import get_logger
from .registry import FFMPEG_PROCS

LOGGER = get_logger(__name__)


async def stop_all_ffmpeg(timeout: int = 5):
    LOGGER.info("[FFMPEG] stopping all ffmpeg processes")

    procs = list(FFMPEG_PROCS)
    FFMPEG_PROCS.clear()

    for proc in procs:
        try:
            if proc.stdin and not proc.stdin.is_closing():
                proc.stdin.close()
        except Exception:
            pass

    for proc in procs:
        try:
            await asyncio.wait_for(proc.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            LOGGER.warning(
                "[FFMPEG] force killing ffmpeg pid=%s", proc.pid
            )
            proc.kill()
        except Exception:
            pass

    LOGGER.info("[FFMPEG] all ffmpeg processes stopped")
