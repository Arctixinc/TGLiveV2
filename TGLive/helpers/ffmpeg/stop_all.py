import asyncio

from TGLive import get_logger
from .ffmpeg_registry import FFMPEG_PROCS

LOGGER = get_logger(__name__)


async def stop_all_ffmpeg():
    LOGGER.info("[FFMPEG] stopping all ffmpeg processes")

    procs = list(FFMPEG_PROCS)
    FFMPEG_PROCS.clear()

    for proc in procs:
        try:
            if proc.stdin and not proc.stdin.is_closing():
                proc.stdin.close()
        except Exception:
            pass

    await asyncio.gather(
        *(proc.wait() for proc in procs),
        return_exceptions=True,
    )

    LOGGER.info("[FFMPEG] all ffmpeg processes stopped")
