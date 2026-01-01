import asyncio
import os

from TGLive import get_logger
from .ffmpeg_registry import FFMPEG_PROCS

LOGGER = get_logger(__name__)


async def start_hls_runner(ts_source, hls_dir: str, stream_name: str):
    LOGGER.debug(
        "[HLS] start_hls_runner ENTER stream_name=%s hls_dir=%s",
        stream_name,
        hls_dir,
    )

    os.makedirs(hls_dir, exist_ok=True)

    playlist_path = os.path.join(hls_dir, "live.m3u8")
    segment_path = os.path.join(hls_dir, "%d.ts")

    ffmpeg_hls = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-threads", "1",
        "-re",
        "-loglevel", "error",
        "-i", "pipe:0",
        "-map", "0:v:0",
        "-map", "0:a?",
        "-c", "copy",
        "-f", "hls",
        "-hls_time", "4",
        "-hls_list_size", "6",
        "-hls_flags",
        "delete_segments+append_list+omit_endlist+program_date_time",
        "-hls_segment_filename", segment_path,
        playlist_path,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )

    FFMPEG_PROCS.add(ffmpeg_hls)
    LOGGER.info("[%s] HLS encoder started (pid=%s)", stream_name, ffmpeg_hls.pid)

    async def drain_stderr():
        try:
            while True:
                line = await ffmpeg_hls.stderr.readline()
                if not line:
                    break
                LOGGER.debug(
                    "[%s] ffmpeg_hls stderr: %s",
                    stream_name,
                    line.decode(errors="ignore").strip(),
                )
        except Exception:
            LOGGER.exception("[%s] HLS stderr drain error", stream_name)

    asyncio.create_task(drain_stderr())

    try:
        async for ts_chunk in ts_source:
            if ffmpeg_hls.stdin.is_closing():
                break

            ffmpeg_hls.stdin.write(ts_chunk)
            await ffmpeg_hls.stdin.drain()

    finally:
        try:
            ffmpeg_hls.stdin.close()
        except Exception:
            pass

        await ffmpeg_hls.wait()
        FFMPEG_PROCS.discard(ffmpeg_hls)
        LOGGER.info("[%s] HLS runner stopped", stream_name)
