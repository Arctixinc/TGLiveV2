import asyncio
from TGLive.helpers.process.registry import FFMPEG_PROCS
from TGLive import get_logger

LOGGER = get_logger(__name__)

async def ffmpeg_cleaner(byte_source, stream_name):
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-loglevel", "error",
        "-fflags", "+genpts",
        "-avoid_negative_ts", "make_zero",
        "-i", "pipe:0",
        "-map", "0:v:0",
        "-map", "0:a?",
        "-c:v", "copy",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ac", "2",
        "-f", "mpegts",
        "pipe:1",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    FFMPEG_PROCS.add(proc)
    LOGGER.info("[%s] cleaner ffmpeg started (pid=%s)", stream_name, proc.pid)

    async def pump():
        async for chunk in byte_source:
            if proc.stdin.is_closing():
                break
            proc.stdin.write(chunk)
            await proc.stdin.drain()
        proc.stdin.close()

    pump_task = asyncio.create_task(pump())

    try:
        while True:
            data = await proc.stdout.read(188 * 256)
            if not data:
                break
            yield data
    finally:
        pump_task.cancel()
        await proc.wait()
        FFMPEG_PROCS.discard(proc)
        LOGGER.info("[%s] cleaner ffmpeg stopped", stream_name)
