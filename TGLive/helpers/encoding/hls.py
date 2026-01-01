import os
from TGLive import get_logger
from TGLive.helpers.encoding.ffmpeg import FFmpegProcess

LOGGER = get_logger(__name__)


async def start_hls_runner(ts_source, hls_dir: str, stream_name: str):
    LOGGER.info(
        "[%s] HLS runner starting → dir=%s",
        stream_name,
        hls_dir,
    )

    os.makedirs(hls_dir, exist_ok=True)

    playlist_path = os.path.join(hls_dir, "live.m3u8")

    # ✅ cross-platform timestamp (NO %s)
    segment_pattern = os.path.join(
        hls_dir, "%Y%m%d%H%M%S.ts"
    )

    cmd = [
        "ffmpeg",
        "-re",
        "-threads", "1",
        "-loglevel", "error",
        "-fflags", "+genpts",
        "-i", "pipe:0",

        "-map", "0:v:0",
        "-map", "0:a?",
        "-c:v", "copy",
        "-c:a", "aac",
        "-b:a", "128k",

        "-f", "hls",
        "-hls_time", "4",
        "-hls_list_size", "10",
        "-hls_flags", "delete_segments+append_list",

        # 🔑 REQUIRED
        "-strftime", "1",
        "-hls_segment_filename", segment_pattern,

        playlist_path,
    ]

    ff = FFmpegProcess(cmd, stream_name)
    await ff.start()

    try:
        async for chunk in ts_source:
            ok = await ff.write(chunk)
            if not ok:
                break
    finally:
        await ff.stop()
        LOGGER.info("[%s] HLS runner stopped", stream_name)
