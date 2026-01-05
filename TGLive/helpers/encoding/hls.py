import os
from TGLive import get_logger
from TGLive.helpers.encoding.ffmpeg import FFmpegProcess

LOGGER = get_logger(__name__)


async def start_hls_runner(
    ts_source,
    hls_dir: str,
    stream_name: str,
    start_number: int,
):
    os.makedirs(hls_dir, exist_ok=True)

    playlist_path = os.path.join(hls_dir, "live.m3u8")
    segment_pattern = os.path.join(hls_dir, "%d.ts")

    cmd = [
        "ffmpeg",
        "-y",

        # 🔒 REAL-TIME pacing (MUST be before -i)
        "-re",

        "-threads", "1",
        "-loglevel", "error",

        # INPUT: CLEAN MPEG-TS
        "-f", "mpegts",
        "-fflags", "+genpts+igndts",
        "-analyzeduration", "100M",
        "-probesize", "100M",
        "-i", "pipe:0",

        # Mapping
        "-map", "0:v:0",
        "-map", "0:a?",

        # Video → H.264 (stable for HLS)
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-pix_fmt", "yuv420p",
        "-profile:v", "baseline",
        "-level", "3.1",
        "-g", "48",
        "-sc_threshold", "0",
        "-keyint_min", "48",

        # Audio → AAC
        "-c:a", "aac",
        "-b:a", "128k",
        "-ac", "2",

        # HLS
        "-f", "hls",
        "-hls_time", "4",
        "-hls_list_size", "10",
        "-hls_flags",
        "delete_segments+append_list+omit_endlist+independent_segments",

        # Segment continuity
        "-start_number", str(start_number),
        "-hls_segment_filename", segment_pattern,

        playlist_path,
    ]

    ff = FFmpegProcess(cmd, stream_name)
    await ff.start()

    try:
        async for chunk in ts_source:
            if not await ff.write(chunk):
                break
    finally:
        await ff.stop()
        LOGGER.info("[%s] HLS ffmpeg stopped cleanly", stream_name)
