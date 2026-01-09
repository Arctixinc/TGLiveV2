import os
import subprocess
from TGLive import get_logger
from TGLive.helpers.encoding.ffmpeg import FFmpegProcess

LOGGER = get_logger(__name__)


_hls_processes: dict[str, subprocess.Popen] = {}

async def start_hls_runner(hls_dir: str, stream_name: str) -> subprocess.Popen:
    if stream_name in _hls_processes:
        return _hls_processes[stream_name]

    os.makedirs(hls_dir, exist_ok=True)

    playlist_path = os.path.join(hls_dir, "live.m3u8")
    segment_pattern = os.path.join(hls_dir, "%d.ts")

    cmd = [
        "ffmpeg",
        "-loglevel", "error",
        "-re",
        "-threads", "1",
        "-fflags", "+genpts",
        "-i", "pipe:0",
        "-map", "0:v:0",
        "-map", "0:a?",
        "-c:v", "copy",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ac", "2",
        "-f", "hls",
        "-hls_time", "4",
        "-hls_list_size", "6",
        "-hls_flags",
        "delete_segments+append_list+omit_endlist+independent_segments",

        "-hls_segment_filename", segment_pattern,
        playlist_path,
    ]
    
    LOGGER.info("[%s] Starting persistent FFmpeg", stream_name)
    
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    _hls_processes[stream_name] = proc
    return proc

    
    
async def stop_all_hls():
    """
    Stop ALL FFmpeg processes cleanly (used on shutdown).
    """
    for name, proc in _hls_processes.items():
        try:
            LOGGER.warning("[%s] Stopping FFmpeg", name)
            if proc.stdin:
                proc.stdin.close()
            proc.terminate()
        except Exception:
            pass

    _hls_processes.clear()