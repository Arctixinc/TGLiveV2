import asyncio
from TGLive import get_logger
from TGLive.helpers.process.registry import FFMPEG_PROCS

LOGGER = get_logger(__name__)


class FFmpegProcess:
    def __init__(self, cmd: list[str], stream_name: str):
        self.cmd = cmd
        self.stream_name = stream_name
        self.proc: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task | None = None

    async def start(self):
        self.proc = await asyncio.create_subprocess_exec(
            *self.cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,  # IMPORTANT
            stderr=asyncio.subprocess.PIPE,
        )

        FFMPEG_PROCS.add(self.proc)

        LOGGER.info(
            "[%s] ffmpeg started (pid=%s)",
            self.stream_name,
            self.proc.pid,
        )

        self._stderr_task = asyncio.create_task(self._drain_stderr())

    async def write(self, data: bytes) -> bool:
        if not self.proc or self.proc.stdin.is_closing():
            return False

        try:
            self.proc.stdin.write(data)
            await self.proc.stdin.drain()
            return True
        except (BrokenPipeError, ConnectionResetError):
            LOGGER.warning("[%s] ffmpeg stdin closed", self.stream_name)
            return False

    async def stop(self):
        if not self.proc:
            return

        LOGGER.info("[%s] stopping ffmpeg", self.stream_name)

        try:
            if self.proc.stdin and not self.proc.stdin.is_closing():
                self.proc.stdin.close()
        except Exception:
            pass

        if self._stderr_task:
            self._stderr_task.cancel()

        try:
            await self.proc.wait()
        except Exception:
            pass

        FFMPEG_PROCS.discard(self.proc)
        LOGGER.info("[%s] ffmpeg stopped", self.stream_name)

        self.proc = None

    async def _drain_stderr(self):
        try:
            while True:
                line = await self.proc.stderr.readline()
                if not line:
                    break
                LOGGER.warning(
                    "[%s] ffmpeg stderr: %s",
                    self.stream_name,
                    line.decode(errors="ignore").strip(),
                )
        except asyncio.CancelledError:
            pass
