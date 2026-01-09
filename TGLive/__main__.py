import os
import asyncio
import signal
import subprocess
from asyncio import gather
from pyrogram import idle

from TGLive import setup_logging, get_logger, __title__, __version__, Telegram
from TGLive.helpers.client import ClientManager
from TGLive.helpers.playlist import VideoPlaylistManager
from TGLive.helpers.playlist.stream_generator import PlaylistStreamGenerator
from TGLive.helpers.database import JsonPlaylistStore, MongoPlaylistStore
from TGLive.helpers.encoding.hls import start_hls_runner, stop_all_hls
from TGLive.helpers.process.stop_all import stop_all_ffmpeg
from TGLive.helpers.streaming.streamer import MultiClientStreamer
from TGLive.helpers.ext_utils import clean_hls_folder
from TGLive.web.server import start_server, stop_server


STREAM_STUCK_TIMEOUT = 20   # seconds (no TS activity)
STREAM_RESTART_DELAY = 5   # seconds before restart


def run_supdate():
    subprocess.run([os.sys.executable, "update.py"], check=False)


async def run_stream_once(stream_name: str, chat_id: int, shutdown_event: asyncio.Event):
    logger = get_logger(stream_name)

    worker_ids = list(ClientManager.multi_clients.keys())
    if not worker_ids:
        raise RuntimeError("No Telegram worker clients available")

    client = ClientManager.multi_clients[worker_ids[0]]

    store = MongoPlaylistStore(Telegram.DATABASE_URL, "TGLive2")

    manager = VideoPlaylistManager(
        client=client,
        chat_id=chat_id,
        store=store,
        auto_checker=True,
    )
    await manager.build()

    playlist_generator = PlaylistStreamGenerator(
        playlist_manager=manager,
        multi_streamer=MultiClientStreamer(),
        stream_name=stream_name,
    )

    hls_dir = f"hls/{stream_name}"
    ffmpeg = None

    last_activity = asyncio.get_running_loop().time()

    async def watchdog():
        nonlocal last_activity
        while not shutdown_event.is_set():
            await asyncio.sleep(5)
            if asyncio.get_running_loop().time() - last_activity > STREAM_STUCK_TIMEOUT:
                raise RuntimeError("Stream stuck: no TS activity")

    try:
        ffmpeg = await start_hls_runner(
            hls_dir=hls_dir,
            stream_name=stream_name,
        )
        logger.info("[%s] FFmpeg started", stream_name)

        watchdog_task = asyncio.create_task(watchdog())

        async for video_id, ts_source in playlist_generator.iter_videos():
            if shutdown_event.is_set():
                break

            logger.info("[%s] Playing video %s", stream_name, video_id)

            async for chunk in ts_source:
                if shutdown_event.is_set():
                    break

                try:
                    ffmpeg.stdin.write(chunk)
                    ffmpeg.stdin.flush()
                    last_activity = asyncio.get_running_loop().time()
                except (BrokenPipeError, OSError):
                    raise RuntimeError("FFmpeg pipe broken")

    finally:
        try:
            watchdog_task.cancel()
        except Exception:
            pass

        if ffmpeg:
            try:
                if ffmpeg.stdin:
                    ffmpeg.stdin.close()
                await asyncio.wait_for(ffmpeg.wait(), timeout=5)
            except Exception:
                ffmpeg.kill()

        logger.warning("[%s] Stream stopped", stream_name)


async def start_stream(stream_index: int, chat_id: int, shutdown_event: asyncio.Event):
    stream_name = f"stream{stream_index}"
    logger = get_logger(stream_name)

    while not shutdown_event.is_set():
        logger.warning("[%s] Starting stream", stream_name)

        try:
            await run_stream_once(stream_name, chat_id, shutdown_event)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("[%s] Stream crashed: %s", stream_name, e)

        if shutdown_event.is_set():
            break

        logger.warning("[%s] Restarting stream in %ss", stream_name, STREAM_RESTART_DELAY)
        await asyncio.sleep(STREAM_RESTART_DELAY)


async def main():
    setup_logging()
    logger = get_logger(__name__)
    logger.info("Starting %s version %s", __title__, __version__)

    shutdown_event = asyncio.Event()
    stream_tasks = []

    clean_hls_folder()

    web_runner = await start_server(port=Telegram.PORT)
    logger.info("Web server started on port %s", Telegram.PORT)

    await gather(
        ClientManager.start(),
        ClientManager.start_multi_clients(),
    )

    for i, chat_id in enumerate(Telegram.STREAM_DB_IDS, start=1):
        stream_tasks.append(
            asyncio.create_task(start_stream(i, chat_id, shutdown_event))
        )

    logger.info("Started %d streams", len(stream_tasks))

    async def shutdown():
        if shutdown_event.is_set():
            return

        shutdown_event.set()
        logger.warning("Shutdown initiated")

        for task in stream_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await stop_all_ffmpeg()
        await stop_server(web_runner)
        await ClientManager.stop()
        await stop_all_hls()
        clean_hls_folder()

        logger.warning("Shutdown completed")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))
        except NotImplementedError:
            pass

    try:
        await idle()
    finally:
        await shutdown()


if __name__ == "__main__":
    asyncio.run(main())
