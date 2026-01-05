import asyncio
import signal
from asyncio import gather
from pyrogram import idle

from TGLive import setup_logging, get_logger, __title__, __version__, Telegram
from TGLive.helpers.client import ClientManager
from TGLive.helpers.playlist import VideoPlaylistManager
from TGLive.helpers.playlist.stream_generator import PlaylistStreamGenerator
from TGLive.helpers.database import JsonPlaylistStore
from TGLive.helpers.encoding.hls import start_hls_runner
from TGLive.helpers.encoding.utils import get_last_segment_number
from TGLive.helpers.process.stop_all import stop_all_ffmpeg
from TGLive.helpers.streaming.streamer import MultiClientStreamer
from TGLive.helpers.ext_utils import clean_hls_folder
from TGLive.web.server import start_server, stop_server


async def main():
    # --------------------------------------------------
    # LOGGING
    # --------------------------------------------------
    setup_logging()
    logger = get_logger(__name__)
    logger.info("Starting %s version %s", __title__, __version__)

    shutdown_event = asyncio.Event()

    # --------------------------------------------------
    # CLEAN HLS FOLDER (START)
    # --------------------------------------------------
    clean_hls_folder()

    # --------------------------------------------------
    # START TELEGRAM CLIENTS
    # --------------------------------------------------
    await gather(
        ClientManager.start(),
        ClientManager.start_multi_clients(),
    )

    # --------------------------------------------------
    # SELECT A WORKER CLIENT
    # --------------------------------------------------
    worker_id = 10
    client = ClientManager.multi_clients.get(worker_id)
    if not client:
        raise RuntimeError(f"Client {worker_id} not found")

    logger.info(
        "Using client | index=%s | session=%s | username=%s",
        worker_id,
        client.name,
        getattr(client.me, "username", None),
    )

    # --------------------------------------------------
    # PLAYLIST STORE + MANAGER
    # --------------------------------------------------
    store = JsonPlaylistStore()

    manager = VideoPlaylistManager(
        client=client,
        chat_id=Telegram.LOCAL_ID,
        store=store,
        auto_checker=True,
    )

    await manager.build()

    # --------------------------------------------------
    # STREAM PIPELINE (SAFE)
    # --------------------------------------------------
    multi_streamer = MultiClientStreamer()

    playlist_generator = PlaylistStreamGenerator(
        playlist_manager=manager,
        multi_streamer=multi_streamer,
        stream_name="stream1",
    )

    hls_dir = "hls/stream1"

    # --------------------------------------------------
    # STREAM SUPERVISOR (CRITICAL)
    # --------------------------------------------------
    async def stream_supervisor():
        logger.info("[stream1] Stream supervisor started")

        while not shutdown_event.is_set():
            try:
                async for video_id, ts_source in playlist_generator.iter_videos():
                    if shutdown_event.is_set():
                        break

                    logger.info("[stream1] Starting video %s", video_id)

                    start_number = get_last_segment_number(hls_dir)

                    # 🔒 STAGE 1 (Cleaner FFmpeg)
                    # 🔒 STAGE 2 (HLS FFmpeg)
                    await start_hls_runner(
                        ts_source=ts_source,   # CLEAN MPEG-TS ONLY
                        hls_dir=hls_dir,
                        stream_name="stream1",
                        start_number=start_number,
                    )

                    logger.info("[stream1] Finished video %s", video_id)

            except asyncio.CancelledError:
                logger.warning("[stream1] Stream task cancelled")
                break

            except Exception as e:
                logger.exception(
                    "[stream1] Stream crashed, restarting in 3s: %s",
                    e,
                )
                await asyncio.sleep(3)

        logger.warning("[stream1] Stream supervisor stopped")

    stream_task = asyncio.create_task(stream_supervisor())
    logger.info("HLS stream supervisor started")

    # --------------------------------------------------
    # START WEB SERVER
    # --------------------------------------------------
    web_runner = await start_server(port=Telegram.PORT)
    logger.info("Web server started on port %s", Telegram.PORT)

    # --------------------------------------------------
    # GRACEFUL SHUTDOWN
    # --------------------------------------------------
    async def shutdown():
        if shutdown_event.is_set():
            return

        shutdown_event.set()
        logger.warning("Shutdown initiated")

        manager.stop()
        await multi_streamer.stop()

        stream_task.cancel()
        try:
            await stream_task
        except asyncio.CancelledError:
            pass

        # STOP ALL FFMPEG FIRST
        await stop_all_ffmpeg()

        await stop_server(web_runner)
        await ClientManager.stop()

        clean_hls_folder()
        logger.warning("Shutdown completed")

    # --------------------------------------------------
    # SIGNAL HANDLING
    # --------------------------------------------------
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig, lambda: asyncio.create_task(shutdown())
            )
        except NotImplementedError:
            pass

    # --------------------------------------------------
    # BLOCK FOREVER
    # --------------------------------------------------
    try:
        await idle()
    finally:
        await shutdown()


if __name__ == "__main__":
    asyncio.run(main())
