import asyncio
import signal
from asyncio import gather
from pyrogram import idle

from TGLive import setup_logging, get_logger, __title__, __version__, Telegram

from TGLive.helpers.client import ClientManager
from TGLive.helpers.playlist import (
    VideoPlaylistManager,
    PlaylistStreamGenerator,
)
from TGLive.helpers.database import JsonPlaylistStore
from TGLive.helpers.ffmpeg import (
    start_hls_runner,
    stop_all_ffmpeg,
    MultiClientStreamer,
)
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
    # STREAM PIPELINE
    # --------------------------------------------------
    multi_streamer = MultiClientStreamer()

    playlist_generator = PlaylistStreamGenerator(
        playlist_manager=manager,
        multi_streamer=multi_streamer,
        stream_name="stream1",
    )

    async def run_stream():
        await start_hls_runner(
            ts_source=playlist_generator.generator(),
            hls_dir="hls/stream1",
            stream_name="stream1",
        )

    stream_task = asyncio.create_task(run_stream())
    logger.info("HLS stream task started")

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

        # stop playlist auto checker
        manager.stop()

        await multi_streamer.stop()

        # stop stream task
        stream_task.cancel()
        try:
            await stream_task
        except asyncio.CancelledError:
            pass

        # stop ffmpeg BEFORE cleaning
        await stop_all_ffmpeg()

        # stop web server
        await stop_server(web_runner)

        # stop telegram clients
        await ClientManager.stop()

        # --------------------------------------------------
        # CLEAN HLS FOLDER (STOP) ✅
        # --------------------------------------------------
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
            pass  # Windows fallback

    # --------------------------------------------------
    # BLOCK FOREVER
    # --------------------------------------------------
    try:
        await idle()
    finally:
        await shutdown()


if __name__ == "__main__":
    asyncio.run(main())
