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
    stream_tasks = []

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
    # WORKER POOL (ROUND-ROBIN)
    # --------------------------------------------------
    worker_ids = list(ClientManager.multi_clients.keys())
    if not worker_ids:
        raise RuntimeError("No Telegram worker clients available")

    worker_index = 0

    # --------------------------------------------------
    # STREAM STARTER
    # --------------------------------------------------
    async def start_stream(stream_index: int, chat_id: int):
        nonlocal worker_index

        stream_name = f"stream{stream_index}"
        stream_logger = get_logger(stream_name)

        # round-robin worker assignment
        worker_id = worker_ids[worker_index % len(worker_ids)]
        worker_index += 1

        client = ClientManager.multi_clients[worker_id]

        stream_logger.info(
            "[%s] Using worker=%s session=%s username=%s",
            stream_name,
            worker_id,
            client.name,
            getattr(client.me, "username", None),
        )

        store = JsonPlaylistStore()

        manager = VideoPlaylistManager(
            client=client,
            chat_id=chat_id,
            store=store,
            auto_checker=True,
        )
        await manager.build()

        multi_streamer = MultiClientStreamer()

        playlist_generator = PlaylistStreamGenerator(
            playlist_manager=manager,
            multi_streamer=multi_streamer,
            stream_name=stream_name,
        )

        hls_dir = f"hls/{stream_name}"

        async def supervisor():
            stream_logger.info("[%s] Supervisor started", stream_name)

            while not shutdown_event.is_set():
                try:
                    async for video_id, ts_source in playlist_generator.iter_videos():
                        if shutdown_event.is_set():
                            break

                        stream_logger.info(
                            "[%s] Starting video %s",
                            stream_name,
                            video_id,
                        )

                        start_number = get_last_segment_number(hls_dir)

                        await start_hls_runner(
                            ts_source=ts_source,
                            hls_dir=hls_dir,
                            stream_name=stream_name,
                            start_number=start_number,
                        )

                        stream_logger.info(
                            "[%s] Finished video %s",
                            stream_name,
                            video_id,
                        )

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    stream_logger.exception(
                        "[%s] Stream crashed, restarting in 3s: %s",
                        stream_name,
                        e,
                    )
                    await asyncio.sleep(3)

            stream_logger.warning("[%s] Supervisor stopped", stream_name)

        return asyncio.create_task(supervisor())

    # --------------------------------------------------
    # START ALL STREAMS (stream1, stream2, ...)
    # --------------------------------------------------
    for i, chat_id in enumerate(Telegram.STREAM_DB_IDS, start=1):
        task = await start_stream(i, chat_id)
        stream_tasks.append(task)

    logger.info("Started %d streams", len(stream_tasks))

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

        for task in stream_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

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
