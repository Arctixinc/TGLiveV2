import asyncio
from asyncio import gather
from pyrogram import idle

from TGLive import setup_logging, get_logger, __title__, __version__, Telegram
from TGLive.helpers.client import ClientManager
from TGLive.helpers.playlist import VideoPlaylistManager
from TGLive.helpers.database import JsonPlaylistStore
from TGLive.web.server import start_server


async def main():
    setup_logging()
    logger = get_logger(__name__)
    logger.info("Starting %s version %s...", __title__, __version__)

    await gather(
        ClientManager.start(),
        ClientManager.start_multi_clients(),
    )

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

    store = JsonPlaylistStore()

    manager = VideoPlaylistManager(
        client=client,
        chat_id=Telegram.LOCAL_ID,
        store=store,
    )

    await manager.build()
    playlist = manager.get_playlist()
    logger.info("Playlist loaded (%d items)", len(playlist))

    await start_server(port=8000)

    await idle()


if __name__ == "__main__":
    asyncio.run(main())
