import asyncio
from asyncio import gather
from pyrogram import idle

from TGLive import setup_logging, get_logger, __title__, __version__, Telegram
from TGLive.helpers.client import ClientManager
from TGLive.helpers.playlist import VideoPlaylistManager
from TGLive.helpers.database import JsonPlaylistStore


async def main():
    setup_logging()
    logger = get_logger(__name__)
    logger.info(f"Starting {__title__} version {__version__}...")

    await gather(
        ClientManager.start(),
        ClientManager.start_multi_clients(),
    )

    store = JsonPlaylistStore()
    client = ClientManager.multi_clients.get(14)

    if not client:
        raise RuntimeError("Client 14 not found")

    logger.info(
        "Using client | index=%s | session=%s | username=%s",
        14,
        client.name,
        getattr(client.me, "username", None),
    )


    manager = VideoPlaylistManager(
        client=client,
        chat_id=Telegram.BEN_ID,
        store=store,
    )

    # ✅ build/load playlist
    await manager.build()

    # ✅ get playlist via manager
    playlist = manager.get_playlist()
    logger.info("Playlist for BEN_ID: %s", playlist)

    await idle()


if __name__ == "__main__":
    asyncio.run(main())
