import asyncio
from asyncio import gather
from pyrogram import idle

from TGLive import setup_logging, get_logger, __title__, __version__
from TGLive.helpers.client import ClientManager


async def main():
    setup_logging()
    logger = get_logger(__name__)
    logger.info(f"Starting {__title__} version {__version__}...")

    await gather(
        ClientManager.start(),
        ClientManager.start_multi_clients(),
    )

    await idle()


if __name__ == "__main__":
    asyncio.run(main())
