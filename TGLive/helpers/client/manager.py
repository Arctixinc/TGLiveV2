import asyncio
from typing import Dict, Optional
from asyncio import gather, sleep

from pyrogram import Client, enums
from pyrogram.errors import AccessTokenExpired, FloodWait

from TGLive.config import Telegram
from .base import LiveTgClient
from TGLive import get_logger

LOGGER = get_logger(__name__)


class TokenParser:
    @staticmethod
    def parse_from_config():
        return {
            i + 1: value
            for i, (key, value) in enumerate(
                sorted(
                    (k, v)
                    for k, v in vars(Telegram).items()
                    if k.startswith("MULTI_TOKEN") and v
                )
            )
        }


class ClientManager:
    bot: Optional[Client] = None
    user: Optional[Client] = None
    helper: Optional[LiveTgClient] = None

    multi_clients: Dict[int, LiveTgClient] = {}
    work_loads: Dict[int, int] = {}

    BNAME: Optional[str] = None
    ID: Optional[str] = None
    HNAME: Optional[str] = None

    _lock = asyncio.Lock()

    @classmethod
    async def start(cls):
        async with cls._lock:
            if cls.bot:
                return

            kwargs = {
                "parse_mode": enums.ParseMode.HTML,
                "max_concurrent_transmissions": 100,
                "skip_updates": False,
            }

            cls.bot = Client(
                "sessions/TGL-Main",
                api_id=Telegram.API_ID,
                api_hash=Telegram.API_HASH,
                bot_token=Telegram.BOT_TOKEN,
                plugins={"root": "TGLive/plugins"},
                **kwargs,
            )
            await cls.bot.start()

            cls.BNAME = cls.bot.me.username
            cls.ID = Telegram.BOT_TOKEN.split(":", 1)[0]

            LOGGER.info(f"TGL Bot : [@{cls.BNAME}] Started!")

            cls.helper = LiveTgClient(
                "sessions/TGL-Helper",
                api_id=Telegram.API_ID,
                api_hash=Telegram.API_HASH,
                bot_token=Telegram.HELPER_BOT_TOKEN,
                **kwargs,
            )
            await cls.helper.start()

            cls.HNAME = cls.helper.me.username if cls.helper.me else "Unknown"

            cls.multi_clients[0] = cls.helper
            cls.work_loads[0] = 0

            LOGGER.info(f"TGL Helper : [@{cls.HNAME}] Started!")

    @classmethod
    async def start_user(cls):
        async with cls._lock:
            if cls.user or not Telegram.USER_SESSION:
                return

            cls.user = Client(
                "sessions/TGL-User",
                api_id=Telegram.API_ID,
                api_hash=Telegram.API_HASH,
                session_string=Telegram.USER_SESSION,
                parse_mode=enums.ParseMode.HTML,
            )
            await cls.user.start()

            LOGGER.info("TGL User client started")

    @classmethod
    async def start_multi_clients(cls):
        tokens = TokenParser.parse_from_config()
        if not tokens:
            return

        async with cls._lock:
            for cid, token in tokens.items():
                try:
                    name = f"sessions/TGL-HBot{cid}"

                    client = LiveTgClient(
                        name,
                        api_id=Telegram.API_ID,
                        api_hash=Telegram.API_HASH,
                        bot_token=token,
                        no_updates=True,
                        parse_mode=enums.ParseMode.HTML,
                        max_concurrent_transmissions=100,
                    )
                    await client.start()

                    cls.multi_clients[cid] = client
                    cls.work_loads[cid] = 0

                    LOGGER.info(f"TGL Worker {cid} : [{name}] Started")

                except AccessTokenExpired:
                    LOGGER.warning(f"[Worker {cid}] Token expired")

                except FloodWait as e:
                    LOGGER.warning(f"[Worker {cid}] FloodWait {e.value}s")
                    await sleep(e.value)

                except Exception:
                    LOGGER.exception(f"[Worker {cid}] Failed to start")

            LOGGER.info(f"Worker clients active: {list(cls.multi_clients.keys())}")

    @classmethod
    async def stop(cls):
        async with cls._lock:
            if cls.bot:
                await cls.bot.stop()
                cls.bot = None

            if cls.user:
                await cls.user.stop()
                cls.user = None

            if cls.multi_clients:
                await gather(*[c.stop() for c in cls.multi_clients.values()])
                cls.multi_clients.clear()
                cls.work_loads.clear()

            cls.helper = None

            LOGGER.info("All Client(s) stopped")

    @classmethod
    async def reload(cls):
        async with cls._lock:
            if cls.bot:
                await cls.bot.restart()

            if cls.user:
                await cls.user.restart()

            if cls.multi_clients:
                await gather(*[c.restart() for c in cls.multi_clients.values()])

            LOGGER.info("All Client(s) restarted")
