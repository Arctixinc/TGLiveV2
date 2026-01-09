from pyrogram.filters import create
from TGLive import Telegram

class CustomFilters:

    @staticmethod
    async def owner_filter(client, message):
        user = message.from_user or message.sender_chat
        uid = user.id
        return uid == Telegram.OWNER_ID

    owner = create(owner_filter)