from typing import AsyncGenerator, Union
from pyrogram import Client, types

class LiveTgClient(Client):
    async def iter_messages(
        self,
        chat_id: Union[int, str],
        limit: int,
        offset: int = 0,
    ) -> AsyncGenerator["types.Message", None]:

        current = offset
        while current < limit:
            chunk = min(200, limit - current)
            ids = list(range(current, current + chunk))

            messages = await self.get_messages(chat_id, ids)
            for msg in messages:
                if msg:
                    yield msg

            current += chunk
