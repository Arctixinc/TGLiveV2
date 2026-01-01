from pyrogram import Client, filters
from TGLive import __title__, __version__, get_logger
from TGLive.helpers.client import ClientManager

LOGGER = get_logger(__name__)


@Client.on_message(filters.command("bot") & filters.private)
async def start_cmd(client, message):
    user_id = message.from_user.id if message.from_user else None
    LOGGER.info("Start command received from user_id=%s", user_id)

    # -------------------------------
    # MULTI CLIENT INFO
    # -------------------------------
    multi_clients = ClientManager.multi_clients
    work_loads = ClientManager.work_loads

    if multi_clients:
        client_lines = []
        for cid, c in sorted(multi_clients.items()):
            load = work_loads.get(cid, 0)
            uname = getattr(c.me, "username", "unknown")
            client_lines.append(
                f"• <b>ID:</b> <code>{cid}</code> | "
                f"<b>User:</b> @{uname} | "
                f"<b>Load:</b> <code>{load}</code>"
            )

        clients_text = "\n".join(client_lines)
    else:
        clients_text = "No worker clients active."

    text = (
        f"👋 <b>Welcome to {__title__}</b>\n\n"
        f"🚀 <b>Version:</b> <code>{__version__}</code>\n\n"
        f"🤖 <b>Main Bot:</b> @{ClientManager.BNAME}\n"
        f"🧩 <b>Helper Bot:</b> @{ClientManager.HNAME}\n\n"
        f"👥 <b>Multi Clients:</b> <code>{len(multi_clients)}</code>\n"
        f"{clients_text}\n\n"
        f"📡 <b>Status:</b> Streaming service is running.\n"
        f"ℹ️ Loads increase automatically while streaming."
    )

    await message.reply_text(
        text,
        disable_web_page_preview=True,
    )
