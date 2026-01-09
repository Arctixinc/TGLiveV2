import asyncio
import io
import os
import sys
import time
import traceback
import html
from io import BytesIO

from pyrogram import Client, filters
from pyrogram.enums.parse_mode import ParseMode

from TGLive.helpers.ext_utils.custom_filter import CustomFilters

from TGLive import get_logger

LOGGER = get_logger(__name__)


# ==========================================================
#                     SHELL COMMAND
# ==========================================================
@Client.on_message(filters.command(["shell", "sh"]) & CustomFilters.owner)
@Client.on_edited_message(filters.command(["shell", "sh"]) & CustomFilters.owner)
async def shell_handler(client, message):

    cmd = None

    # ---------- extract command ----------
    if message.reply_to_message:
        r = message.reply_to_message
        if r.text:
            cmd = r.text.strip()
        elif r.caption:
            cmd = r.caption.strip()
        elif r.document and r.document.file_name and r.document.file_name.endswith((".sh", ".txt")):
            path = await r.download()
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    cmd = f.read().strip()
            finally:
                if os.path.exists(path):
                    os.remove(path)

    if not cmd:
        if not message.text or len(message.text.split(maxsplit=1)) < 2:
            await message.reply_text(
                "❗ **Usage:** `/sh <command>`\n\nExample: `/sh ls -la`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        cmd = message.text.split(maxsplit=1)[1].strip()

    # ---------- safety ----------
    FORBIDDEN = ["rm ", "shutdown", "reboot", ":(){", "mkfs", "dd "]
    if any(x in cmd.lower() for x in FORBIDDEN):
        await message.reply_text("🚫 Command blocked for safety")
        return

    status_message = await message.reply_text("⏳ Processing ...")
    LOGGER.info(f"Executing shell command: {cmd[:100]}")

    try:
        start = time.time()

        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=300)

        exec_time = round(time.time() - start, 2)

        out = stdout.decode(errors="ignore").strip() or "No Output"
        err = stderr.decode(errors="ignore").strip() or "No Error"

        result = (
            f"<b>💻 Shell Executed</b>\n\n"
            f"<b>🧾 Command:</b> <code>{html.escape(cmd[:500])}</code>\n"
            f"<b>📌 Return Code:</b> <code>{process.returncode}</code>\n"
            f"<b>⏱️ Time:</b> <code>{exec_time}s</code>\n\n"
            f"<b>⚠️ STDERR:</b>\n<code>{html.escape(err[:2000])}</code>\n\n"
            f"<b>✅ STDOUT:</b>\n<code>{html.escape(out[:2000])}</code>"
        )

        if len(result) > 4096:
            with BytesIO(
                f"Command:\n{cmd}\n\nSTDERR:\n{err}\n\nSTDOUT:\n{out}".encode()
            ) as f:
                f.name = "shell_output.txt"
                await message.reply_document(f)
        else:
            await message.reply_text(result, parse_mode=ParseMode.HTML)

    except Exception as e:
        LOGGER.error("Shell error", exc_info=True)
        await message.reply_text(
            f"⚠️ Error:\n<code>{html.escape(str(e))}</code>",
            parse_mode=ParseMode.HTML
        )
    finally:
        await status_message.delete()


# ==========================================================
#                       EVAL COMMAND
# ==========================================================
@Client.on_message(filters.command(["eval"]) & CustomFilters.owner)
@Client.on_edited_message(filters.command(["eval"]) & CustomFilters.owner)
async def eval_handler(client, message):

    if not message.text or len(message.text.split(maxsplit=1)) < 2:
        await message.reply_text(
            "❗ **Usage:** `/eval <code>`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    code = message.text.split(maxsplit=1)[1]

    status_message = await message.reply_text("⏳ Processing ...")

    try:
        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout, sys.stderr = io.StringIO(), io.StringIO()

        start = time.time()
        error = None

        try:
            await asyncio.wait_for(aexec(code, client, message), timeout=60)
        except Exception:
            error = traceback.format_exc()

        exec_time = round(time.time() - start, 2)

        stdout = sys.stdout.getvalue()
        stderr = sys.stderr.getvalue()

        sys.stdout, sys.stderr = old_stdout, old_stderr

        output = error or stderr or stdout or "✅ Success"

        await message.reply_text(
            f"<b>🧠 EVAL</b>\n\n"
            f"<b>⏱️ Time:</b> <code>{exec_time}s</code>\n\n"
            f"<code>{html.escape(output[:4000])}</code>",
            parse_mode=ParseMode.HTML
        )

    finally:
        await status_message.delete()


# ==========================================================
#                   ASYNC EXECUTOR
# ==========================================================
async def aexec(code, client, message):
    env = {
        "__builtins__": __builtins__,
        "client": client,
        "message": message,
        "asyncio": asyncio,
        "os": os,
        "sys": sys,
        "time": time,
    }

    exec(
        "async def __aexec(client, message):\n"
        + "\n".join(f"    {line}" for line in code.splitlines()),
        env
    )
    return await env["__aexec"](client, message)
