import os
import shutil
import glob
from typing import Optional
from traceback import format_exc

from pyrogram import Client
from pyrogram.file_id import FileId

from .exception import FIleNotFound

from TGLive import get_logger

LOGGER = get_logger(__name__)

# =========================
# MEDIA CHECKER
# =========================

def is_media(message):
    """Returns the media object if message contains any supported media."""
    media_attrs = [
        "document",
        "photo",
        "video",
        "audio",
        "voice",
        "video_note",
        "sticker",
        "animation",
    ]

    for attr in media_attrs:
        media = getattr(message, attr, None)
        if media:
            return media

    return None


# =========================
# FILE ID (FULL MEDIA DETAIL)
# =========================

async def get_file_ids(
    client: Client,
    chat_id: int,
    message_id: int
) -> Optional[FileId]:
    """
    Returns a decoded FileId with additional attributes:
        - file_name
        - file_size
        - mime_type
        - unique_id

    Raises FIleNotFound when:
        - Message empty
        - No media in message
    """
    try:
        message = await client.get_messages(chat_id, message_id)

        if not message or message.empty:
            raise FIleNotFound("Message not found or empty")

        media = is_media(message)
        if not media:
            raise FIleNotFound("No supported media found in message")

        # Decode FileId
        file_id_obj = FileId.decode(media.file_id)

        # Attach useful fields
        setattr(file_id_obj, "file_name", getattr(media, "file_name", None))
        setattr(file_id_obj, "file_size", getattr(media, "file_size", 0))
        setattr(file_id_obj, "mime_type", getattr(media, "mime_type", None))
        setattr(file_id_obj, "unique_id", media.file_unique_id)

        return file_id_obj
    
    except FIleNotFound:
        raise

    except Exception:
        LOGGER.error("Error getting file IDs")
        LOGGER.error(format_exc())
        raise


# =========================
# READABLE TIME FORMATTER
# =========================

def get_readable_time(seconds: int) -> str:
    """
    Converts seconds into a human-readable format.

    Examples:
        65     -> "1m: 5s"
        3725   -> "1h: 2m: 5s"
        90000  -> "1 days, 1h: 0m"
    """
    count = 0
    readable_time = ""
    time_list = []
    time_suffix_list = ["s", "m", "h", " days"]

    while count < 4:
        count += 1

        if count < 3:
            remainder, result = divmod(seconds, 60)
        else:
            remainder, result = divmod(seconds, 24)

        if seconds == 0 and remainder == 0:
            break

        time_list.append(int(result))
        seconds = int(remainder)

    # Attach suffixes
    for i in range(len(time_list)):
        time_list[i] = f"{time_list[i]}{time_suffix_list[i]}"

    # Days format
    if len(time_list) == 4:
        readable_time += time_list.pop() + ", "

    time_list.reverse()
    readable_time += ": ".join(time_list)

    return readable_time


# =========================
# HLS FOLDER CLEANER
# =========================

def clean_hls_folder(base_dir: str = "hls"):
    """Deletes all files and folders inside the HLS directory."""
    if not os.path.exists(base_dir):
        return

    try:
        for name in os.listdir(base_dir):
            path = os.path.join(base_dir, name)
            try:
                if os.path.isfile(path) or os.path.islink(path):
                    os.unlink(path)
                elif os.path.isdir(path):
                    shutil.rmtree(path)
            except Exception:
                LOGGER.warning(f"Failed to remove {path}")

        LOGGER.info("HLS folder cleaned successfully")

    except Exception:
        LOGGER.error("Failed to clean HLS folder")
        LOGGER.error(format_exc())




def clean_hls_stream(stream_name: str):
    path = os.path.join("hls", stream_name)
    if not os.path.isdir(path):
        return

    for f in glob.glob(os.path.join(path, "*.ts")):
        os.remove(f)

    for f in glob.glob(os.path.join(path, "*.m3u8")):
        os.remove(f)

    LOGGER.info(f"[{stream_name}] HLS stream folder cleaned")
