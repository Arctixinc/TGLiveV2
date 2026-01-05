from aiohttp import web
from TGLive import Telegram

DEFAULT_QUALITY = "720p"


async def playlist_handler(request: web.Request):
    """
    IPTV-style M3U playlist (EXTINF)
    Generated from STREAM_DB_IDS
    """
    base_url = f"{request.scheme}://{request.host}"

    lines = ["#EXTM3U"]

    for idx, _chat_id in enumerate(Telegram.STREAM_DB_IDS, start=1):
        stream_name = f"stream{idx}"

        tvg_id = f"{stream_name}@TG"
        name = stream_name
        quality = DEFAULT_QUALITY

        lines.append(
            f'#EXTINF:-1 tvg-id="{tvg_id}",{name} ({quality})'
        )
        lines.append(
            f"{base_url}/hls/{stream_name}/live.m3u8"
        )

    return web.Response(
        text="\n".join(lines) + "\n",
        content_type="application/vnd.apple.mpegurl",
    )
