import asyncio
from TGLive import get_logger
from TGLive.helpers.encoding.cleaner import ffmpeg_cleaner

LOGGER = get_logger(__name__)


class PlaylistStreamGenerator:
    """
    Generates (video_id, ts_source) pairs
    using VideoPlaylistManager + MultiClientStreamer
    """

    def __init__(self, playlist_manager, multi_streamer, stream_name: str):
        self.manager = playlist_manager
        self.multi_streamer = multi_streamer
        self.stream_name = stream_name

    async def iter_videos(self):
        """
        Infinite playlist iterator.
        Safe for restarts.
        """

        while True:
            playlist = await self.manager.get_playlist()

            if not playlist:
                LOGGER.warning(
                    "[%s] Playlist empty, waiting 5s",
                    self.stream_name,
                )
                await asyncio.sleep(5)
                continue

            for video_id in playlist:
                # 🔒 RAW TELEGRAM BYTES
                raw_source = self.multi_streamer.stream_video(
                    chat_id=self.manager.chat_id,
                    message_id=video_id,
                    stream_name=self.stream_name,
                )

                # 🔒 CLEAN → MPEG-TS (MANDATORY)
                ts_source = ffmpeg_cleaner(
                    raw_source,
                    self.stream_name,
                )

                yield video_id, ts_source
