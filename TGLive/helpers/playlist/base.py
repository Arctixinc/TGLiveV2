from typing import List, Optional, Tuple, Protocol


class PlaylistStore(Protocol):
    """
    Common playlist DB interface.

    All database backends MUST implement these methods
    with the same behavior.

    Rules:
    - playlist stored OLD → NEW
    - reverse only affects playback
    - append_new only APPENDS
    """

    async def load(
        self, chat_id: int | str
    ) -> Tuple[List[int], Optional[int], Optional[int], bool, Optional[str]]:
        """
        Returns:
            playlist          -> List[int]
            last_started_id   -> Optional[int]
            last_completed_id -> Optional[int]
            reverse           -> bool
            channel_name      -> Optional[str]
        """
        ...

    async def append_new(
        self,
        chat_id: int | str,
        new_ids: List[int],
        reverse: bool = False,
        channel_name: Optional[str] = None,
    ):
        """
        Append new video IDs (no reordering, no duplicates).
        """
        ...

    async def remove_video(self, chat_id: int | str, video_id: int):
        """
        Remove a video ID from playlist.
        """
        ...

    async def set_last_started(self, chat_id: int | str, video_id: int):
        """
        Mark a video as started.
        """
        ...

    async def set_last_completed(self, chat_id: int | str, video_id: int):
        """
        Mark a video as completed.
        """
        ...

    async def get_playlist(self, chat_id: int | str) -> List[int]:
        """
        Return playlist respecting reverse flag.
        """
        ...
