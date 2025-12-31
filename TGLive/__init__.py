from time import time
from datetime import datetime
import logging
from logging import (
    FileHandler,
    StreamHandler,
    Formatter,
    getLogger,
    DEBUG,
    INFO,
    WARNING,
    ERROR,
)
import pytz

from .version import get_version
from .config import Telegram

START_TIME = time()

__title__ = "TGLive V2"
__version__ = get_version()
__author__ = "Arctix Inc."
__license__ = "MIT"
__copyright__ = "Copyright 2026, Arctix Inc."

DEBUG_MODE = Telegram.DEBUG_MODE
IST = pytz.timezone("Asia/Kolkata")


class ISTFormatter(Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, IST)
        return dt.strftime(datefmt or "%Y-%m-%d %I:%M:%S %p")


LOG_FORMAT = (
    "[%(asctime)s] "
    "[%(levelname)s] "
    "[%(name)s] "
    "[%(filename)s:%(lineno)d] "
    "%(message)s"
)


def setup_logging():
    root = logging.getLogger()

    if root.handlers:
        return

    root.setLevel(DEBUG if DEBUG_MODE else INFO)

    formatter = ISTFormatter(LOG_FORMAT)

    file_handler = FileHandler("log.txt", mode="w", encoding="utf-8")
    stream_handler = StreamHandler()

    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    root.addHandler(file_handler)
    root.addHandler(stream_handler)

    noisy_loggers = {
        "requests": WARNING,
        "urllib3": WARNING,
        "httpx": WARNING,
        "pymongo": WARNING,
        "pyrogram": ERROR,
        "aiohttp": ERROR,
        "apscheduler": ERROR,
    }

    for name, level in noisy_loggers.items():
        getLogger(name).setLevel(level)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


__all__ = (
    "START_TIME",
    "__title__",
    "__version__",
    "__author__",
    "__license__",
    "setup_logging",
    "get_logger",
    "Telegram",
)
