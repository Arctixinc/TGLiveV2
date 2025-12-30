from time import time
from .version import get_version

import logging
from logging import FileHandler, StreamHandler, DEBUG, INFO, Formatter, getLogger, WARNING, ERROR
from datetime import datetime
import pytz

START_TIME = time()

__title__ = "TGLive V2"
__version__ = get_version()
__author__ = "Arctix Inc."
__license__ = "MIT"
__copyright__ = "Copyright 2026, Arctix Inc."

DEBUG_MODE = False
IST = pytz.timezone("Asia/Kolkata")

class ISTFormatter(Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, IST)
        return dt.strftime(datefmt or "%d-%b-%y %I:%M:%S %p")

LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s"

def setup_logging():
    formatter = ISTFormatter(LOG_FORMAT)

    handlers = [
        FileHandler("log.txt", "w"),
        StreamHandler(),
    ]

    for handler in handlers:
        handler.setFormatter(formatter)

    logging.basicConfig(
        level=DEBUG if DEBUG_MODE else INFO,
        handlers=handlers,
    )
    
    getLogger("requests").setLevel(WARNING)
    getLogger("urllib3").setLevel(WARNING)
    getLogger("httpx").setLevel(WARNING)
    getLogger("pymongo").setLevel(WARNING)

    getLogger("pyrogram").setLevel(ERROR)
    getLogger("aiohttp").setLevel(ERROR)
    getLogger("apscheduler").setLevel(ERROR)

def get_logger(name: str):
    return logging.getLogger(name)

__all__ = (
    "START_TIME",
    "__title__",
    "__version__",
    "__author__",
    "__license__",
    "setup_logging",
    "get_logger",
)
