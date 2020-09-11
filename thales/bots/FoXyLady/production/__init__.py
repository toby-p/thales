"""Implementation of an FX trading bot."""

import datetime
import os
import random
import time

from thales.config.bots import register_bot, validate_bot_name
from thales.config.paths import io_path
from thales.config.utils import DAY_FORMAT


# BOT SETUP
# ==============================================================================
BOT_NAME = "FoXyLady"
TEST = False
try:
    BOT_NAME = validate_bot_name(BOT_NAME)
except AssertionError:
    register_bot(BOT_NAME)
BOT_DIR = io_path("bot_data", BOT_NAME)
DATA_DIR = os.path.join(BOT_DIR, "TEST" if TEST else "PRODUCTION")
SUBDIRS = ["67_data"]
if not os.path.isdir(DATA_DIR):
    os.mkdir(DATA_DIR)
for sd in SUBDIRS:
    subdir = os.path.join(DATA_DIR, sd)
    if not os.path.isdir(subdir):
        os.mkdir(subdir)


# HELPER FUNCTIONS:
# ==============================================================================
class Data67:

    data_dir = os.path.join(DATA_DIR, "67_data")
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    def __init__(self):
        self.data = dict()

    def __call__(self, dt: datetime.datetime):
        date_str = dt.strftime(DAY_FORMAT)
        try:
            return self.data[date_str]
        except KeyError:
            raise NotImplementedError()


data_67 = Data67()


# EVENT HANDLERS:
# ==============================================================================
class TradeHandler:

    data_dir = os.path.join(DATA_DIR, "trade_data")
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    def __init__(self):
        pass

    def __call__(self, **kwargs):
        timestamp = kwargs["timestamp"]
        if timestamp.hour not in (6, 7):
            raise NotImplementedError()


# EVENTS (DATA SOURCES):
# ==============================================================================
class DataSource:

    data_format = {
        "timestamp": datetime.datetime.now(),
        "open": random.random(),
        "high": random.random(),
        "low": random.random(),
        "close": random.random(),
    }

    def __init__(self, *handler):
        self.handlers = list(handler)
        self.start_datetime = datetime.datetime.now()
        self.query_datetime = datetime.datetime.now()

    def __call__(self, *args, **kwargs):
        raise NotImplementedError()


# BOT PROGRAM
# ==============================================================================
class Bot:

    def __init__(self, src: DataSource, interval: float = 1):
        self.src = src
        self.interval = interval

    def __call__(self):
        while True:
            self.src()
            time.sleep(self.interval)


if __name__ == "__main__":
    event_handlers = [TradeHandler()]
    data_source = DataSource(*event_handlers)
    bot = Bot(data_source)
    bot()
