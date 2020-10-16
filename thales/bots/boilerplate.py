"""Boilerplate for implementing a very simple trading bot based on the Observer
pattern for event-handling. In this situation the data source can be thought of
as the event, and handlers are called each time new data is produced.
"""

import datetime
import json
import os

from thales.bots import DataSource, EventHandler, TradingBot
from thales.config.bots import register_bot, validate_bot_name
from thales.config.paths import io_path
from thales.positions import PositionManager


# ==============================================================================
# This variable needs to be a unique name which is a valid Python identifier:
BOT_NAME = "test"
# ==============================================================================
try:
    BOT_NAME = validate_bot_name(BOT_NAME)
except AssertionError:
    register_bot(BOT_NAME)
BOT_DIR = io_path("bot_data", BOT_NAME)


class TradeHandler(EventHandler):
    data_dir = os.path.join(BOT_DIR, "handler_data")
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    def __init__(self, positions: PositionManager):
        super().__init__()
        self.positions = positions

    def __call__(self, **kwargs):
        # ======================================================================
        # Logic for what the handler does when called with some data goes here.
        # In this example it just saves the data as a timestamped JSON file.
        ts = kwargs["timestamp"].strftime(format="%Y_%m_%d %H;%M;%S;%f")
        kwargs["timestamp"] = ts
        fp = os.path.join(self.data_dir, f"{ts}.json")
        with open(fp, "w") as f:
            json.dump(kwargs, f)
        # ======================================================================


class TestSource(DataSource):
    def __init__(self):
        super().__init__()

    def generator(self):
        # ======================================================================
        # Logic to produce data goes here:
        data = dict(timestamp=datetime.datetime.now(), open=None, high=None, low=None, close=None)
        while True:
            yield data
        # ======================================================================


class Bot(TradingBot):
    def __init__(self, src: TestSource, *handler: TradeHandler):
        super().__init__(src, *handler)

    def __call__(self):
        generator = self.src.generator()
        while True:
            data = next(generator)
            for handler in self.handlers:
                handler(**data)


if __name__ == "__main__":
    position_handler = PositionManager(bot_name=BOT_NAME, test=True, create_test_dir=True)
    event_handlers = [TradeHandler(positions=position_handler)]
    data_source = TestSource()
    bot = Bot(data_source, *event_handlers)
    bot()
