"""Implementation of an FX trading bot."""

import datetime
import json
import os
import pandas as pd
import random
import time

from thales.config.bots import register_bot, validate_bot_name
from thales.config.paths import DIR_BOT_DATA
from thales.data import load_toy_dataset
from thales.positions import ManagePositions, Position


BOT_NAME = "FoXyLady"
TEST = True
try:
    BOT_NAME = validate_bot_name(BOT_NAME)
except AssertionError:
    register_bot(BOT_NAME)
BOT_DIR = os.path.join(DIR_BOT_DATA, BOT_NAME)


date_format = "%Y_%m_%d"
milisecond_format = "%Y_%m_%d %H;%M;%S;%f"
minute_format = "%Y_%m_%d %H;%M"


class Handler67:

    data_dir = os.path.join(BOT_DIR, "67data")
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    def __init__(self):
        self.lows = dict()
        self.highs = dict()
        self.means = dict()

    def __call__(self, **kwargs):
        timestamp = kwargs["timestamp"]
        if timestamp.hour in (6, 7):
            date = datetime.date(timestamp.year, timestamp.month, timestamp.day)
            low = kwargs["low"]
            high = kwargs["high"]
            changed = False
            if date not in self.lows or low < self.lows[date]:
                self.lows[date] = low
                changed = True
            if date not in self.highs or high > self.highs[date]:
                self.highs[date] = high
                changed = True
            if changed:
                self.means[date] = (self.lows[date] + self.highs[date]) / 2
                ts = timestamp.strftime(date_format)
                fp = os.path.join(self.data_dir, f"{ts}.json")
                data = {"low": self.lows[date], "high": self.highs[date], "mean": self.means[date]}
                with open(fp, "w") as f:
                    json.dump(data, f)


class HandlerTrades:

    data_dir = os.path.join(BOT_DIR, "trade_data")
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    def __init__(self):
        self.mean_67 = dict()

    def __call__(self, **kwargs):
        timestamp = kwargs["timestamp"]
        if timestamp.hour not in (6, 7):
            date = datetime.date(timestamp.year, timestamp.month, timestamp.day)
            if date not in self.mean_67:
                ts = timestamp.strftime(date_format)
                fp = os.path.join(BOT_DIR, "67data", f"{ts}.json")
                with open(fp, "r") as f:
                    self.mean_67[date] = json.load(fp)["mean"]
            mean = self.mean_67[date]
            high = kwargs["high"]
            low = kwargs["low"]
            close = kwargs["close"]
            open_positions = ManagePositions.list_open_positions(bot_name=BOT_NAME, test=TEST)
            position_timestamp = timestamp.strftime(milisecond_format)
            if open_positions:
                for p in open_positions:
                    position = ManagePositions.get_position(p)
                    stop_signal = low < (position.buy_price - 0.2)
                    sell_signal = high > (position.buy_price + 0.3)
                    if stop_signal or sell_signal:
                        position.sell(timestamp=position_timestamp, price=close)
            else:
                buy_signal = (high > mean + 0.2) and (close < mean + 0.3)
                if buy_signal:
                    Position(open_timestamp=position_timestamp, buy_price=close,
                             amount=100, test=TEST, bot_name=BOT_NAME)


class TestDataSource:

    data_format = {
        "timestamp": datetime.datetime.now(),
        "open": random.random(),
        "high": random.random(),
        "low": random.random(),
        "close": random.random(),
    }

    def __init__(self, *handler):
        self.handlers = list(handler)
        self.year: int = 0
        self.year_df = pd.DataFrame()

    def __call__(self, dt: datetime.datetime):
        data = self.get_minute_data(dt)
        for handler in self.handlers:
            handler(**data)

    def load_year_data(self, year: int):
        fn = f"GBPJPY_{year}_1m"
        self.year_df = load_toy_dataset(fn)
        self.year_df["date_str"] = self.year_df["datetime"].dt.strftime(minute_format)
        self.year = year

    def get_minute_data(self, dt: datetime.datetime):
        year = dt.year
        if not year == self.year:
            self.load_year_data(year)
        minute_str = dt.strftime(minute_format)
        row = self.year_df.loc[self.year_df["date_str"] == minute_str]
        if len(row) == 1:
            d = row.iloc[0].to_dict()
            d["timestamp"] = d["date_str"]
            d = {k: v for k, v in d.items() if k in self.data_format}
            return d
        elif len(row) == 0:
            return None
        else:
            raise IndexError(f"Multiple rows returned for single minute: {minute_str}")


class Bot:

    def __init__(self, src: TestDataSource, interval: float = 15):
        self.src = src
        self.interval = interval

    def __call__(self):
        while True:
            self.src()
            time.sleep(self.interval)


if __name__ == "__main__":
    event_handlers = [Handler67(), HandlerTrades()]
    data_source = TestDataSource(*event_handlers)
    bot = Bot(data_source)
    bot()
