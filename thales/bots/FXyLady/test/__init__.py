"""Implementation of an FX trading bot."""

import datetime
import json
import os
import pandas as pd
import random

from thales.config.bots import register_bot, validate_bot_name
from thales.config.paths import io_path
from thales.config.utils import DATE_FORMAT, MILISECOND_FORMAT, MINUTE_FORMAT
from thales.data import load_toy_dataset
from thales.positions import ManagePositions, Position


# BOT SETUP
# ==============================================================================
BOT_NAME = "FoXyLady"
TEST = True
TEST_START = datetime.datetime(2009, 8, 8, 5, 59, 0)
TEST_DURATION = datetime.timedelta(days=365)
try:
    BOT_NAME = validate_bot_name(BOT_NAME)
except AssertionError:
    register_bot(BOT_NAME)
BOT_DIR = io_path("bot_data", BOT_NAME)
DATA_DIR = os.path.join(BOT_DIR, "TEST" if TEST else "PRODUCTION")


# DATA SOURCE:
# ==============================================================================
class TestDataGenerator:

    def __init__(self):
        self.year = 0
        self.year_df = pd.DataFrame()
        date_fp = io_path("bot_data", "FXyLady", "test", filename="dates.csv")
        dates = pd.read_csv(date_fp, encoding="utf-8")
        dates["dt"] = pd.to_datetime(dates["dates"], format="%Y_%m_%d")
        self.dates = dates["dt"].sort_values().reset_index(drop=True)
        self._67_data = dict()

    def get_67(self, dt: datetime.datetime):
        date_str = dt.strftime(DATE_FORMAT)
        try:
            return self._67_data[date_str]
        except KeyError:
            fp = io_path("bot_data", "FXyLady", "test", "67_data", f"{date_str}.json")
            if os.path.exists(fp):
                with open(fp, "r") as f:
                    data = json.load(f)
                    self._67_data[date_str] = data
                    return data
            else:
                return dict()

    def _load_year(self, year: int):
        if self.year != year:
            fn = f"GBPJPY_{year}_1m.csv"
            self.year_df = load_toy_dataset(fn).sort_values(by=["datetime"])
            self.year = year

    def previous_date(self, dt: datetime.date):
        dt_ix = self.dates[self.dates == dt].index
        try:
            return self.dates.loc[dt_ix - 1].iloc[0].date()
        except IndexError:
            raise IndexError(f"previous_date failed on date: {dt}")

    def generator(self, start: datetime.datetime, n_days: int = 100):

        # Initial parameters:
        day_count = 0
        self._load_year(start.year)
        ix = self.year_df.loc[self.year_df["datetime"] >= start].index[0]
        current_date = start.date()

        while day_count < n_days:

            # Get the minute's data:
            row = self.year_df.loc[ix]
            row_date = row["datetime"].date()
            data_minute = row.to_dict()

            # If the row is from a new day, increase the counter:
            if row_date > current_date:
                current_date = row_date
                day_count += 1

            # Get the 6-7am data. If the hour is less than 6am, need the
            # previous day's numbers:
            date_need_67 = datetime.datetime(row_date.year, row_date.month, row_date.day)
            if row["datetime"].hour < 6:
                date_need_67 = self.previous_date(date_need_67)
            data_67 = self.get_67(date_need_67)

            # Increment the index position:
            ix += 1
            # If we've reached the end of the year_df, load the next year:
            if ix > self.year_df.index.max():
                self._load_year(self.year + 1)
                ix = 0

            if data_67 and data_minute:
                yield {"67": data_67, "minute": data_minute}
        return


# EVENT HANDLERS:
# ==============================================================================
class TestTradeHandler:

    def __init__(self):
        self.data_obj = TestGetData()

    def __call__(self, dt: datetime.datetime, high: float, low: float,
                 close: float):
        data_67 = self.data_obj.get_67(dt)
        data_minute = self.data_obj.get_minute(dt)
        if (not data_67) or (not data_minute):
            return  # Not enough data was returned to make a decision.
        timestamp = kwargs["timestamp"]
        print("timestamp: ", timestamp)
        if timestamp.hour not in (6, 7):
            date_needed = datetime.datetime(timestamp.year, timestamp.month, timestamp.day)
            if timestamp.hour < 6:
                date_needed = date_needed - datetime.timedelta(days=1)
            print("date_needed: ", date_needed)
            baseline_data = data_67(date_needed)
            if baseline_data:
                mean = baseline_data["mean"]
                open_positions = ManagePositions.list_open_positions(bot_name=BOT_NAME, test=TEST)
                position_timestamp = timestamp.strftime(MILISECOND_FORMAT)
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


# EVENTS (DATA SOURCES):
# ==============================================================================
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
        self.start_datetime = TEST_START
        self.year = 0
        self.year_df = pd.DataFrame()

    def __call__(self, dt: datetime.datetime):
        data = self._get_minute_data(dt)
        if data:
            print("Calling handlers")
            for handler in self.handlers:
                handler(**data)

    def _load_year_data(self, year: int):
        print(f"Loaded year data: {year}")
        self.year_df = load_toy_dataset(f"GBPJPY_{year}_1m")
        self.year_df["date_str"] = self.year_df["datetime"].dt.strftime(MINUTE_FORMAT)
        self.year = year

    def _get_minute_data(self, dt: datetime.datetime):
        year = dt.year
        if not year == self.year:
            self._load_year_data(year)
        minute_str = dt.strftime(MINUTE_FORMAT)
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


# BOT PROGRAM
# ==============================================================================
class TestBot:

    def __init__(self, src: TestDataSource, interval: float = 0):
        self.src = src
        self.interval = interval

    def __call__(self):
        dt = TEST_START
        while True:
            print(dt)
            self.src(dt)
            dt += datetime.timedelta(minutes=1)
            if dt > TEST_START + TEST_DURATION:
                break


if __name__ == "__main__":
    event_handlers = [TestTradeHandler()]
    data_source = TestDataSource(*event_handlers)
    bot = TestBot(data_source)
    bot()
