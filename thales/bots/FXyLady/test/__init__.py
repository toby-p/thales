"""Implementation of an FX trading bot."""

import datetime
import json
import os
import pandas as pd

from thales.config.bots import register_bot, validate_bot_name
from thales.config.paths import io_path
from thales.config.utils import DATE_FORMAT, MILISECOND_FORMAT
from thales.data import load_toy_dataset
from thales.positions import ManagePositions, Position


# BOT SETUP
# ==============================================================================
BOT_NAME = "FoXyLady"
TEST = True
try:
    BOT_NAME = validate_bot_name(BOT_NAME)
except AssertionError:
    register_bot(BOT_NAME)


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
            hour = row["datetime"].hour
            if hour in (6, 7):
                data_minute = dict()
            else:
                data_minute = row.to_dict()

            # If the row is from a new day, increase the counter:
            row_date = row["datetime"].date()
            if row_date > current_date:
                current_date = row_date
                day_count += 1

            # Get the 6-7am data. If the hour is less than 6am, need the previous
            # day's numbers:
            date_need_67 = datetime.datetime(row_date.year, row_date.month, row_date.day)
            if hour < 6:
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
        self.dates_traded = list()

    def __call__(self, **data):
        mean_67 = data["67"]["mean"]
        high = data["minute"]["high"]
        low = data["minute"]["low"]
        close = data["minute"]["close"]
        open_positions = ManagePositions.list_open_positions(bot_name=BOT_NAME, test=TEST)
        dt = data["minute"]["datetime"]
        timestamp = dt.strftime(MILISECOND_FORMAT)
        date = dt.date()
        if open_positions:
            for p in open_positions:
                open_p = ManagePositions.get_position(p)
                stop_signal = low < (open_p.buy_price - 0.2)
                sell_signal = high > (open_p.buy_price + 0.3)
                if stop_signal or sell_signal:
                    open_p.sell(timestamp=timestamp, price=close)
                    print(f"Closed position {open_p.uuid} (amount={open_p.amount}, "
                          f"buy_price={open_p.buy_price}, sell_price={open_p.sell_price})")
                    self.dates_traded = sorted(set(self.dates_traded) | {date})
        else:
            buy_signal = (high > mean_67 + 0.2) and (close < mean_67 + 0.3) and (date not in self.dates_traded)
            if buy_signal:
                p = Position(open_timestamp=timestamp, buy_price=close, amount=100, test=TEST, bot_name=BOT_NAME)
                print(f"Opened position {p.uuid} (amount={p.amount}, buy_price={p.buy_price})")
                self.dates_traded = sorted(set(self.dates_traded) | {date})


# BOT PROGRAM:
# ==============================================================================
class TestBot:

    def __init__(self, src: TestDataGenerator, handler: TestTradeHandler,
                 start: datetime.datetime, n_days: int):
        self.src = src
        self.handler = handler
        self.start = start
        self.n_days = n_days

    def __call__(self):
        generator = self.src.generator(start=self.start, n_days=self.n_days)
        while True:
            self.handler(**next(generator))
