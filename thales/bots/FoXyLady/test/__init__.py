"""Implementation of an FX trading bot."""

import datetime
import json
import os
import pandas as pd

from thales.config.bots import register_bot, validate_bot_name
from thales.config.paths import io_path
from thales.config.utils import DATE_FORMAT, MILISECOND_FORMAT
from thales.data import load_toy_dataset
from thales.positions import Positions


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
        # Some days don't have 6-7am data for some reason, so try a few times
        # to get the previous day's data until it's found:
        for i in range(10):
            try:
                dt_ix = self.dates[self.dates == dt].index
                return self.dates.loc[dt_ix - 1].iloc[0].date()
            except IndexError:
                dt -= datetime.timedelta(days=1)

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

            # Get the 6-7am data. If the hour is less than 6am, need the prior
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

    __slots__ = ["positions", "dates_traded", "entry_signal", "sell_signal", "abs_long_stop", "abs_short_stop"]

    def __init__(self, positions: Positions, entry_signal: float = 0.2,
                 sell_signal: float = 0.3, abs_long_stop: float = 0.3,
                 abs_short_stop: float = 0.3):
        """Class which when called with trading data handles the logic for
        making decisions on whether to trade or not.


        Args:
            positions: object to manage opening/closing of positions.
            entry_signal: number of basis points above/below baseline daily data
                upon which to enter a long/short trade.
            sell_signal: number of basis points above/below baseline daily data
                upon which to exit a long/short trade (>`entry_signal`).
            abs_long_stop: absolute number of basis points below `entry_signal`
                upon which a long trade will be stopped.
            abs_short_stop: absolute number of basis points above `entry_signal`
                upon which a short trade will be stopped.
        """
        assert sell_signal > entry_signal, "sell_signal must be greater than entry_signal"
        assert abs_long_stop >= 0, "abs_long_stop must be absolute (non-negative) float"
        assert abs_short_stop >= 0, "abs_short_stop must be absolute (non-negative) float"
        self.positions = positions
        self.dates_traded = list()
        self.entry_signal = entry_signal
        self.sell_signal = sell_signal
        self.abs_long_stop = abs_long_stop
        self.abs_short_stop = abs_short_stop

    def __call__(self, **data):
        mean_67 = data["67"]["mean"]
        high = data["minute"]["high"]
        low = data["minute"]["low"]
        close = data["minute"]["close"]
        open_positions = self.positions.open_positions
        dt = data["minute"]["datetime"]
        timestamp = dt.strftime(MILISECOND_FORMAT)
        date = dt.date()
        if open_positions:
            for p in open_positions:
                pos = self.positions.get_position(p)
                ptype: str = pos.ptype
                if ptype == "Long":
                    stop_decision = low < (pos.buy_price - self.abs_long_stop)
                    sell_decision = high > (pos.buy_price + self.sell_signal)
                    if stop_decision or sell_decision:
                        pos.sell(timestamp=timestamp, price=close)
                        print(f"Closed {ptype} position {pos.uuid} (amount={pos.amount}, "
                              f"buy_price={pos.buy_price}, sell_price={pos.sell_price})")
                        self.dates_traded = sorted(set(self.dates_traded) | {date})
                elif ptype == "Short":
                    stop_decision = high > (pos.buy_price + self.abs_short_stop)
                    sell_decision = low > (pos.buy_price - self.sell_signal)
                    if stop_decision or sell_decision:
                        pos.sell(timestamp=timestamp, price=close)
                        print(f"Closed {ptype} position {pos.uuid} (amount={pos.amount}, "
                              f"buy_price={pos.buy_price}, sell_price={pos.sell_price})")
                        self.dates_traded = sorted(set(self.dates_traded) | {date})
                else:
                    raise ValueError(f"Invalid position type: {ptype}")
        elif date not in self.dates_traded:  # Only trade max once per day.
            long_buy_decision = (high > mean_67 + self.entry_signal) and (close < mean_67 + self.sell_signal)
            short_buy_decision = (low < mean_67 - self.entry_signal) and (close > mean_67 - self.sell_signal)
            if long_buy_decision:
                metadata = dict(buy_signal=self.entry_signal, sell_signal=self.sell_signal,
                                abs_long_stop=self.abs_long_stop, abs_short_stop=self.abs_short_stop, mean_67=mean_67,
                                high=high, low=low, close=close)
                p = self.positions.open_new_position(ptype="long", open_timestamp=timestamp, buy_price=close,
                                                     amount=100, test=TEST, **metadata)
                print(f"Opened Long position {p.uuid} (amount={p.amount}, buy_price={p.buy_price})")
                self.dates_traded = sorted(set(self.dates_traded) | {date})
            elif short_buy_decision:
                metadata = dict(buy_signal=self.entry_signal, sell_signal=self.sell_signal,
                                abs_long_stop=self.abs_long_stop, abs_short_stop=self.abs_short_stop, mean_67=mean_67,
                                high=high, low=low, close=close)
                p = self.positions.open_new_position(ptype="short", open_timestamp=timestamp, buy_price=close,
                                                     amount=100, test=TEST, **metadata)
                print(f"Opened Short position {p.uuid} (amount={p.amount}, buy_price={p.buy_price})")
                self.dates_traded = sorted(set(self.dates_traded) | {date})
        self.positions.save_metadata(last_timestamp=timestamp)


# BOT PROGRAM:
# ==============================================================================
class TestBot:

    def __init__(self, src: TestDataGenerator, handler: TestTradeHandler,
                 start: datetime.datetime, n_days: int, test_name: str = None):
        self.src = src
        self.handler = handler
        self.start = start
        self.n_days = n_days
        self.test_name = test_name

    def __call__(self):
        generator = self.src.generator(start=self.start, n_days=self.n_days)
        while True:
            try:
                data = {**{"test_name": self.test_name}, **next(generator)}
                self.handler(**data)
            except StopIteration:
                print("Test complete")
                break
