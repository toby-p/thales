"""Implementation of an FX trading bot."""

import datetime
import json
import numpy as np
import os
import pandas as pd
import random
import time

from thales.config.bots import validate_bot_name
from thales.config.paths import DIR_TEMP
from thales.data import load_toy_dataset
from thales.positions import ManagePositions


date_format = "%Y_%m_%d"
milisecond_format = "%Y_%m_%d %H;%M;%S;%f"
minute_format = "%Y_%m_%d %H;%M"


class TestDataSource:
    data_format = {
        "timestamp": datetime.datetime.now(),
        "open": random.random(),
        "high": random.random(),
        "low": random.random(),
        "close": random.random(),
    }

    def __init__(self):
        self.year: int = 0
        self.year_df = pd.DataFrame()
        self.avg_67 = dict()

    def __call__(self, dt: datetime.datetime):
        return self.get_minute_data(dt)

    def load_year_data(self, year: int):
        fn = f"GBPJPY_{year}_1m"
        self.year_df = load_toy_dataset(fn)
        self.year_df["date_str"] = self.year_df["datetime"].dt.strftime(minute_format)
        self.year = year

    def calculate_67_av(self, year: int):
        if not year == self.year:
            self.load_year_data(year)
        df = self.year_df.copy()
        years = df["datetime"].dt.year
        months = df["datetime"].dt.month
        days = df["datetime"].dt.day
        df["date"] = pd.to_datetime(dict(year=years, month=months, day=days))
        df["hour"] = df["datetime"].dt.hour
        df["67_high"] = np.where(df["hour"].isin([6, 7]), df["high"], np.nan)
        df["67_high"] = df.groupby(["date"])["67_high"].transform("max")
        df["67_low"] = np.where(df["hour"].isin([6, 7]), df["low"], np.nan)
        df["67_low"] = df.groupby(["date"])["67_low"].transform("min")
        df["67_avg"] = df[["67_low", "67_high"]].mean(axis=1)
        df = df.dropna()
        df["date_str"] = df["date"].dt.strftime(date_format)
        self.avg_67 = dict(zip(df["date_str"], df["67_avg"]))

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
            date_str = dt.strftime(date_format)
            try:
                d["67_avg"] = self.avg_67[date_str]
            except KeyError:
                self.calculate_67_av(year)
                d["67_avg"] = self.avg_67[date_str]
            return d
        elif len(row) == 0:
            return None
        else:
            raise IndexError(f"Multiple rows returned for single minute: {minute_str}")


class Handler:

    def __init__(self, bot_name: str = "FoXyLady", test: bool = True):
        self.bot_name = validate_bot_name(bot_name)
        self.test = test

    @property
    def open_positions(self):
        return ManagePositions.list_open_positions(bot_name=self.bot_name, test=self.test)

    @property
    def closed_positions(self):
        return ManagePositions.list_closed_positions(bot_name=self.bot_name, test=self.test)

    def __call__(self, data: dict):
        """Logic for what to do with a single piece of data from a feed."""
        timestamp_str = data["timestamp"].strftime(format="%Y_%m_%d %H;%M;%S;%f")
        data["timestamp"] = timestamp_str
        fp = os.path.join(DIR_TEMP, f"{timestamp_str}.json")
        with open(fp, "w") as f:
            json.dump(data, f)
