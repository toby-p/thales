"""Class for performing fast back-testing of the bot strategy."""

import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import random

from thales.config.utils import MILISECOND_FORMAT
from thales.data.test_dataset import TestDataset
from thales.positions import PositionManager


class FoXyLadyTester(TestDataset):
    bot_name = "FoXyLady"
    amount = 100

    def __init__(self, start_date: object, end_date: object,
                 name: str = "GBPJPY_1m", alpha_signal: float = 0.2,
                 beta_signal: float = 0.1, stop_loss: float = np.inf):
        super().__init__(name=name, start_date=start_date, end_date=end_date)

        df = self.df

        # Add columns for calculating 6-7am high and low values.
        df["hour"] = df.reset_index()["datetime"].dt.hour.values
        df["date"] = df.reset_index()["datetime"].dt.date.values
        df["h"] = np.where(df["hour"].isin([6, 7]), df["high"], np.nan)
        df["h"] = df.groupby(["date"])["h"].transform("max")
        df["l"] = np.where(df["hour"].isin([6, 7]), df["low"], np.nan)
        df["l"] = df.groupby(["date"])["l"].transform("min")

        # Create the 6-7am mu column:
        df["mu"] = df[["h", "l"]].mean(axis=1)

        # Drop rows where h/l for day couldn't be calculated:
        df = df.dropna()

        # Drop all hours before 8am as these are now irrelevant:
        df = df.loc[df["hour"] >= 8]

        # Add signals to open long/short trades:
        df["alpha_l"] = np.where(df["high"] > df["h"] + alpha_signal, 1, 0)
        df["alpha_s"] = np.where(df["low"] < df["l"] - alpha_signal, 1, 0)
        df["alpha"] = df[["alpha_l", "alpha_s"]].sum(axis=1)

        # Add signals to close long/short trades:
        df["beta_l"] = np.where(df["high"] > df["h"] + (alpha_signal + beta_signal), 1, 0)
        df["beta_s"] = np.where(df["low"] < df["l"] - (alpha_signal + beta_signal), 1, 0)
        df["beta"] = df[["beta_l", "beta_s"]].sum(axis=1)

        self.df = df
        self.stop_loss = stop_loss

        # Create instance of position manager object:
        self.pm = PositionManager(bot_name=self.bot_name, test=True, create_test_dir=True)

        # Some attributes for storing results when test is run:
        self.num_days_in_test = len(self.df["date"].unique())
        self.num_long_trades = 0
        self.num_short_trades = 0

    def __call__(self):
        """Run the test until all test data in `df` is used up."""
        df = self.df

        # Find the first alpha row in the DataFrame:
        alpha_rows = df[(df["alpha"] > 0)]
        if not len(alpha_rows):
            return

        # Get data from the first alpha row and open a position:
        first_alpha_row = alpha_rows.index[0]
        pos_data = df.loc[first_alpha_row]
        if pos_data["alpha_l"] == 1:
            ptype = "long"
            beta_col = "beta_l"
            self.num_long_trades += 1
        elif pos_data["alpha_s"] == 1:
            ptype = "short"
            beta_col = "beta_s"
            self.num_short_trades += 1
        else:
            raise ValueError()
        open_ts = pos_data.name.strftime(MILISECOND_FORMAT)
        buy_price = pos_data["close"]
        pos = self.pm.open_new_position(ptype=ptype, open_timestamp=open_ts, buy_price=buy_price,
                                        amount=self.amount, test=True, **pos_data)

        # Add the signal column for the stop loss:
        if ptype == "short":
            df["stop_signal"] = (df["high"] > (buy_price + self.stop_loss)).astype(int)
        elif ptype == "long":
            df["stop_signal"] = (df["low"] < (buy_price - self.stop_loss)).astype(int)

        # Move to the next row in `df`, then find the first beta row or stop loss row:
        df = df.loc[(df.index > first_alpha_row)]
        stop_or_beta_rows = df[(df[beta_col] > 0) | (df["stop_signal"] > 0)]

        # Check if signal is same day and close then if so, else just close at end of day:
        date_opened = pos_data["date"]
        if (not len(stop_or_beta_rows)) or (stop_or_beta_rows.iloc[0]["date"] > date_opened):
            # Close at end of day:
            close_datetime = df.loc[(df["date"] == date_opened)].index.max()
        else:
            # Close when stop/beta signal occurs:
            close_datetime = stop_or_beta_rows.index[0]
        try:
            metadata = df.loc[close_datetime]
        except KeyError as e:
            print(stop_or_beta_rows)
            raise e
        self.pm.close_position(position_name=pos.name, timestamp=close_datetime.strftime(MILISECOND_FORMAT),
                               price=metadata["close"], **metadata)

        # Move to the next day in the `df` attribute and call again:
        self.df = self.df.loc[(self.df["date"] > metadata["date"])]
        self.__call__()


class FoXyLadyMegaTester:

    def __init__(self, start_year: int = 2015, end_year: int = 2020,
                 n_days: int = 120, stop_loss: float = np.inf,
                 random_seed: int = None):
        self.random_seed = random_seed
        self.start_dates, self.end_dates = \
            self.generate_start_end_dates(start_year=start_year, end_year=end_year, n_days=n_days)
        self.test_objects = list()
        for start, end in zip(self.start_dates, self.end_dates):
            test = FoXyLadyTester(start, end, "GBPJPY_1m", stop_loss=stop_loss)
            test()
            test.pm.construct_dataframe()  # Construct results.
            # Add column with time from start time position was closed:
            test.pm.df["time_from_start"] = test.pm.df["close_timestamp"] - test.start_date
            self.test_objects.append(test)

    def generate_start_end_dates(self, start_year: int = 2015,
                                 end_year: int = 2020, n_days: int = 120):
        """Generate random start and end dates of the duration given by `n_days`
        where one start date will be produced for each year in the range.
        """
        start_dates, end_dates = list(), list()
        for year in range(start_year, end_year, 1):
            start = datetime.datetime(year, 1, 1)
            end = datetime.datetime(year, 12, 31)
            dates = pd.date_range(start, end, freq="D")
            random.seed(self.random_seed)
            start_date = random.choice(dates)
            start_dates.append(start_date)
            end_date = start_date + datetime.timedelta(days=n_days)
            end_dates.append(end_date)
        return start_dates, end_dates

    def plot_all_results(self, figsize=(12, 8)):
        """Plot the cumulative delta for all tests performed with number of days
        since test start on the x-axis."""
        series = list()
        max_days = 0
        for t in self.test_objects:
            timedeltas = t.pm.df["time_from_start"]
            days = timedeltas.dt.total_seconds() / 60 / 60 / 24
            max_days = max(days.max(), max_days)
            s = pd.Series(t.pm.df["delta_cumsum"].values, index=days).rename(t.start_date.strftime("%Y-%m-%d"))
            series.append(s)

        fig, ax = plt.subplots(figsize=figsize)
        for s in series:
            ax.plot(s, label=s.name)
        ax.legend()
        xmin, xmax = 0, ax.get_xlim()[1]
        ax.hlines(0, xmin, xmax, color="#e6e6e6", linestyle="--")
        ax.set_xlim(xmin, xmax)
        ymin, ymax = ax.get_ylim()
        abs_max = max([abs(ymin), abs(ymax)])
        ax.set_ylim(-abs_max, abs_max)
        for tick in ax.get_xticklabels():
            tick.set_rotation(45)
        ax.set_ylabel("delta", size=14)
        ax.set_xlabel("number_days", size=12)

        return fig
