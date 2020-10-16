
import datetime
import os
import pandas as pd

from thales.config.paths import package_path
from thales.config.utils import parse_datetime, PRICE_COLS


class TestDataset:
    """Class for handling test datasets for back-testing strategies."""

    def __init__(self, name: str, start_date: object = None,
                 end_date: object = None):
        """Class for interacting with locally stored historical data.

        Args:
            name: valid name of a test dataset.
            start_date: string or datetime representing the back-test start.
            end_date: string or datetime representing the back-test end.
        """
        self.df = pd.DataFrame(columns=list(PRICE_COLS)+["datetime"]).set_index(["datetime"])
        data_dir = package_path("data", "toy_datasets", name)
        assert os.path.exists(data_dir), f"Invalid data directory name: {name}"
        self.data_dir = data_dir
        self.name = name
        self.start_date = parse_datetime(start_date)
        self.end_date = parse_datetime(end_date)
        if start_date and end_date:
            self.load_by_date(start_date, end_date)

    def load_by_date(self, start_date: object, end_date: object):
        """Load data to the `df` attribute between the 2 dates."""
        self.start_date = parse_datetime(start_date)
        self.end_date = parse_datetime(end_date)
        years = list(range(self.start_date.year, self.end_date.year + 1, 1))
        for y in years:
            self.load_year(y)
        self.df = self.df.loc[(self.df.index >= self.start_date) & (self.df.index <= self.end_date)]

    def _build_stats(self):
        """Build and save a CSV of statistics about the dataset."""
        stats = pd.DataFrame()
        for year in self.available_years:
            df = self.open_year_csv(year)
            df["year"], df["month"] = df["datetime"].dt.year, df["datetime"].dt.month
            year_stats = df.groupby(["year", "month"])[PRICE_COLS].agg({c: ["min", "max"] for c in PRICE_COLS})
            stats = stats.append(year_stats, sort=False)
        stats.index.names = ["Year", "Month"]
        stats.to_csv(os.path.join(self.data_dir, "stats.csv"), encoding="utf-8", index=True)
        return stats

    @property
    def stats(self):
        try:
            fp = os.path.join(self.data_dir, "stats.csv")
            return pd.read_csv(fp, encoding="utf-8", header=[0, 1], index_col=[0, 1])
        except FileNotFoundError:
            return self._build_stats()

    @property
    def available_years(self):
        files = [f for f in os.listdir(self.data_dir) if f.endswith(".csv") and f != "stats.csv"]
        return sorted([int(f[:-4]) for f in files])

    def open_year_csv(self, year: int):
        """Open a raw CSV file containing a year's data."""
        fp = os.path.join(self.data_dir, f"{year}.csv")
        try:
            df = pd.read_csv(fp, encoding="utf-8")
            for c in df.columns:
                if "date" in c:
                    df[c] = pd.to_datetime(df[c])
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"No data available for year: {year}")

    @property
    def current_datetime(self):
        """Current first datetime in the `df` attribute's index."""
        return self.df.index[0].to_pydatetime()

    def load_year(self, year: int = None):
        """Append a year's data to the `df` attribute. If no argument is passed,
        loads the year after the latest loaded year."""
        if not year:
            year = self.year + 1
        df = self.open_year_csv(year)
        current_df = self.df.reset_index()
        new_df = current_df.append(df, sort=False).drop_duplicates(subset=["datetime"])
        self.df = new_df.set_index(["datetime"]).sort_index()
        self.year = year

    def jump_to_date(self, dt: object):
        """For a specific datetime, ensure the relevant year's data is loaded,
        and then filter the `df` attribute so that it starts at the closest
        available datetime which is greater/equal to `dt`."""
        dt = parse_datetime(dt)
        if dt.year != self.year:
            self.load_year(dt.year)
        self.df = self.df.loc[self.df.index >= dt]
        # If no data in df have reached year end, so load next year:
        if not len(self.df):
            self.load_year(dt.year + 1)
            self.df = self.df.loc[self.df.index >= dt]

    def jump_days(self, n: int = 1):
        """Jump to first available datetime `n` days after current datetime."""
        dt = self.current_datetime
        current = datetime.date(dt.year, dt.month, dt.day)
        next_date = current + datetime.timedelta(days=n)
        self.jump_to_date(next_date)

    def jump_hours(self, n: int = 1):
        """Jump to first available datetime `n` hours after current datetime."""
        dt = self.current_datetime
        current = datetime.datetime(dt.year, dt.month, dt.day, dt.hour)
        next_date = current + datetime.timedelta(hours=n)
        self.jump_to_date(next_date)

    def jump_minutes(self, n: int = 1):
        """Jump to first available datetime `n` minutes after current datetime.
        """
        dt = self.current_datetime
        current = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, minute=dt.minute)
        next_date = current + datetime.timedelta(minutes=n)
        self.jump_to_date(next_date)

    def jump_to_condition(self, *condition: str):
        """Filter the `df` attribute to the first available row which meets the
        given criteria (i.e. search for signals).

        Args:
            condition: string representing a condition for a specific price
                column, in format: `{column}_{operator}_{value}`
                Valid operators are: `g` (greater), `ge` (greater-equal),
                `l` (less), `le` (less equal). E.g. to find rows where `close`
                is greater/equal to 123.45 pass: `close_ge_123.45`.
        """
        assert condition, "No conditions passed"
        expressions = list()
        for cond in condition:
            column, operator, value = cond.split("_")
            assert column in self.df.columns, f"Invalid column: {column}"
            operator = {"g": ">", "ge": ">=", "l": "<", "le": "<="}[operator]
            value = float(value)
            expressions.append(f"(self.df['{column}'] {operator} {value})")

        while True:  # Eventually raises an error when tries to load year without data.
            mask = False
            for expr in expressions:
                mask = mask | eval(expr)
            rows = self.df.loc[mask]
            if not len(rows):  # No data matches criteria, load next year:
                self.load_year()
            else:
                self.jump_to_date(rows.index[0])
                break
