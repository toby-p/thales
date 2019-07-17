import os
import pandas as pd
import warnings

from ..nav import DIR_DATA, SYMBOLS


class AlphaVantage:

    @staticmethod
    def load(function: str = "TIME_SERIES_DAILY_ADJUSTED", *symbol: str):

        directory = os.path.join(DIR_DATA, "alphavantage")
        assert os.path.isdir(directory), f"No data from `alphavantage`."
        directory = os.path.join(directory, function)
        assert os.path.isdir(directory), f"Invalid `function`: {function}"
        csvs = os.listdir(directory)

        if not symbol:
            symbol = SYMBOLS
        targets = [f"{str.upper(s)}.csv" for s in symbol]
        to_load = sorted(set(csvs) & set(targets))
        missing = sorted(set(targets) - set(to_load))
        if len(missing):
            missing = [m[:-4] for m in missing]
            warnings.warn(f"No data found for symbols: {', '.join(missing)}")
        if not len(to_load):
            return

        dfs = list()
        for csv in to_load:
            fp = os.path.join(directory, csv)
            new = pd.read_csv(fp, encoding="utf-8")
            dfs.append(new)

        df = pd.concat(dfs, sort=False)
        df.reset_index(drop=True, inplace=True)
        df["DateTime"] = pd.to_datetime(df["DateTime"])

        return df
