
import pandas as pd

from thales.config.sources import validate_source
from thales.data import DataSet


class HistoryAnalyzer:

    @staticmethod
    def analyze(sym: str, max_hold_n: int = 5, target: str = "close",
                src: str = None, subdir: str = None) -> pd.DataFrame:
        """Analyze historical trading data to identify the best/worst possible
        combinations of buy-sell dates. This information can be used to train
        machine learning algorithms to identify the best indicators to identify
        optimum buy/sell dates.

        Args:
            sym (str); target stock symbol.
            max_hold_n (str): maximum number of periods to analyze trades
                for. All possible trades from 1 to the max are calculated.
            target (str): price to analyze, e.g. `open`, `close`...
            src (str): source of historic data.
            subdir (str): subdirectory of source historic data.
        """
        assert max_hold_n >= 1, f"Trading hold length must be at least 1 (got: {max_hold_n})"
        src = validate_source(src)
        df = DataSet.load_by_symbol(sym, src=src, subdir=subdir)[["datetime", target]]
        n_rows_raw = len(df)
        dfs = list()
        for i in range(1, max_hold_n + 1, 1):
            df["buy"] = df[target].shift(-i)
            df["buy_date"] = df["datetime"].shift(-i)
            df["hold_n"] = i
            dfs.append(df.dropna())
        df = pd.concat(dfs)
        df.rename(columns={target: "sell", "datetime": "sell_date"}, inplace=True)
        df["margin"] = df["sell"] - df["buy"]
        df["hold_duration"] = df["sell_date"] - df["buy_date"]
        df.sort_values(by=["sell_date", "buy_date"], ascending=[False, True], inplace=True)
        expected_n = (n_rows_raw * max_hold_n) - (max_hold_n * (max_hold_n + 1)) / 2
        assert expected_n == len(df), f"Something went wrong, expected {expected_n:,} rows, created {len(df):,}"
        column_order = ["buy_date", "buy", "sell_date", "sell", "hold_duration", "hold_n", "margin"]
        return df[column_order].reset_index(drop=True)
