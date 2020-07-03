
import pandas as pd

from thales.config import DEFAULT_SUBDIR
from thales.config.exceptions import InvalidIndicator
from thales.dataset import DataSet
from thales.indicators import apply_df_indicator, apply_series_indicator, dataframe_indicators, series_indicators


class OneSymDaily(DataSet):
    """Build a dataset for a machine learning task on a single symbol's daily
    data."""

    src = "alphavantage"

    @staticmethod
    def load(sym: str):
        df = OneSymDaily.load_by_symbol(sym, src=OneSymDaily.src, subdir=DEFAULT_SUBDIR, precision=5)
        df = df.set_index("datetime").sort_index()
        apply_df_indicator(df, "tp")  # Add typical price.
        return df[["open", "low", "high", "close", "typical"]]

    @staticmethod
    def apply_indicators(df: pd.DataFrame, target: str = "close", **kwargs):
        """Apply indicators to the DataFrame based on the keyword arguments,
        where key is the name of the indicator, and value is another dict of
        keyword args to pass to the indicator function. If no kwargs are passed
        then all indicators are applied with default values."""
        if not kwargs:
            kwargs = {k: None for k in {**series_indicators, **dataframe_indicators}.keys()}
        ignore = ["tp"]  # Typical price is not really an indicator and should have already been applied.
        for k, v in kwargs.items():
            if k in ignore:
                continue
            if not v:
                v = dict()
            if k in series_indicators:
                apply_series_indicator(df=df, indicator=k, target=target, **v)
            elif k in dataframe_indicators:
                apply_df_indicator(df=df, indicator=k, **v)
            else:
                raise InvalidIndicator(k)



