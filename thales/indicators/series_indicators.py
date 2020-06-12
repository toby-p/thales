"""Simple indicators that can be calculated from a pandas Series with a
DateTime index."""

import pandas as pd


def series_data_validation(s: pd.Series):
    """Validation checks to make sure series is in correct format to apply
    indicators."""
    assert isinstance(s.index, pd.DatetimeIndex), "Series index must be pd.DatetimeIndex"
    assert len(s.dropna()) == len(s), "Series cannot contain NaNs"


def simple_moving_average(s: pd.Series, n: int = 5, validate: bool = True):
    """Calculate simple moving average over `n` number of intervals."""
    if validate:
        series_data_validation(s)
    s = s.sort_index()
    return s.rolling(n).sum() / n
