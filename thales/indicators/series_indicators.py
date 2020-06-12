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


def exponential_moving_average(s: pd.Series, alpha: float = 0.5):
    """Simple implementation of an exponential moving average.

     Equivalent to:
        >>> vals = list(range(20, 30, 1))
        >>> s = pd.Series(vals)
        >>> s.ewm(alpha=0.5, adjust=False).mean()
     """
    assert 0 < alpha < 1
    y1 = s[0]
    s_ema = [y1]
    for y_i in s[1:]:
        prev_s = s_ema[-1]
        s_ema.append(alpha*y_i + (1-alpha) * prev_s)
    return s_ema
