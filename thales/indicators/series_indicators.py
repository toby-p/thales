"""Indicators that can be calculated directly from a single pandas Series of
price data with a DateTime index."""

import numpy as np
import pandas as pd
from scipy.signal import convolve


def series_data_validation(s: pd.Series, date_ascending: bool = True):
    """Validation checks to make sure series is in correct format to apply
    indicators."""
    assert isinstance(s.index, pd.DatetimeIndex), "Series index must be pd.DatetimeIndex"
    s = s.sort_index(ascending=date_ascending)
    assert len(s.dropna()) == len(s), "Series cannot contain NaNs"
    return s


def simple_moving_average(s: pd.Series, n: int = 5, validate: bool = True):
    """Calculate simple moving average over `n` number of intervals."""
    if validate:
        s = series_data_validation(s)
    return s.rolling(window=n).mean().rename(f"sma (n={n:.0f})")


def exponential_moving_average(s: pd.Series, alpha: float = None,
                               span: float = None, validate: bool = True):
    """Exponential moving average. When alpha is specified, ema is:

    >>> ema = [s[0]]
    >>> for y_i in s[1:]:
    >>>     prev_s = ema[-1]
    >>>     ema.append(alpha*y_i + (1-alpha) * prev_s)
    """
    if validate:
        s = series_data_validation(s, date_ascending=True)
    if not alpha and not span:
        span = len(s)
    label, value = ("alpha", alpha) if alpha else ("span", span)
    return s.ewm(alpha=alpha, span=span, adjust=False).mean().rename(f"ema ({label}={value})")


def weighted_moving_average(s: pd.Series, n: int = 5, validate: bool = True):
    """Weighted moving average."""
    if validate:
        s = series_data_validation(s, date_ascending=True)
    weights = np.arange(n, 0, -1)
    weights = weights / weights.sum()
    data = convolve(s, weights, mode="valid", method="auto")
    index = s.index[n-1:]
    return pd.Series(data, index=index).rename(f"wma (n={n})")


def double_exponential_moving_average(s: pd.Series, alpha: float = None,
                                      span: float = None,
                                      validate: bool = True):
    """Double exponential moving average. Implemented as:

    >>> ema_s = exponential_moving_average(s)
    >>> double_ema = (2 * ema_s) - exponential_moving_average(ema_s)
    """
    if not alpha and not span:
        span = len(s)
    label, value = ("alpha", alpha) if alpha else ("span", span)
    ema_s = exponential_moving_average(s, alpha=alpha, span=span, validate=validate)
    dema = (2 * ema_s) - exponential_moving_average(ema_s, alpha=alpha, span=span, validate=validate)
    return dema.rename(f"dema ({label}={value})")


def triple_exponential_moving_average(s: pd.Series, alpha: float = None,
                                      span: float = None,
                                      validate: bool = True):
    """Triple exponential moving average. Implemented as:

    >>> ema = exponential_moving_average  # The base ema function.
    >>> ema_s = ema(s)
    >>> triple_ema = (3 * ema_s) - (3 * ema(ema_s)) + (ema(ema(ema_s)))
    """
    if not alpha and not span:
        span = len(s)
    label, value = ("alpha", alpha) if alpha else ("span", span)
    ema, a, k, v = exponential_moving_average, alpha, span, validate
    ema_s = ema(s, a, k, v)
    tema = (3 * ema_s) - (3 * ema(ema_s, a, k, v)) + ema(ema(ema_s, a, k, v), a, k, v)
    return tema.rename(f"tema ({label}={value})")


def triangular_moving_average(s: pd.Series, n: int = 5, validate: bool = True):
    """Triangular moving average."""
    sma = simple_moving_average(s, n, validate).dropna()
    return simple_moving_average(sma, n, validate).rename(f"trima (n={n})")


def kaufman_efficiency_ratio(s: pd.Series, n: int = 10, validate: bool = True):
    """Kaufman Efficiency ratio of price direction to volatility."""
    if validate:
        s = series_data_validation(s, date_ascending=True)
    trend = s.diff(n).abs()
    volatility = s.diff().abs().rolling(window=n).sum()
    return (trend / volatility).rename(f"er (n={n})")


def kaufman_adaptive_moving_average(s: pd.Series, er: int = 10,
                                    ema_fast: int = 2, ema_slow: int = 30,
                                    n: int = 20, validate: bool = True):
    """Kaufman adaptive moving average.
    See: https://school.stockcharts.com/doku.php?id=technical_indicators:kaufman_s_adaptive_moving_average

    Args:
        s (pd.Series): price data to calculate indicator from.
        er (int): Kaufman efficiency ratio window.
        ema_fast (int): number of periods for fast EMA constant.
        ema_slow (int): number of periods for slow EMA constant.
        n (int): number of periods for simple moving average calculation for
            first kama value.
        validate (bool): if True, ensure `s` is in correct format.
    """
    assert n >= er, f"n must be greater/equal to er."
    if validate:
        s = series_data_validation(s, date_ascending=True)
    s.name = "price"
    calc_df = pd.DataFrame(s)
    calc_df["e_ratio"] = kaufman_efficiency_ratio(s, n=er, validate=False)
    fast_c, slow_c = 2/(ema_fast+1), 2/(ema_slow+1)
    calc_df["smoothing_constant"] = (calc_df["e_ratio"] * (fast_c-slow_c) + slow_c) ** 2
    sma = simple_moving_average(s, n=n).dropna()
    calc_df = calc_df.loc[sma.index[1:]]
    kama = list()
    kama.append(sma.iloc[0])  # First value is sma.
    for price, sc in zip(calc_df["price"], calc_df["smoothing_constant"]):
        prior_kama = kama[-1]
        kama.append(prior_kama + sc * (price - prior_kama))
    name = f"kama (er={er}, ema_fast={ema_fast}, ema_slow={ema_slow}, n={n})"
    return pd.Series(kama[1:], index=calc_df.index, name=name)


def moving_average_convergence_divergence(s: pd.Series, p_fast: int = 12,
                                          p_slow: int = 26, signal: int = 9,
                                          validate: bool = True) -> pd.DataFrame:
    """Moving average convergence-divergence. See:
        https://www.investopedia.com/articles/forex/05/macddiverge.asp

    Args:
        s (pd.Series): price data to calculate indicator from.
        p_fast (int): number of periods in fast moving average.
        p_slow (int): number of periods in slow moving average.
        signal (int): number of periods in ema of macd.
        validate (bool): if True, ensure `s` is in correct format.

    """
    if validate:
        s = series_data_validation(s, date_ascending=True)
    ema_fast = exponential_moving_average(s, span=p_fast, validate=False)
    ema_slow = exponential_moving_average(s, span=p_slow, validate=False)
    macd = (ema_fast - ema_slow).rename(f"macd (p_fast={p_fast}, p_slow={p_slow})")
    signal_name = f"macd_signal (p_fast={p_fast}, p_slow={p_slow}, signal={signal})"
    macd_signal = exponential_moving_average(macd, span=signal, validate=False).rename(signal_name)
    return pd.concat([macd, macd_signal], axis=1)
