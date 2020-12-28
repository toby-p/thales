"""Indicators that can be calculated directly from a single Pandas.Series of
price data with a DateTime index."""

__all__ = ["DEMA", "EMA", "KAMA", "KER", "MACD", "RSI", "SMA", "STOCH", "STOCHF", "TEMA", "TRIMA", "WMA"]

import numpy as np
import pandas as pd
from scipy.signal import convolve

from thales.config.utils import OHLC
from thales.indicators.base import DataFrameInSeriesOut, SeriesInSeriesOut, SeriesInDfOut


# Typical parameters for number of time periods in moving average indicators:
MA_TYPICAL_N = list(range(2, 11, 1)) + list(range(20, 81, 20)) + list(range(100, 1001, 100))


class SMA(SeriesInSeriesOut):
    """Simple Moving Average."""

    parameters = {"n": MA_TYPICAL_N}

    def __init__(self, s: pd.Series, n: int = 5, as_percent_diff: bool = True,
                 **kwargs):
        super().__init__(s, n=n, indicator_name=f"SMA (n={n:.0f})", as_percent_diff=as_percent_diff, **kwargs)
        self.n = n

    def apply_indicator(self, s: pd.Series, n: int = 5):
        """Simple moving average of `n` time periods."""
        return s.rolling(window=n).mean()


class EMA(SeriesInSeriesOut):
    """Exponential moving average. When alpha is specified EMA is equivalent to:

    >>> s = pd.Series()
    >>> ema = [s[0]]
    >>> alpha = 0.5
    >>> for y_i in s[1:]:
    >>>     prev_s = ema[-1]
    >>>     ema.append(alpha*y_i + (1-alpha) * prev_s)
    """

    parameters = {"alpha": [0.05, 0.1, 0.2, 0.5, 0.95]}

    def __init__(self, s: pd.Series, alpha: float = None,
                 span: float = None, as_percent_diff: bool = True, **kwargs):
        if not alpha and not span:
            span = len(s)
        label, value = ("alpha", alpha) if alpha else ("span", span)
        super().__init__(s, alpha=alpha, span=span, indicator_name=f"EMA ({label}={value})",
                         as_percent_diff=as_percent_diff, **kwargs)
        self.alpha = alpha
        self.span = span

    def apply_indicator(self, s: pd.Series, alpha: float = None,
                        span: float = None):
        return s.ewm(alpha=alpha, span=span, adjust=False).mean()


class WMA(SeriesInSeriesOut):
    """Weighted Moving Average."""

    parameters = {"n": MA_TYPICAL_N}

    def __init__(self, s: pd.Series, n: int = 5, as_percent_diff: bool = True,
                 **kwargs):
        super().__init__(s, n=n, indicator_name=f"WMA (n={n})", as_percent_diff=as_percent_diff, **kwargs)
        self.n = n

    @staticmethod
    def apply_indicator(s: pd.Series, n: int = 5):
        weights = np.arange(n, 0, -1)
        weights = weights / weights.sum()
        data = convolve(s, weights, mode="valid", method="auto")
        index = s.index[n-1:]
        return pd.Series(data, index=index)


class DEMA(SeriesInSeriesOut):
    """Double exponential moving average. Implemention is equivalent to:

    >>> s = pd.Series()
    >>> ema_s: pd.Series = EMA(s)
    >>> double_ema = (2 * ema_s) - EMA(ema_s)
    """

    parameters = {"alpha": np.arange(0.05, 1, 0.05)}

    def __init__(self, s: pd.Series, alpha: float = None,
                 span: float = None, as_percent_diff: bool = True, **kwargs):
        if not alpha and not span:
            span = len(s)
        label, value = ("alpha", alpha) if alpha else ("span", span)
        super().__init__(s, alpha=alpha, span=span, indicator_name=f"DEMA ({label}={value})",
                         as_percent_diff=as_percent_diff, **kwargs)
        self.alpha = alpha
        self.span = span

    def apply_indicator(self, s: pd.Series, alpha: float = None,
                        span: float = None):
        ema = EMA(s, alpha=alpha, span=span, validate=False, as_percent_diff=False)
        return (2 * ema) - EMA(ema, alpha=alpha, span=span, validate=False, as_percent_diff=False)


class TEMA(SeriesInSeriesOut):
    """Triple exponential moving average. Implemention is equivalent to:

    >>> s = pd.Series
    >>> ema = EMA(s)
    >>> triple_ema = (3 * ema) - (3 * EMA(ema)) + (EMA(EMA(ema)))
    """

    parameters = {"alpha": np.arange(0.05, 1, 0.05)}

    def __init__(self, s: pd.Series, alpha: float = None,
                 span: float = None, as_percent_diff: bool = True, **kwargs):
        if not alpha and not span:
            span = len(s)
        label, value = ("alpha", alpha) if alpha else ("span", span)
        super().__init__(s, alpha=alpha, span=span, indicator_name=f"TEMA ({label}={value})",
                         as_percent_diff=as_percent_diff, **kwargs)
        self.alpha = alpha
        self.span = span

    def apply_indicator(self, s: pd.Series, alpha: float = None,
                        span: float = None):
        ema = EMA(s, alpha=alpha, span=span, validate=False, as_percent_diff=False)
        return (3 * ema) - (3 * EMA(ema, alpha, span, validate=False, as_percent_diff=False)) + \
               EMA(EMA(ema, alpha, span, validate=False, as_percent_diff=False),
                   alpha, span, validate=False, as_percent_diff=False)


class TRIMA(SeriesInSeriesOut):
    """Triangular moving average."""

    parameters = {"n": MA_TYPICAL_N}

    def __init__(self, s: pd.Series, n: int = 5, as_percent_diff: bool = True,
                 **kwargs):
        super().__init__(s, n=n, indicator_name=f"TRIMA (n={n})", as_percent_diff=as_percent_diff, **kwargs)
        self.n = n

    def apply_indicator(self, s: pd.Series, n: int = 5):
        sma = SMA(s, n=n, validate=False, as_percent_diff=False).dropna()
        return SMA(sma, n=n, as_percent_diff=False)


class KER(SeriesInSeriesOut):
    """Kaufman Efficiency Ratio."""

    parameters = {"n": [10, 25, 50, 100, 250, 500]}

    def __init__(self, s: pd.Series, n: int = 5, **kwargs):
        super().__init__(s, n=n, indicator_name=f"KER (n={n})", **kwargs)
        self.n = n

    def apply_indicator(self, s: pd.Series, n: int = 5):
        trend = s.diff(n).abs()
        volatility = s.diff().abs().rolling(window=n).sum()
        return trend / volatility


class KAMA(SeriesInSeriesOut):
    """Kaufman adaptive moving average.
    See: https://school.stockcharts.com/doku.php?id=technical_indicators:kaufman_s_adaptive_moving_average
    """

    parameters = {"er": KER.parameters["n"], "ema_fast": [2, 5, 10, 20], "ema_slow": [10, 50, 100, 200],
                  "n": [5, 20, 100, 200]}

    def __init__(self, s: pd.Series, er: int = 10,
                 ema_fast: int = 2, ema_slow: int = 30,
                 n: int = 20, as_percent_diff: bool = True, **kwargs):
        name = f"KAMA (er={er}, ema_fast={ema_fast}, ema_slow={ema_slow}, n={n})"
        super().__init__(s, er=er, ema_fast=ema_fast, ema_slow=ema_slow, n=n, indicator_name=name,
                         as_percent_diff=as_percent_diff, **kwargs)
        self.er = er
        self.ema_fast = ema_fast
        self.ema_slow = ema_slow
        self.n = n

    def apply_indicator(self, s: pd.Series, er: int = 10,
                        ema_fast: int = 2, ema_slow: int = 30,
                        n: int = 20):
        """
        Args:
            s: price data.
            er: Kaufman Efficiency Ratio window.
            ema_fast: number of periods for fast EMA constant.
            ema_slow: number of periods for slow EMA constant.
            n: number of periods for SMA calculation for first KAMA value.
        """
        assert n >= er, f"`n` must be greater/equal to `er`."
        assert ema_slow > ema_fast, f"`ema_slow` timeframe must be longer than `ema_fast`"
        s_name = s.name
        calc_df = pd.DataFrame(s)
        calc_df["e_ratio"] = KER(s, n=er, validate=False)
        fast_c, slow_c = 2/(ema_fast+1), 2/(ema_slow+1)
        calc_df["smoothing_constant"] = (calc_df["e_ratio"] * (fast_c-slow_c) + slow_c) ** 2
        sma = SMA(s, n=n, validate=False, as_percent_diff=False).dropna()
        calc_df = calc_df.loc[sma.index[1:]]
        kama = list()
        kama.append(sma.iloc[0])  # First value is sma.
        for price, sc in zip(calc_df[s_name], calc_df["smoothing_constant"]):
            prior_kama = kama[-1]
            kama.append(prior_kama + sc * (price - prior_kama))
        return pd.Series(kama[1:], index=calc_df.index)


class MACD(SeriesInDfOut):
    """Moving average convergence-divergence. See:
        https://www.investopedia.com/articles/forex/05/macddiverge.asp
    """

    parameters = {"p_fast": [5, 12, 25, 50, 100], "p_slow": [10, 26, 50, 100, 200], "signal": [2, 9, 25, 50, 100]}

    def __init__(self, s: pd.Series, p_fast: int = 12, p_slow: int = 26,
                 signal: int = 9, **kwargs):
        super().__init__(s, p_fast=p_fast, p_slow=p_slow, signal=signal, **kwargs)
        self.p_fast = p_fast
        self.p_slow = p_slow
        self.signal = signal

    def apply_indicator(self, s: pd.Series, p_fast: int = 12, p_slow: int = 26,
                        signal: int = 9):
        ema_fast = EMA(s, span=p_fast, validate=False, as_percent_diff=False)
        ema_slow = EMA(s, span=p_slow, validate=False, as_percent_diff=False)
        macd_name = f"{s.name} - MACD (p_fast={p_fast}, p_slow={p_slow})"
        macd = (ema_fast - ema_slow).rename(macd_name)
        signal_name = f"{s.name} - MACD_signal (p_fast={p_fast}, p_slow={p_slow}, signal={signal})"
        macd_signal = EMA(macd, span=signal, validate=False, as_percent_diff=False).rename(signal_name)
        return pd.concat([macd, macd_signal], axis=1)


class RSI(SeriesInSeriesOut):
    """Relative strength index; see:
        https://www.investopedia.com/terms/r/rsi.asp
    """

    parameters = {"n": MA_TYPICAL_N}

    def __init__(self, s: pd.Series, n: int = 14, **kwargs):
        super().__init__(s, n=n, indicator_name=f"RSI (n={n})", **kwargs)
        self.n = n

    def apply_indicator(self, s: pd.Series, n: int = 14):
        up, down = s.diff(1), s.diff(1)
        up.loc[(up < 0)], down.loc[(down > 0)] = 0, 0
        up_ewm = EMA(up, span=n, validate=False, as_percent_diff=False)
        down_ewm = EMA(down.abs(), span=n, validate=False, as_percent_diff=False)
        rsi = up_ewm / (up_ewm + down_ewm)
        return rsi


class STOCH(DataFrameInSeriesOut):
    """'Slow' Stochastic oscillator, also known as '%K' see:
        https://www.investopedia.com/terms/s/stochasticoscillator.asp
    """

    parameters = {"n": MA_TYPICAL_N}

    def __init__(self, df: pd.DataFrame, n: int = 14,
                 sym: str = None, **kwargs):
        super().__init__(df=df, sym=sym, ohlc=kwargs.pop("ohlc", OHLC(sym)),
                         indicator_name=f"STOCH (n={n:.0f})", n=n, **kwargs)
        self.n = n

    def apply_indicator(self, df: pd.DataFrame, ohlc: OHLC, n: int = 14):
        low = df[ohlc.low].rolling(n).min()
        high = df[ohlc.high].rolling(n).max()
        k = (df[ohlc.close] - low) / (high - low)
        k.index = df.index if isinstance(df.index, pd.DatetimeIndex) else df["datetime"]
        return k


class STOCHF(DataFrameInSeriesOut):
    """'Fast' Stochastic oscillator, also known as '%D' (just a simple moving
    average of the slow stochastic_oscillator) see:
        https://www.investopedia.com/terms/s/stochasticoscillator.asp
    """

    parameters = {"n": MA_TYPICAL_N}

    def __init__(self, df: pd.DataFrame, n: int = 3,
                 k_n: int = 14, sym: str = None, **kwargs):
        super().__init__(df=df, sym=sym, ohlc=kwargs.pop("ohlc", OHLC(sym)),
                         indicator_name=f"STOCHF (n={n:.0f}, k_n={k_n:.0f})", n=n, k_n=k_n, **kwargs)
        self.n = n
        self.k_n = k_n

    def apply_indicator(self, df: pd.DataFrame, ohlc: OHLC, n: int = 3,
                        k_n: int = 14):
        k = STOCH(df=df, n=k_n, ohlc=ohlc)
        return SMA(k, n=n, validate=False, as_percent_diff=False)


