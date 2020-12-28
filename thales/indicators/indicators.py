"""Indicators that can be calculated directly from a single Pandas.Series of
price data with a DateTime index."""

__all__ = ["DEMA", "EMA", "KAMA", "KER", "MACD", "MESA", "RSI", "SMA", "STOCH", "STOCHF", "TEMA", "TP", "TRIMA", "WMA"]

import numpy as np
import pandas as pd
from scipy.signal import convolve

from thales.config.utils import OHLC
from thales.indicators.base import DataFrameInDataFrameOut, DataFrameInSeriesOut, SeriesInSeriesOut, \
    SeriesInDataFrameOut


# Typical parameters for number of time periods in moving average indicators:
MA_TYPICAL_N = list(range(2, 11, 1)) + list(range(20, 81, 20)) + list(range(100, 1001, 100))


class TP(DataFrameInSeriesOut):
    """`Typical Price` average of low, high, and open prices. Assumes prices
    have already been adjusted based on adjusted close."""

    def __init__(self, df: pd.DataFrame, sym: str = None, **kwargs):
        super().__init__(df=df, sym=sym, ohlc=kwargs.pop("ohlc", OHLC(sym)),
                         indicator_name="TP", **kwargs)

    def apply_indicator(self, df: pd.DataFrame, ohlc: OHLC):
        if isinstance(df.index, pd.DatetimeIndex):
            return pd.Series(df[[ohlc.low, ohlc.high, ohlc.open]].sum(axis=1) / 3)
        else:
            return pd.Series(df.set_index("datetime")[[ohlc.low, ohlc.high, ohlc.open]].sum(axis=1) / 3)


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


class MACD(SeriesInDataFrameOut):
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


class MESA(DataFrameInDataFrameOut):
    """MESA Adaptive Moving Average, see notes at:
        https://www.mesasoftware.com/papers/mama.pdf
        https://www.tradingpedia.com/forex-trading-indicators/ehlers-mesa-adaptive-moving-average

    Returns a DataFrame containing the mama and fama calculation results.

    Adapted from:
        https://github.com/codersnotepad/techind/blob/master/techind/mesaAdaptiveMovingAverage.py
    """

    parameters = {"fast_limit": [0.5], "slow_limit": [0.05]}

    def __init__(self, df: pd.DataFrame, fast_limit: float = 0.5,
                 slow_limit: float = 0.05, sym: str = None, **kwargs):
        super().__init__(df=df, sym=sym, ohlc=kwargs.pop("ohlc", OHLC(sym)),
                         fast_limit=fast_limit, slow_limit=slow_limit, **kwargs)
        self.fast_limit = fast_limit
        self.slow_limit = slow_limit

    def apply_indicator(self, df: pd.DataFrame, ohlc: OHLC,
                        fast_limit: float = 0.5, slow_limit: float = 0.05):
        high, low = df[ohlc.high], df[ohlc.low]

        data = (high + low) / 2
        s = np.zeros(len(data))  # smooth
        d = np.zeros(len(data))  # detrenders
        p = np.zeros(len(data))  # periods
        sp = np.zeros(len(data))  # smoothed periods
        ph = np.zeros(len(data))  # phases
        q1 = np.zeros(len(data))  # q1
        q2 = np.zeros(len(data))  # q2
        i1 = np.zeros(len(data))  # i1
        i2 = np.zeros(len(data))  # i2
        re = np.zeros(len(data))  # re
        im = np.zeros(len(data))  # im

        mama = np.zeros(len(data))  # mama ouput.
        fama = np.zeros(len(data))  # fama output.

        # Calculate mama and fama:
        for i in range(5, len(data), 1):

            s[i] = (4 * data[i] + 3 * data[i - 1] + 2 * data[i - 2] + data[i - 3]) / 10
            d[i] = (
                           0.0962 * s[i]
                           + 0.5769 * s[i - 2]
                           - 0.5769 * s[i - 4]
                           - 0.0962 * s[i - 6]
                   ) * (0.075 * p[i - 1] + 0.54)

            # Compute InPhase and Quadrature components:
            q1[i] = (
                            0.0962 * d[i]
                            + 0.5769 * d[i - 2]
                            - 0.5769 * d[i - 4]
                            - 0.0962 * d[i - 6]
                    ) * (0.075 * p[i - 1] + 0.54)
            i1[i] = d[i - 3]

            # Advance the phase of I1 and Q1 by 90 degrees:
            ji = (
                         0.0962 * i1[i - i]
                         + 0.5769 * i1[i - 2]
                         - 0.5769 * i1[i - 4]
                         - 0.0962 * i1[i - 6]
                 ) * (0.075 * p[i - 1] + 0.54)
            jq = (
                         0.0962 * q1[i - i]
                         + 0.5769 * q1[i - 2]
                         - 0.5769 * q1[i - 4]
                         - 0.0962 * q1[i - 6]
                 ) * (0.075 * p[i - 1] + 0.54)

            # Phasor addition for 3 bar averaging:
            _i2 = i1[i] - jq
            _q2 = q1[i] + ji

            # Smooth the I and Q components before applying the discriminator:
            i2[i] = 0.2 * _i2 + 0.8 * i2[i]
            q2[i] = 0.2 * _q2 + 0.8 * q2[i]

            # Homodyne Discriminator:
            _re = i2[i] * i2[i - 1] + q2[i] * q2[i - 1]
            _im = i2[i] * q2[i - 1] + q2[i] * i2[i - 1]
            re[i] = 0.2 * _re + 0.8 * re[i - 1]
            im[i] = 0.2 * _im + 0.8 * im[i - 1]

            # Set period value:
            period = 0
            if _im != 0 and _re != 0:
                period = 360 / np.arctan(_im / _re)
            if period > 1.5 * p[-1]:
                period = 1.5 * p[i - 1]
            if period < 0.67 * p[i - 1]:
                period = 0.67 * p[i - 1]
            if period < 6:
                period = 6
            if period > 50:
                period = 50
            p[i] = 0.2 * period + 0.8 * p[i - 1]
            sp[i] = 33 * p[i - 1] + 0.67 * sp[i - 1]

            if i1[i] != 0:
                ph[i] = np.arctan(q1[i] / i1[i])

            # Delta phase:
            delta_phase = ph[i - 1] - ph[i]
            if delta_phase < 1:
                delta_phase = 1

            # Alpha:
            alpha = fast_limit / delta_phase
            if alpha < slow_limit:
                alpha = slow_limit

            # Add to output using EMA formula:
            mama[i] = alpha * data[i] + (1 - alpha) * mama[i - 1]
            fama[i] = 0.5 * alpha * mama[i] + (1 - 0.5 * alpha) * fama[i - 1]

        # remove the mama and fama warm-up values
        #     for i in range(warmUpPeriod + 1):
        #         if i <= warmUpPeriod:
        #             mama[i] = np.nan
        #             fama[i] = np.nan

        ix = df.index if isinstance(df.index, pd.DatetimeIndex) else df["datetime"]

        # Name columns:
        mama_name = f"MAMA (fast_limit={fast_limit:.3f}, slow_limit={slow_limit:.3f})"
        fama_name = f"FAMA (fast_limit={fast_limit:.3f}, slow_limit={slow_limit:.3f})"
        return pd.DataFrame(data={mama_name: mama, fama_name: fama}, index=ix)
