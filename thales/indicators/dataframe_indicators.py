"""Indicators that can be calculated directly from a pandas DataFrame containing
multiple columns of price data (e.g. high & low) with a DateTime index."""

import numpy as np
import pandas as pd

from thales.indicators.series_indicators import simple_moving_average


def typical_price(df: pd.DataFrame) -> pd.Series:
    """Add the `typical price` average of low, high and open prices. Assumes
    that prices have already been adjusted based on adjusted close."""
    if isinstance(df.index, pd.DatetimeIndex):
        return pd.Series(df[["low", "high", "open"]].sum(axis=1) / 3, name="typical")
    else:
        return pd.Series(df.set_index("datetime")[["low", "high", "open"]].sum(axis=1) / 3, name="typical")


def slow_stochastic_oscillator(df: pd.DataFrame, n: int = 14) -> pd.Series:
    """'Slow' Stochastic oscillator, also known as '%K' see:
        https://www.investopedia.com/terms/s/stochasticoscillator.asp
    """
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.sort_index(ascending=True)
    else:
        df = df.sort_values(by="datetime", ascending=True)
    low = df["low"].rolling(n).min()
    high = df["high"].rolling(n).max()
    k = (df["close"] - low) / (high - low)
    k.index = df.index if isinstance(df.index, pd.DatetimeIndex) else df["datetime"]
    return k.rename(f"stoch (n={n})")


def fast_stochastic_oscillator(df: pd.DataFrame, n: int = 3,
                               k_n: int = 14) -> pd.Series:
    """'Fast' Stochastic oscillator, also known as '%D' (just a simple moving
     average of the slow stochastic_oscillator) see:
        https://www.investopedia.com/terms/s/stochasticoscillator.asp
    """
    k = slow_stochastic_oscillator(df, n=k_n)
    return simple_moving_average(k, n=n, validate=False).rename(f"stoch_f (n={n}, k_n={k_n})")


def mesa_adaptive_moving_average(df: pd.DataFrame, fast_limit: float = 0.5,
                                 slow_limit: float = 0.05) -> pd.DataFrame:
    """MESA Adaptive Moving Average, see notes at:
        https://www.mesasoftware.com/papers/mama.pdf
        https://www.tradingpedia.com/forex-trading-indicators/ehlers-mesa-adaptive-moving-average

    Returns a DataFrame containing the mama and fama calculation results.

    Adapted from:
        https://github.com/codersnotepad/techind/blob/master/techind/mesaAdaptiveMovingAverage.py
    """
    high, low = df["high"], df["low"]

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
    return pd.DataFrame(data={"mama": mama, "fama": fama}, index=ix)
