
from thales.config.exceptions import InvalidIndicator
from .series_indicators import *
from .dataframe_indicators import *

# Dictionaries of all available indicators. Where they are included in their
# API, the naming convention follows that of AlphaVantage, see:
# https://www.alphavantage.co/documentation/#technical-indicators
# This means those technical indicator endpoints don't need to be queried, since
# they can be calculated directly from the raw price data.

series_indicators = {
    "sma": simple_moving_average,
    "ema": exponential_moving_average,
    "wma": weighted_moving_average,
    "dema": double_exponential_moving_average,
    "tema": triple_exponential_moving_average,
    "trima": triangular_moving_average,
    "er": kaufman_efficiency_ratio,
    "kama": kaufman_adaptive_moving_average,
    "macd": moving_average_convergence_divergence,
}


dataframe_indicators = {
    "tp": typical_price,
    "mama": mesa_adaptive_moving_average,
    "stoch": slow_stochastic_oscillator,
    "stochf": fast_stochastic_oscillator,
}


def apply_series_indicator(df: pd.DataFrame, indicator: str,
                           target: str = "close", **kwargs):
    """Apply technical indicators in-place to a DataFrame of raw data, based on
    the `target` column series."""
    try:
        func = series_indicators[indicator]
    except KeyError:
        raise InvalidIndicator(indicator)
    i = func(s=df[target], **kwargs)
    if isinstance(i, pd.Series):
        df[i.name] = i
    elif isinstance(i, pd.DataFrame):
        for col in i.columns:
            s = i[col]
            df[s.name] = s


def apply_df_indicator(df: pd.DataFrame, indicator: str, **kwargs):
    """Apply technical indicators to a DataFrame of raw data."""
    try:
        func = dataframe_indicators[indicator]
    except KeyError:
        raise InvalidIndicator(indicator)
    i = func(df=df, **kwargs)
    if isinstance(i, pd.Series):
        df[i.name] = i
    elif isinstance(i, pd.DataFrame):
        for col in i.columns:
            s = i[col]
            df[s.name] = s
