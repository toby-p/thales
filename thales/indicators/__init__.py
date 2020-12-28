
from thales.config.exceptions import InvalidIndicator
from .indicators import *
from .dataframe_indicators import *

# Dictionaries of all available indicators. Where they are included in their
# API, the naming convention follows that of AlphaVantage, see:
# https://www.alphavantage.co/documentation/#technical-indicators
# This means those technical indicator endpoints don't need to be queried, since
# they can be calculated directly from the raw price data.

all_indicators = {
    "sma": SMA,
    "ema": EMA,
    "wma": WMA,
    "dema": DEMA,
    "tema": TEMA,
    "trima": TRIMA,
    "ker": KER,
    "kama": KAMA,
    "macd": MACD,
    "rsi": RSI,
    "stoch": STOCH,
    "stochf": STOCHF,
}


dataframe_indicators = {
    "tp": typical_price,
    "mama": mesa_adaptive_moving_average,
}


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
