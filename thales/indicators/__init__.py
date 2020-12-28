
from .indicators import *

# Dict of all available indicators. If included in the AlphaVantage API their
# naming convention is used in the dict key, see:
# https://www.alphavantage.co/documentation/#technical-indicators
# This means those technical indicator endpoints don't need to be queried, since
# they can be calculated directly from the raw price data.

ALL_INDICATORS = {
    "sma": SMA,
    "ema": EMA,
    "wma": WMA,
    "dema": DEMA,
    "kama": KAMA,
    "ker": KER,
    "macd": MACD,
    "mama": MESA,
    "tema": TEMA,
    "tp": TP,
    "trima": TRIMA,
    "rsi": RSI,
    "stoch": STOCH,
    "stochf": STOCHF,
}
