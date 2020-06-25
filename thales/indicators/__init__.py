
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
