
from .series_indicators import *
from .dataframe_indicators import *


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
}
