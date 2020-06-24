
from .series_indicators import *
from .dataframe_indicators import *

indicators = {
    "sma": simple_moving_average,
    "ema": exponential_moving_average,
    "wma": weighted_moving_average,
    "dema": double_exponential_moving_average,
    "tema": triple_exponential_moving_average,
    "trima": triangular_moving_average,
    "ker": kaufman_efficiency_ratio,
    "kama": kaufman_adaptive_moving_average,
    "mama": mesa_adaptive_moving_average,
}