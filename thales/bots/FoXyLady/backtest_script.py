
import pandas as pd
import random

try:
    import thales
except ModuleNotFoundError:
    import os
    import sys
    module_dir = os.path.realpath(__file__).split("thales")[0]
    sys.path.append(module_dir)
    import thales


from thales.bots.FoXyLady.test import TestTradeHandler, TestDataGenerator, TestBot, BOT_NAME
from thales.config.utils import MINUTE_FORMAT
from thales.positions import PositionManager


data_source = TestDataGenerator()

min_year = 2015
one_year = int(366 * 5/7)
dates = data_source.dates.iloc[1: -one_year+1]
dates = dates.loc[dates.dt.year >= min_year]


if __name__ == "__main__":

    for n_tests in range(10):
        try:
            test_start = pd.Timestamp(random.choice(dates.values))
            test_n_days = one_year
            test_name = f"{test_start.strftime(MINUTE_FORMAT)}_n={test_n_days}"
            event_handler = TestTradeHandler()
            data_source = TestDataGenerator()
            bot = TestBot(src=data_source, handler=event_handler, start=test_start, n_days=test_n_days,
                          test_name=test_name)
            bot()
            PositionManager.calc_bot_performance(bot_name=BOT_NAME, test_name=test_name, test=True, save=True)
        except IndexError:
            continue
