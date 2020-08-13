
import datetime
import os
import sys

try:
    import thales
except ModuleNotFoundError:
    module_dir = os.path.realpath(__file__).split("thales")[0]
    sys.path.append(module_dir)
    import thales
from thales.bots.FoXyLady.test import TestTradeHandler, TestDataGenerator, TestBot, BOT_NAME
from thales.config.utils import MINUTE_FORMAT
from thales.positions import ManagePositions


if __name__ == "__main__":
    test_start = datetime.datetime(2011, 8, 12, 8)
    test_n_days = 20
    test_name = f"{test_start.strftime(MINUTE_FORMAT)}_n={test_n_days}"
    event_handler = TestTradeHandler()
    data_source = TestDataGenerator(test_name=test_name)
    bot = TestBot(src=data_source, handler=event_handler, start=test_start, n_days=test_n_days)
    bot()
    ManagePositions.bot_performance(bot_name=BOT_NAME, test_name=test_name, test=True, save=True)
