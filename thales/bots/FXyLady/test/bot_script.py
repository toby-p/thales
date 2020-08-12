
import datetime
import os
import sys


try:
    import thales
except ModuleNotFoundError:
    module_dir = os.path.realpath(__file__).split("thales")[0]
    sys.path.append(module_dir)
    import thales
from thales.bots.FXyLady.test import TestTradeHandler, TestDataGenerator, TestBot

if __name__ == "__main__":
    test_start = datetime.datetime(2009, 8, 12, 8)
    test_n_days = 365
    event_handler = TestTradeHandler()
    data_source = TestDataGenerator()
    bot = TestBot(src=data_source, handler=event_handler, start=test_start, n_days=test_n_days)
    bot()
