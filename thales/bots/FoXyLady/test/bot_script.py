
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
from thales.config.utils import MILISECOND_FORMAT
from thales.positions import PositionManager


if __name__ == "__main__":
    test_start = datetime.datetime(2019, 9, 5, 8)
    test_n_days = 50
    position_handler = PositionManager(bot_name=BOT_NAME, test=True, create_test_dir=True)
    position_handler.save_metadata(start_timestamp=test_start.strftime(MILISECOND_FORMAT))
    event_handler = TestTradeHandler(positions=position_handler,
                                     entry_signal=0.2, sell_signal=0.3,
                                     abs_long_stop=0.3, abs_short_stop=0.3)
    data_source = TestDataGenerator()
    bot = TestBot(src=data_source, handler=event_handler, start=test_start, n_days=test_n_days)
    bot()
