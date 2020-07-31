"""Template for a very simple implementation of a trading bot."""

import datetime
import json
import os
import random
import time

from thales.config.paths import DIR_TEMP


class DataSource:

    def __init__(self):
        pass

    def __call__(self):
        return {
            "timestamp": datetime.datetime.now(),
            "open": random.random(),
            "high": random.random(),
            "low": random.random(),
            "close": random.random(),
        }


class Handler:

    def __init__(self):
        pass

    def __call__(self, data: dict):
        """Logic for what to do with a single piece of data from a feed."""
        timestamp_str = data["timestamp"].strftime(format="%Y_%m_%d %H;%M;%S;%f")
        data["timestamp"] = timestamp_str
        fp = os.path.join(DIR_TEMP, f"{timestamp_str}.json")
        with open(fp, "w") as f:
            json.dump(data, f)


class Bot:

    def __init__(self, src: DataSource, handler: Handler, interval: float = 1):
        self.src = src
        self.handler = handler
        self.interval = interval

    def __call__(self):
        while True:
            data = self.src()
            self.handler(data)
            time.sleep(self.interval)


if __name__ == "__main__":
    data_source = DataSource()
    event_handler = Handler()
    bot = Bot(data_source, event_handler)
    bot()
