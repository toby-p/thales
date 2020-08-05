"""Boilerplate for implementing a very simple trading bot based on the Observer
pattern for event-handling. In this situation the data source can be thought of
as the event, and handlers are called each time new data is produced.
"""

import datetime
import json
import os
import time

from thales.config.paths import DIR_TEMP


class Handler:

    def __init__(self):
        pass

    def __call__(self, **kwargs):
        # ======================================================================
        # Logic for what the handler does when called with some data goes here.
        # In this case it just saves the data as a timestamped JSON file.
        timestamp_str = kwargs["timestamp"].strftime(format="%Y_%m_%d %H;%M;%S;%f")
        kwargs["timestamp"] = timestamp_str
        fp = os.path.join(DIR_TEMP, f"{timestamp_str}.json")
        with open(fp, "w") as f:
            json.dump(kwargs, f)
        # ======================================================================


class DataSource:

    def __init__(self, *handler: Handler):
        self.handlers = list(handler)

    def __call__(self):
        # ======================================================================
        # Logic to produce data goes here:
        data = dict(timestamp=datetime.datetime.now(), open=None, high=None, low=None, close=None)
        # ======================================================================

        for handler in self.handlers:
            handler(**data)


class Bot:

    def __init__(self, src: DataSource, interval: float = 15):
        self.src = src
        self.interval = interval

    def __call__(self):
        while True:
            self.src()
            time.sleep(self.interval)


if __name__ == "__main__":
    event_handlers = [Handler()]
    data_source = DataSource(*event_handlers)
    bot = Bot(data_source)
    bot()