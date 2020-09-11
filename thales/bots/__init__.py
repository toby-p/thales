"""Abstract base classes for bot implementation."""

import datetime


class DataSource:
    """The DataSource class must have a `generator` method which can be used
    with the `yield` keyword to continually generate new data as per the
    internal logic. For live trading bots, the logic will usually perform an API
    query to get data from a website. For test trading bots, the function
    usually just iterates through some saved file of historic data."""

    def __init__(self):
        pass

    def generator(self, *args, **kwargs):
        # ======================================================================
        # Logic to produce data goes here:
        yield dict(timestamp=datetime.datetime.now(), open=None, high=None, low=None, close=None)
        # ======================================================================


class EventHandler:
    """When called with data, the EventHandler class makes a trading decision or
    does something with the data. A bot an have one or more different event
    handlers which all get called each time new data is produced."""

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, **data):
        # ======================================================================
        # Logic for what a handler does when called with some data goes here:
        return data
        # ======================================================================


class TradingBot:
    """Class that when called runs the bot program. It must be instantiated with
    an instance of the `DataSource` class, and one or more instances of the
    `EventHandler` class."""

    def __init__(self, src: DataSource, *handler: EventHandler):
        self.src = src
        self.handlers = handler

    def __call__(self):
        generator = self.src.generator()
        while True:
            data = next(generator)
            for handler in self.handlers:
                handler(**data)
