"""Manage trading positions."""

import datetime
import json
import os
from uuid import uuid4

from thales.config.bots import validate_bot_name
from thales.config.paths import io_path
from thales.config.utils import MILISECOND_FORMAT


class Position:

    __slots__ = ["test", "test_name", "uuid", "bot_name", "open_timestamp", "buy_price", "amount", "close_timestamp",
                 "sell_price", "metadata"]

    def __init__(self, open_timestamp: str, buy_price: float, amount: float,
                 test: bool = True, bot_name: str = None,
                 close_timestamp: str = None, sell_price: float = None,
                 uuid: str = None, test_name: str = None, **metadata):
        """Capture details of a single trade. Metadata associated with the trade
        can be passed as JSON-serialisable keyword arguments."""
        self.test = test
        self.test_name = test_name
        self.uuid = uuid if uuid else str(uuid4())
        if bot_name:
            bot_name = validate_bot_name(bot_name)
        self.bot_name = bot_name

        self.open_timestamp = open_timestamp
        self.buy_price = buy_price
        self.amount = amount

        self.close_timestamp = close_timestamp
        self.sell_price = sell_price

        self.metadata = metadata

        self.save()

    @property
    def is_open(self):
        return not self.close_timestamp and not self.sell_price

    @property
    def hold_duration(self):
        if not self.close_timestamp:
            close = datetime.datetime.now()
        else:
            close = datetime.datetime.strptime(self.close_timestamp, MILISECOND_FORMAT)
        return close - datetime.datetime.strptime(self.open_timestamp, MILISECOND_FORMAT)

    @property
    def name(self):
        test_name = "test" if self.test else ""
        test_name += f"_{self.test_name}" if self.test_name else ""
        name_elements = [test_name, self.bot_name if self.bot_name else None, self.uuid]
        return "__".join([f for f in name_elements if f])

    @property
    def sell_buy_ratio(self):
        if not self.is_open:
            return self.sell_price / self.buy_price

    @property
    def delta(self):
        if not self.is_open:
            return (self.amount * self.sell_buy_ratio) - self.amount

    def sell(self, timestamp: str, price: float, **metadata):
        assert self.is_open, f"Position already closed."
        self.close_timestamp = timestamp
        self.sell_price = price
        self.metadata = {**self.metadata, **metadata}
        self.save()

    def save(self):
        if self.is_open:
            fp = io_path("positions", "open", filename=f"{self.name}.json")
            with open(fp, "w") as f:
                json.dump(repr(self), f)
        else:
            fp = io_path("positions", "closed", filename=f"{self.name}.json")
            with open(fp, "w") as f:
                json.dump(repr(self), f)
            open_fp = io_path("positions", "open", filename=f"{self.name}.json")
            if os.path.exists(open_fp):
                os.remove(open_fp)

    def __repr__(self):
        return json.dumps({k: getattr(self, k) for k in self.__slots__})

    def __str__(self):
        return f"Position('{self.name}')"

    def __add__(self, other):
        return self.delta + other.delta


def _list_positions(bot_name: str = None, test: bool = True,
                    test_name: str = None, closed: bool = True):
    subdir = "closed" if closed else "open"
    files = [f[:-5] for f in os.listdir(io_path("positions", subdir)) if f.endswith(".json")]
    starts_with = "test_" if test else ""
    bot_name = f"_{validate_bot_name(bot_name)}_" if bot_name else ""
    test_name = f"_{test_name}_" if test_name else ""
    return [f for f in files if f.startswith(starts_with) and bot_name in f and test_name in f]


class ManagePositions:

    @staticmethod
    def list_open_positions(bot_name: str = None, test: bool = True,
                            test_name: str= None):
        return _list_positions(bot_name=bot_name, test=test, test_name=test_name, closed=False)

    @staticmethod
    def list_closed_positions(bot_name: str = None, test: bool = True,
                              test_name: str= None):
        return _list_positions(bot_name=bot_name, test=test, test_name=test_name, closed=True)

    @staticmethod
    def list_tests(bot_name: str):
        """Get a list of names of backtests performed for a specific bot."""
        directory = io_path("back_tests", validate_bot_name(bot_name))
        return [f[:-5] for f in os.listdir(directory) if f.endswith(".json")]

    @staticmethod
    def get_position(position_name: str):
        uuid = position_name.split("__")[-1]
        for directory in (io_path("positions", "closed"), io_path("positions", "open")):
            files = os.listdir(directory)
            files = [f for f in files if (uuid in f) and (f.endswith(".json"))]
            if len(files) == 1:
                fp = os.path.join(directory, f"{files[0]}")
                with open(fp, "r") as f:
                    position_data = json.loads(json.load(f))
                    cls_data = {k: v for k, v in position_data.items() if k in Position.__slots__}
                    metadata = cls_data.pop("metadata", dict())
                    return Position(**cls_data, **metadata)
            elif len(files) > 1:
                raise ValueError(f"Multiple positions found for uuid: {uuid}")
        raise ValueError(f"No position found for uuid: {uuid}")

    @staticmethod
    def delete_all_test_positions(del_open: bool = True,
                                  del_closed: bool = True):
        """Delete all JSON files of positions starting with 'test__' in both the
        open and closed position directories (warning: can't be undone!)"""
        directories = list()
        if del_open:
            directories.append(io_path("positions", "open"))
        if del_closed:
            directories.append(io_path("positions", "closed"))
        for directory in directories:
            files = [f for f in os.listdir(directory) if (f.startswith("test_")) and (f.endswith(".json"))]
            for f in files:
                os.remove(os.path.join(directory, f))

    @staticmethod
    def calc_bot_performance(bot_name: str, test: bool = True, test_name: str = None,
                             save: bool = False):
        closed_positions = ManagePositions.list_closed_positions(bot_name=bot_name, test=test, test_name=test_name)
        if not closed_positions:
            return {"number_trades": 0}
        deltas, sell_buy_ratios, hold_durations = list(), list(), list()
        for p in closed_positions:
            position = ManagePositions.get_position(p)
            deltas.append(position.delta)
            sell_buy_ratios.append(position.sell_buy_ratio)
            hold_durations.append(position.hold_duration)
        wins = sum([1 if d > 0 else 0 for d in deltas])
        number_trades = len(closed_positions)
        results = {
            "number_trades": number_trades,
            "delta": sum(deltas),
            "sell_buy_ratio": sum(sell_buy_ratios) / number_trades,
            "average_hold_duration": sum(hold_durations, datetime.timedelta()) / number_trades,
            "number_wins": wins,
            "win_pc": wins / number_trades,
        }
        if save:
            assert bot_name and test_name, "Both bot_name and test_name must be passed to save results."
            fp = io_path("back_tests", bot_name, filename=f"{test_name}.json", make_subdirs=True, make_file=False)
            with open(fp, "w") as f:
                json.dump({k: str(v) for k, v in results.items()}, f)
            print(f"Saved results for bot {bot_name} - test name {test_name}")
        return results

    @staticmethod
    def open_bot_test_performance(bot_name: str, test_name: str):
        """Open the results of a back test."""  # TODO: change data types of dict returned.
        bot_name = validate_bot_name(bot_name)
        test_name = f"{test_name}.json" if not test_name.endswith(".json") else test_name
        fp = io_path("back_tests", validate_bot_name(bot_name), test_name)
        if os.path.exists(fp):
            with open(fp, "r") as f:
                return json.load(f)  # json.loads(json.load(f))
        else:
            assert test_name[:-5] in ManagePositions.list_tests(bot_name)
            return ManagePositions.calc_bot_performance(bot_name, test=True, test_name=test_name, save=True)
