"""Manage trading positions."""

import json
import os
import uuid

from thales.config.bots import validate_bot_name
from thales.config.paths import DIR_POSITIONS_CLOSED, DIR_POSITIONS_OPEN


class Position:

    def __init__(self, open_timestamp: str, buy_price: float, amount: float,
                 test: bool = True, bot_name: str = None, **kwargs):
        self.test = test
        self.uuid = kwargs.get("uuid", str(uuid.uuid4()))
        if bot_name:
            bot_name = validate_bot_name(bot_name)
        self.bot_name = bot_name

        self.open_timestamp = open_timestamp
        self.buy_price = buy_price
        self.amount = amount

        self.close_timestamp = kwargs.get("close_timestamp")
        self.sell_price = kwargs.get("sell_price")
        self.sell_buy_ratio = kwargs.get("sell_buy_ratio")
        self.delta = kwargs.get("delta")

        self.save()

    @property
    def is_open(self):
        return not (any([self.close_timestamp, self.sell_price, self.sell_buy_ratio, self.delta]))

    @property
    def name(self):
        name_elements = ["test" if self.test else None, self.bot_name if self.bot_name else None, self.uuid]
        return "__".join([f for f in name_elements if f])

    def sell(self, timestamp: str, price: float):
        assert self.is_open and (not self.sell_price), f"Position already closed."
        self.close_timestamp = timestamp
        self.sell_price = price
        self.sell_buy_ratio = self.sell_price / self.buy_price
        self.delta = (self.amount * self.sell_buy_ratio) - self.amount

        self.save()

    def save(self):
        if self.is_open:
            fp = os.path.join(DIR_POSITIONS_OPEN, f"{self.name}.json")
            with open(fp, "w") as f:
                json.dump(repr(self), f)
        else:
            fp = os.path.join(DIR_POSITIONS_CLOSED, f"{self.name}.json")
            with open(fp, "w") as f:
                json.dump(repr(self), f)
            open_fp = os.path.join(DIR_POSITIONS_OPEN, f"{self.name}.json")
            if os.path.exists(open_fp):
                os.remove(open_fp)

    def __repr__(self):
        parameters = ["uuid", "bot_name", "open_timestamp", "buy_price", "amount"]
        if not self.is_open:
            parameters += ["close_timestamp", "sell_price", "sell_buy_ratio", "delta"]
        d = {k: getattr(self, k) for k in parameters}
        return json.dumps(d)

    def __str__(self):
        return f"Position('{self.name}')"


class ManagePositions:

    @staticmethod
    def list_open_positions(bot_name: str = None, test: bool = True):
        files = [f[:-5] for f in os.listdir(DIR_POSITIONS_OPEN) if f.endswith(".json")]
        starts_with = "test__" if test else ""
        starts_with += validate_bot_name(bot_name) if bot_name else ""
        return [f for f in files if f.startswith(starts_with)]

    @staticmethod
    def list_closed_positions(bot_name: str = None, test: bool = True):
        files = [f[:-5] for f in os.listdir(DIR_POSITIONS_CLOSED) if f.endswith(".json")]
        starts_with = "test__" if test else ""
        starts_with += validate_bot_name(bot_name) if bot_name else ""
        return [f for f in files if f.startswith(starts_with)]

    @staticmethod
    def get_position(position_name: str):
        position_uuid = position_name.split("__")[-1]
        for directoy in (DIR_POSITIONS_OPEN, DIR_POSITIONS_CLOSED):
            files = os.listdir(directoy)
            files = [f for f in files if (position_uuid in f) and (f.endswith(".json"))]
            if len(files) == 1:
                fp = os.path.join(directoy, f"{files[0]}")
                with open(fp, "r") as f:
                    return Position(**json.loads(json.load(f)))
            elif len(files) > 1:
                raise ValueError(f"Multiple positions found for uuid: {position_uuid}")
        raise ValueError(f"No position found for uuid: {position_uuid}")

    @staticmethod
    def delete_all_test_positions():
        """Delete all JSON files of positions starting with 'test__' in both the
        open and closed position directories (warning: can't be undone!)"""
        for directory in (DIR_POSITIONS_OPEN, DIR_POSITIONS_CLOSED):
            files = [f for f in os.listdir(directory) if (f.startswith("test__")) and (f.endswith(".json"))]
            for f in files:
                os.remove(os.path.join(directory, f))

