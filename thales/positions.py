"""Manage trading positions."""

import datetime
import json
import matplotlib.pyplot as plt
from numbers import Number
import os
import pandas as pd
import shutil
from uuid import uuid4
import yaml

from thales.config.bots import validate_bot_name
from thales.config.paths import io_path
from thales.config.utils import MILISECOND_FORMAT, now_str


class _Position:
    """Abstract class defining general attributes of a trading position."""
    ptype = ""

    __slots__ = ["test", "uuid", "bot_name", "open_timestamp", "buy_price",
                 "amount", "close_timestamp", "sell_price", "directory", "metadata"]

    def __init__(self, open_timestamp: str, buy_price: float, amount: float,
                 test: bool = True, bot_name: str = None,
                 close_timestamp: str = None, sell_price: float = None,
                 uuid: str = None, directory: str = None, **metadata):
        """Capture details of a single trade. Metadata associated with the trade
        can be passed as keyword arguments."""
        self.test = test
        self.uuid = uuid if uuid else str(uuid4())
        if bot_name:
            bot_name = validate_bot_name(bot_name)
        self.bot_name = bot_name
        if directory:
            assert os.path.isdir(directory), f"Invalid dirtory location: {directory}"
            self.directory = directory
        else:
            self.directory = io_path("positions", bot_name, make_subdirs=True)
        self.open_timestamp = open_timestamp
        self.buy_price = buy_price
        self.amount = amount
        self.close_timestamp = close_timestamp
        self.sell_price = sell_price
        self.metadata = self._convert_metadata(**metadata)
        self.save()

    @staticmethod
    def _convert_metadata(**metadata):
        """Convert numpy/datetime types in metadata to Python objects so that
        they can be JSON serialized."""
        converted_metadata = dict()
        for k, v in metadata.items():
            if type(k).__module__ == "numpy":
                k = k.item()
            elif type(k).__module__ == "datetime":
                k = str(k)
            if type(v).__module__ == "numpy":
                v = v.item()
            elif type(v).__module__ == "datetime":
                v = str(v)
            converted_metadata[k] = v
        return converted_metadata

    @property
    def is_open(self):
        return (not self.close_timestamp) and (not isinstance(self.sell_price, Number))

    @property
    def sell_buy_ratio(self):
        if not self.is_open:
            return self.sell_price / self.buy_price

    @property
    def hold_duration(self):
        if not self.close_timestamp:
            close = datetime.datetime.now()
        else:
            close = datetime.datetime.strptime(self.close_timestamp, MILISECOND_FORMAT)
        return close - datetime.datetime.strptime(self.open_timestamp, MILISECOND_FORMAT)

    @property
    def name(self):
        starts_with = "test__" if self.test else ""
        return f"{starts_with}{self.uuid}"

    def sell(self, timestamp: str, price: float, **metadata):
        assert self.is_open, f"Position already closed."
        assert timestamp and isinstance(price, Number), f"timestamp and price must be passed"
        self.close_timestamp = timestamp
        self.sell_price = price
        self.metadata = {**self.metadata, **self._convert_metadata(**metadata)}
        assert not self.is_open, "Something went wrong."
        self.save()

    def save(self):
        """Save YAML file of position details in internal IO directories."""
        # The `open` filepath is required for saving open OR closed
        # positions, as it needs to be removed if closing the position:
        open_fp = os.path.join(self.directory, "open", f"{self.name}.yaml")
        data = json.loads(repr(self))
        if self.is_open:
            with open(open_fp, "w") as stream:
                yaml.safe_dump(data, stream)
        else:
            # The `closed` position filepath is only required to close it:
            close_fp = os.path.join(self.directory, "closed", f"{self.name}.yaml")
            with open(close_fp, "w") as stream:
                yaml.safe_dump(data, stream)
            if os.path.exists(open_fp):
                os.remove(open_fp)

    def __repr__(self):
        return json.dumps({**{"ptype": self.ptype}, **{k: getattr(self, k) for k in self.__slots__ + ["delta"]}})

    def __str__(self):
        return f"{self.ptype}Position('{self.name}')"


class Long(_Position):
    """Class for recording long trading positions."""
    ptype = "Long"

    def __init__(self, open_timestamp: str, buy_price: float, amount: float,
                 test: bool = True, bot_name: str = None,
                 close_timestamp: str = None, sell_price: float = None,
                 uuid: str = None, **metadata):
        super().__init__(open_timestamp=open_timestamp, buy_price=buy_price, amount=amount, test=test,
                         bot_name=bot_name, close_timestamp=close_timestamp, sell_price=sell_price,
                         uuid=uuid, **metadata)

    @property
    def delta(self):
        if not self.is_open:
            return (self.amount * self.sell_buy_ratio) - self.amount


class Short(_Position):
    """Class for recording long trading positions."""
    ptype = "Short"

    def __init__(self, open_timestamp: str, buy_price: float, amount: float,
                 test: bool = True, bot_name: str = None,
                 close_timestamp: str = None, sell_price: float = None,
                 uuid: str = None, **metadata):
        super().__init__(open_timestamp=open_timestamp, buy_price=buy_price, amount=amount, test=test,
                         bot_name=bot_name, close_timestamp=close_timestamp, sell_price=sell_price,
                         uuid=uuid, **metadata)

    @property
    def delta(self):
        if not self.is_open:
            return -((self.amount * self.sell_buy_ratio) - self.amount)


class PositionManager:
    """API for managing trading positions."""
    def __init__(self, bot_name: str = None, test: bool = True,
                 create_test_dir: bool = False, open_most_recent: bool = False,
                 open_timestamp: str = None):
        """Create a new instance for managing positions.

        Args:
            bot_name: optionally specify a bot name, in which case all positions
                will be managed in the bot sub-directory of the main directory.
            test: whether or not positions being managed are test or real.
            create_test_dir: if True, create a time-stamped sub-directory for
                managing positions so they can be evaluated for a back-test.
            open_most_recent: if True manage the positions in the most recently
                created back-test sub-directory.
            open_timestamp: if passed manage the positions in the back-test
                sub-directory specified.
        """
        choices = [i for i in (create_test_dir, open_most_recent, open_timestamp) if i]
        assert len(choices) <= 1, "Only 1 option can be used from: create_test_dir, open_most_recent, open_timestamp"
        subdirs = list()
        self.bot_name = validate_bot_name(bot_name) if bot_name else None
        subdirs.append(self.bot_name)
        if create_test_dir:
            test = True
            self.timestamp = now_str(fmt=MILISECOND_FORMAT)
        elif open_most_recent:
            test = True
            top_dir = io_path("positions", *subdirs, make_subdirs=True)
            timestamps = list()
            for i in os.listdir(top_dir):
                if os.path.isdir(os.path.join(top_dir, i)):
                    try:
                        timestamps.append(datetime.datetime.strptime(i, MILISECOND_FORMAT))
                    except ValueError:
                        continue
            assert len(timestamps), f"No timestamped directories found in directory: {top_dir}"
            most_recent = max(timestamps)
            self.timestamp = most_recent.strftime(MILISECOND_FORMAT)
        elif open_timestamp:
            test = True
            top_dir = io_path("positions", *subdirs, make_subdirs=True)
            assert open_timestamp in os.listdir(top_dir), \
                f"timestamp {open_timestamp} not found in directory: {top_dir}"
            self.timestamp = open_timestamp
        else:
            self.timestamp = None
        subdirs.append(self.timestamp)
        self.dir_fp = io_path("positions", *subdirs, make_subdirs=True)
        self.open_fp = io_path("positions", *subdirs, "open", make_subdirs=True)
        self.closed_fp = io_path("positions", *subdirs, "closed", make_subdirs=True)
        self.test = test
        # DataFrame which can be constructed to analyse all positions:
        self.df = pd.DataFrame()

    def _list_positions(self, closed: bool = True):
        fp = self.closed_fp if closed else self.open_fp
        files = [f[:-5] for f in os.listdir(fp) if f.endswith(".yaml")]
        if self.test:
            return [f for f in files if f.startswith("test_")]
        else:
            return [f for f in files if not f.startswith("test_")]

    @property
    def open_positions(self):
        return self._list_positions(closed=False)

    @property
    def closed_positions(self):
        return self._list_positions(closed=True)

    def get_position(self, position_name: str):
        """Open a single position as an instance of either a `Long` or `Short`
        position object."""
        uuid = position_name.split("__")[-1]
        for directory in (self.open_fp, self.closed_fp):
            files = os.listdir(directory)
            files = [f for f in files if (uuid in f) and (f.endswith(".yaml"))]
            if len(files) == 1:
                fp = os.path.join(directory, f"{files[0]}")
                with open(fp, "r") as stream:
                    position_data = yaml.safe_load(stream)
                    cls = dict(Short=Short, Long=Long)[position_data.pop("ptype")]
                    cls_data = {**{k: v for k, v in position_data.items() if k in _Position.__slots__},
                                **{"directory": self.dir_fp}}
                    metadata = cls_data.pop("metadata", dict())
                    return cls(**cls_data, **metadata)
            elif len(files) > 1:
                raise ValueError(f"Multiple positions found for uuid: {uuid}")
        raise ValueError(f"No position found for uuid: {uuid}")

    def del_test_positions(self, del_open: bool = True,
                           del_closed: bool = True):
        """Delete all YAML files of positions starting with 'test__' in the
        open or closed position directories (WARNING: can't be undone)."""
        directories = [t[1] for t in ((del_open, self.open_fp), (del_closed, self.closed_fp)) if t[0]]
        for directory in directories:
            files = [f for f in os.listdir(directory) if (f.startswith("test__")) and (f.endswith(".yaml"))]
            for f in files:
                os.remove(os.path.join(directory, f))
            if len(files):
                print(f"Deleted {len(files):,} files from directory: {directory}")

    def del_test_dir(self):
        """Delete the entire time-stamped test directory (WARNING: can't be
        undone)."""
        if self.timestamp:
            deleted = self.dir_fp
            shutil.rmtree(self.dir_fp)
            self.timestamp = None
            self.dir_fp = None
            self.closed_fp = None
            self.open_fp = None
            self.bot_name = None
            print(f"Deleted directory & all contents: {deleted}")

    def open_new_position(self, ptype: str, open_timestamp: str,
                          buy_price: float, amount: float, test: bool = True,
                          **metadata):
        """Open a new position."""
        cls = dict(short=Short, long=Long)[ptype.lower()]
        pos = cls(open_timestamp=open_timestamp, buy_price=buy_price, amount=amount,
                  test=test, bot_name=self.bot_name, directory=self.dir_fp, **metadata)
        return pos

    def close_position(self, position_name: str, timestamp: str, price: float,
                       **metadata):
        pos = self.get_position(position_name)
        pos.sell(timestamp=timestamp, price=price, **metadata)

    def save_metadata(self, **data):
        """Save any arbitrary data to the file `metadata.yaml` in the position
        directory, e.g. to provide details of back-tests. If any keys passed
        already exist in the metadata file data will be overwritten."""
        if data:
            old_data = self.metadata
            data = {**old_data, **data}
            with open(os.path.join(self.dir_fp, "metadata.yaml"), "w") as stream:
                yaml.safe_dump(data, stream)

    @property
    def metadata(self):
        """Get any metadata currently saved in the position directory."""
        fp = os.path.join(self.dir_fp, "metadata.yaml")
        if os.path.exists(fp):
            with open(fp, "r") as stream:
                return yaml.safe_load(stream)
        else:
            return dict()

    def construct_dataframe(self):
        """Construct a DataFrame of all position details including metadata for
        analysis & plotting."""
        positions = [self.get_position(p) for p in self.open_positions + self.closed_positions]
        metadata = pd.DataFrame([{**{"uuid": p.uuid}, **p.metadata} for p in positions])
        df = pd.DataFrame([json.loads(repr(p)) for p in positions])
        for date_col in ("open_timestamp", "close_timestamp"):
            df[date_col] = pd.to_datetime(df[date_col], format=MILISECOND_FORMAT)
            df[date_col] = pd.to_datetime(df[date_col], format=MILISECOND_FORMAT)
        df = df.sort_values(by=["open_timestamp"]).reset_index(drop=True)
        df["delta_cumsum"] = df["delta"].cumsum()
        df = pd.merge(df, metadata, left_on="uuid", right_on="uuid", how="outer")
        self.df = df

    @property
    def cumsum(self):
        """DataFrame of all the changes in the cumulative sum of deltas, from
        both the open and close timestamps of trades."""
        df = self.df[["open_timestamp", "close_timestamp", "delta_cumsum"]].dropna()
        o_df = pd.DataFrame( {"timestamp": df["open_timestamp"],
                              "cumsum": [0] + list(df["delta_cumsum"].shift(1).iloc[1:])})
        c_df = pd.DataFrame({"timestamp": df["close_timestamp"], "cumsum": df["delta_cumsum"]})
        return o_df.append(c_df).set_index("timestamp").sort_index()

    def plot_trades(self, figsize=(12, 8)):
        """Plot all trades recorded in this instance."""
        df = self.df.dropna(subset=["delta_cumsum"])
        fig, ax = plt.subplots(figsize=figsize)
        for i, row in df.iterrows():
            color = "g" if row["delta"] >= 0 else "r"
            ax.plot([row["open_timestamp"], row["close_timestamp"]], [row["delta"], row["delta"]],
                    color=color, linewidth=3)
            ax.scatter([row["open_timestamp"], row["close_timestamp"]], [row["delta"], row["delta"]],
                       marker=".", color=color, s=75)
        ax.plot(self.cumsum, linewidth="3", color="#9933ff", alpha=0.5, label="cumulative_sum")
        if "last_timestamp" in self.metadata:  # Plot any remaining open trades:
            xmax = datetime.datetime.strptime(self.metadata["last_timestamp"], MILISECOND_FORMAT)
            open_df = self.df.loc[(self.df["open_timestamp"].notna()) & (self.df["close_timestamp"].isna())]
            for i, row in open_df.iterrows():
                ax.plot([row["open_timestamp"], xmax], [0, 0], color="#ff9900", linewidth=3)
                ax.scatter(row["open_timestamp"], 0, marker=".", color="#ff9900", s=75)
            # Extend cumsum line:
            final_delta = self.cumsum.iloc[-1].values[0]
            ax.plot([self.cumsum.index[-1], xmax], [final_delta, final_delta],
                    linewidth="3", color="#9933ff", alpha=0.5)
        else:
            xmax = df["close_timestamp"].max()
        xmin = df["open_timestamp"].min()
        ax.hlines(0, xmin, xmax, color="#e6e6e6", linestyle="--")
        ax.set_xlim(xmin, xmax)
        ymin, ymax = ax.get_ylim()
        abs_max = max([abs(ymin), abs(ymax)])
        ax.set_ylim(-abs_max, abs_max)
        for tick in ax.get_xticklabels():
            tick.set_rotation(45)
        ax.set_ylabel("delta", size=16)
        ax.legend()
        return fig


def delete_all_tests(bot_name: str = None):
    """Delete all time-stamped test directories. WARNING: can't be undone."""
    count = 0
    bot_name = validate_bot_name(bot_name) if bot_name else None
    while True:
        try:
            p = PositionManager(bot_name=bot_name, test=True, open_most_recent=True)
            p.del_test_dir()
            count += 1
        except AssertionError:
            break
    if not count:
        print("No test directories to delete.")
    p = PositionManager(bot_name=bot_name)
    p.del_test_positions()
