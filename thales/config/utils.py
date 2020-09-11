"""Common variables and utility functions required throughout the package."""

from collections import Counter
import datetime
from dateutil.parser import parse
from keyword import iskeyword
import os
import pandas as pd
import pytz
import requests

from thales.config.paths import io_path


# Default values:
PRICE_COLS = ("open", "high", "low", "close")
DEFAULT_SUBDIR = "TIME_SERIES_DAILY_ADJUSTED"
DEFAULT_FIELDMAP = {
    "datetime": "DATETIME",
    "symbol": "SYMBOL",
    "open": "OPEN",
    "high": "HIGH",
    "low": "LOW",
    "close": "CLOSE",
    "raw_close": "RAW_CLOSE",
    "volume": "VOLUME"
}

# Unicode symbols for status messages:
PASS, FAIL = "\u2714", "\u2718"

# String format for reading/saving datetimes:
DAY_FORMAT = "%Y_%m_%d"
SECOND_FORMAT = "%Y_%m_%d %H;%M;%S"
MILISECOND_FORMAT = "%Y_%m_%d %H;%M;%S;%f"
MINUTE_FORMAT = "%Y_%m_%d %H;%M"
DATE_FORMATS = {"day": DAY_FORMAT, "second": SECOND_FORMAT, "milisecond": MILISECOND_FORMAT, "minute": MINUTE_FORMAT}


def parse_datetime(dt: object, **kwargs) -> datetime.datetime:
    """Take a string or date/datetime object and return a datetime.datetime if
    possible."""
    if isinstance(dt, datetime.datetime):
        return dt
    elif isinstance(dt, datetime.date):  # Convert to datetime:
        return datetime.datetime(dt.year, dt.month, dt.day)
    elif isinstance(dt, pd.Timestamp):
        return dt.to_pydatetime()
    if isinstance(dt, str):
        # Try date formats from most to least specific:
        formats = ("milisecond", "second", "minute", "day")
        for f in formats:
            try:
                return datetime.datetime.strptime(dt, DATE_FORMATS[f])
            except ValueError:
                continue
        return parse(dt, **kwargs)


def sp500():
    """Pandas DataFrame of S&P500 constituents scraped from:

        https://datahub.io/core
    """
    url = "https://pkgstore.datahub.io/core/s-and-p-500-companies/" \
          "constituents_json/data/64dd3e9582b936b0352fdd826ecd3c95/constituents_json.json"
    r = requests.get(url)
    assert r.status_code == 200, f"Unable to get S&P500 from url: {url}"
    return pd.DataFrame(r.json())


def merge_dupe_cols(df: pd.DataFrame):
    """If a DataFrame has somehow ended up with duplicate column names, merge
    those columns into 1. Assumes that the columns don't contain multiple non-NA
    values in a single row."""
    df = df.copy()
    column_count = Counter(df.columns)
    dupe_cols = {k: v for k, v in column_count.items() if v > 1}.items()
    for col, count in dupe_cols:
        dupe_df = df[col].copy()
        col_names = [col]
        for i in range(count-1):
            col_names.append(f"{col}_{i}")
        dupe_df.columns = col_names
        for c in col_names[1:]:
            dupe_df[col] = dupe_df[col].fillna(dupe_df[c])
        df.drop(columns=col, inplace=True)
        df[col] = dupe_df[col]
    return df


def now_str(fmt: str = DAY_FORMAT, timezone: str = "US/Eastern"):
    """String representation of the current datetime.

    Args:
        fmt (str): string format for dates, see: http://strftime.org/
        timezone (str): timezone, see options at: `pytz.all_timezones`.
    """
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    tz_now = utc_now.astimezone(pytz.timezone(timezone)).strftime(fmt)
    return tz_now


def empty_temp_dir():
    """Delete all files in the temp directory (warning: can't be undone!)"""
    files = [f for f in os.listdir(io_path("temp")) if f != "README.txt"]
    for f in files:
        os.remove(io_path("temp", filename=f))


def date_col_from_datetime_col(df, date_col: str = "date",
                               datetime_col: str = "datetime"):
    """Create a date column from a datetime column in a pandas DataFrame
    inplace."""
    data = {"year": df[datetime_col].dt.year,
            "month": df[datetime_col].dt.month,
            "day": df[datetime_col].dt.day}
    df[date_col] = pd.to_datetime(data)


def get_file_modified_date(fp):
    """Get the datetime stamp of when a file was last modified."""
    unix_time = os.path.getmtime(fp)
    return datetime.datetime.fromtimestamp(unix_time)


def is_valid_variable_name(name: str):
    """Boolean test to see if a string is a valid Python variable name."""
    return name.isidentifier() and not iskeyword(name)


def is_iterable(obj: object):
    """Returns True if an object is iterable, else False."""
    try:
        iter(obj)
    except Exception:
        return False
    else:
        return True

