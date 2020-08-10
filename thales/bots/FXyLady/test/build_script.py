"""Build out the test data in the right format for the bot."""

import json
import numpy as np
import os
import pandas as pd
import sys

try:
    import thales
except ModuleNotFoundError:
    module_dir = os.path.realpath(__file__).split("thales")[0]
    sys.path.append(module_dir)
    import thales
from thales.config.paths import DIR_BOT_DATA, DIR_TOY_DATA
from thales.config.utils import date_col_from_datetime_col, DATE_FORMAT, MINUTE_FORMAT
from thales.data import load_toy_dataset


# Build out the directories for the package data:
BOT_NAME = "FXyLady"
BOT_DIR = os.path.join(DIR_BOT_DATA, BOT_NAME)
DATA_DIR = os.path.join(BOT_DIR, "test")
if not os.path.isdir(DATA_DIR):
    os.mkdir(DATA_DIR)
SUBDIRS = ["67_data", "minute_data"]
for sd in SUBDIRS:
    subdir = os.path.join(DATA_DIR, sd)
    if not os.path.isdir(subdir):
        os.mkdir(subdir)
DIR_67 = os.path.join(DATA_DIR, "67_data")
DIR_MINUTE = os.path.join(DATA_DIR, "minute_data")


# Whether or not to rebuild files which already exist:
REBUILD = False


# Create the JSON files of each day's high/low/mean prices in the hours 6-7am:
def make_67_json(year: int):
    """Create the test JSON files of the high/low/mean price between the hours
    of 6 and 7am each day."""
    fn = f"GBPJPY_{year}_1m.csv"
    year_df = load_toy_dataset(fn)
    date_col_from_datetime_col(year_df)
    year_df["hour"] = year_df["datetime"].dt.hour
    year_df["67_high"] = np.where(year_df["hour"].isin((6, 7)), year_df["high"], np.nan)
    year_df["67_low"] = np.where(year_df["hour"].isin((6, 7)), year_df["low"], np.nan)
    high = year_df.groupby(["date"])["67_high"].max()
    low = year_df.groupby(["date"])["67_low"].min()
    data = pd.concat([high, low], axis=1).dropna()
    data["mean"] = data.mean(axis=1)

    # Save each row as a JSON file:
    for ix in data.index:
        fn = f"{ix.strftime(DATE_FORMAT)}.json"
        fp = os.path.join(DIR_67, fn)
        with open(fp, "w") as f:
            json.dump(data.loc[ix].to_dict(), f)
    print(f"Saved all 6-7am data JSON files for year: {year}")


years_parsed = {int(f[:4]) for f in os.listdir(DIR_67) if f.endswith(".json")}
csv_years = {int(f[7:11]) for f in os.listdir(DIR_TOY_DATA) if f.startswith("GBPJPY_") and f.endswith("_1m.csv")}
if not REBUILD:
    csv_years = csv_years - years_parsed
for y in csv_years:
    make_67_json(y)


# Make the CSV file listing all dates which we have data for:
fp = os.path.join(DATA_DIR, "dates.csv")
if REBUILD or not os.path.exists(fp):
    dates = sorted({f[:-5] for f in os.listdir(os.path.join(DATA_DIR, "67_data")) if f.endswith(".json")})
    df = pd.DataFrame({"dates": dates})
    df.to_csv(fp, encoding="utf-8", index=False)
    print("Saved CSV of all dates we have data for.")


# Make the JSON files of all the minute data (TAKES A LONG TIME!):
def make_minute_json(year: int):
    fn = f"GBPJPY_{year}_1m.csv"
    year_df = load_toy_dataset(fn)
    year_df["datetime"] = year_df["datetime"].dt.strftime(MINUTE_FORMAT)
    for ix in year_df.index:
        row = year_df.loc[ix].to_dict()
        fn = f"{row.pop('datetime')}.json"
        json_fp = os.path.join(DIR_MINUTE, fn)
        with open(json_fp, "w") as f:
            json.dump(row, f)
    print(f"Saved all minute data JSON files for year: {year}")


years_parsed = {int(f[:4]) for f in os.listdir(DIR_MINUTE) if f.endswith(".json")}
csv_years = {int(f[7:11]) for f in os.listdir(DIR_TOY_DATA) if f.startswith("GBPJPY_") and f.endswith("_1m.csv")}
if not REBUILD:
    csv_years = csv_years - years_parsed
for y in csv_years:
    make_minute_json(y)


print("All test files built.")
