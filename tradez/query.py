
import os
import pandas as pd
import warnings

from tradez.symbols import get_symbols_master
from tradez.utils import DIR_SCRAPED_DATA, SOURCES


def load_data(*symbol: str, source: str = "alphavantage",
              subdir: str = "TIME_SERIES_DAILY_ADJUSTED"):
    assert source in SOURCES, f"Invalid source: {source}"
    directory = os.path.join(DIR_SCRAPED_DATA, source)
    assert os.path.isdir(directory), f"No data for source `{source}`."
    directory = os.path.join(directory, subdir)
    assert os.path.isdir(directory), f"Invalid `subdir`: {subdir}"
    csvs = os.listdir(directory)

    if not symbol:
        symbol = get_symbols_master()

    targets = [f"{str.upper(s)}.csv" for s in symbol]
    to_load = sorted(set(csvs) & set(targets))
    missing = sorted(set(targets) - set(to_load))
    if missing:
        missing = [m[:-4] for m in missing]
        warnings.warn(f"No data available for symbols: {', '.join(missing)}")
    if not to_load:
        return

    dfs = list()
    for csv in to_load:
        fp = os.path.join(directory, csv)
        new = pd.read_csv(fp, encoding="utf-8")
        dfs.append(new)
    df = pd.concat(dfs, sort=False)
    df["DateTime"] = pd.to_datetime(df["DateTime"])

    return df.reset_index(drop=True)
