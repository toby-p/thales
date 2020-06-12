
import os
import pandas as pd
import warnings

from thales.config import get_fieldmap, MasterSymbols, validate_source
from thales.utils import DIR_SCRAPED_DATA


class DataSet:
    """API for loading a CSV file into memory to do something with it."""

    default_src = "alphavantage"
    default_subdir = "TIME_SERIES_DAILY_ADJUSTED"

    def __init__(self, src: str = None):
        pass

    @staticmethod
    def apply_fieldmap(df: pd.DataFrame, src: str = None):
        """Map the default fieldnames onto the source's custom field names. If
        `datetime` is found in the mapped in fields it is set as the index."""
        if not src:
            src = DataSet.default_src
        fieldmap = get_fieldmap(src)
        rename = {v: k for k, v in fieldmap.items() if v in df.columns}
        return df.rename(columns=rename)

    @staticmethod
    def load_by_symbol(*sym: str, src: str = None, subdir: str = None,
                       precision: float = 5):
        """Load a DataFrame of stocks for the specified symbols."""
        if not src:
            src = DataSet.default_src
        if not subdir:
            subdir = DataSet.default_subdir

        directory = os.path.join(DIR_SCRAPED_DATA, validate_source(src), subdir)
        assert os.path.isdir(directory), f"No data directory: {directory}"
        csvs = os.listdir(directory)

        if not sym:
            sym = MasterSymbols.get()  # Loads entire master symbols list.

        targets = [f"{str.upper(s)}.csv" for s in sym]
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
        df = df.round(precision).drop_duplicates()

        return DataSet.apply_fieldmap(df.reset_index(drop=True), src=src)
