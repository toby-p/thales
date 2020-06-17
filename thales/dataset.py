
import os
import pandas as pd
import warnings

from thales.config import apply_fieldmap, DEFAULT_SUBDIR, MasterSymbols, validate_source
from thales.config.utils import DIR_SCRAPED_DATA


class DataSet:
    """API for loading a CSV file into memory as a Pandas DataFrame to do
    something with it."""

    @staticmethod
    def load_by_symbol(*sym: str, src: str = None, subdir: str = None,
                       precision: float = 5):
        """Load a DataFrame of stocks for the specified symbols."""
        src = validate_source(src)

        if not subdir:
            subdir = DEFAULT_SUBDIR

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

        return apply_fieldmap(df.reset_index(drop=True), src=src)
