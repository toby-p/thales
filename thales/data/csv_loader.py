
import os
import pandas as pd
import warnings

from thales.config.fieldmaps import apply_fieldmap, get_fieldmap
from thales.config.paths import io_path
from thales.config.sources import validate_source
from thales.config.symbols import MasterSymbols
from thales.config.utils import DEFAULT_SUBDIR, merge_dupe_cols, SECOND_FORMAT


class CSVLoader:
    """API for loading a CSV file into memory as a Pandas DataFrame to do
    something with it."""

    @staticmethod
    def load_by_symbol(*sym: str, src: str = None, subdir: str = None,
                       precision: float = 5):
        """Load a DataFrame of stocks for the specified symbols."""
        src = validate_source(src)

        if not subdir:
            subdir = DEFAULT_SUBDIR

        directory = io_path("scraped_data", validate_source(src), subdir)
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
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.round(precision).drop_duplicates()

        # Convert field names to the standard names:
        df = apply_fieldmap(df.reset_index(drop=True), src=src)

        # Clean and de-dupe data:
        df = CSVLoader.clean_dataset(df, src=src)
        df = CSVLoader.dedupe_by_request_time(df)

        # Adjust open/low/high prices if necessary:
        CSVLoader.adjust_prices(df)

        return df.reset_index(drop=True)

    @staticmethod
    def clean_dataset(df: pd.DataFrame, src: str = None):
        """Clean a DataFrame loaded from CSV. If a mixture of standard/custom
        fields have accidentally both been saved, it will convert all to the
        standard format. Also fixes data types.
        """
        src = validate_source(src)
        df = merge_dupe_cols(df)
        fieldmap = get_fieldmap(src)

        # Merge any duplicate standard/custom field columns, keeping only the specified column:
        for keep_col, drop_col in fieldmap.items():
            if (keep_col in df.columns) and (drop_col in df.columns):
                df[keep_col] = df[keep_col].fillna(df[drop_col])
            elif drop_col in df.columns:
                df[keep_col] = df[drop_col]
        drop_cols = fieldmap.values()
        df = df[[c for c in df.columns if c not in drop_cols]]

        # Fix data types:
        df["datetime"] = pd.to_datetime(df["datetime"])
        df["symbol"] = df["symbol"].astype(str).str.upper()
        float_cols = [k for k in fieldmap.keys() if k not in ("datetime", "symbol")]
        for f_col in float_cols:
            df[f_col] = df[f_col].astype(float)

        return df

    @staticmethod
    def dedupe_by_request_time(df: pd.DataFrame):
        """If a symbol has been scraped multiple times in a date period then
        there may be duplicate rows of data for a single period, which will
        cause errors in analysis. This deduplicates the rows, leaving the most
        recently scraped data in place.
        """
        sort_cols = ["datetime", "request_time", "volume"]
        default_request_time = "2020_01_01 00;00;00"
        if "request_time" not in df.columns:
            df["request_time"] = default_request_time
        df["request_time"] = df["request_time"].fillna(default_request_time)
        df["request_time"] = pd.to_datetime(df["request_time"], format=SECOND_FORMAT)
        df.sort_values(by=sort_cols, ascending=True, inplace=True)
        return df.drop_duplicates(subset=["datetime"], keep="last")

    @staticmethod
    def rows_need_adjusting(df: pd.DataFrame, precision: int = 5):
        """Returns all rows in a DataFrame where the data suggests open/high/low
        prices need adjusting based on the adjusted close price."""
        high_condition = (df["high"].round(precision) > df["close"].round(precision))
        low_condition = (df["low"].round(precision) > df["close"].round(precision))
        open_condition = (df["open"].round(precision) > df["close"].round(precision))
        return df[high_condition & low_condition & open_condition]

    @staticmethod
    def adjust_prices(df: pd.DataFrame):
        """Add adjusted price columns for `low`, `open`, and `high` based on the
        ratio between the close price and adjusted close price."""
        if "raw_close" in df.columns and "close" in df.columns:
            if len(CSVLoader.rows_need_adjusting(df)):
                adjustment_factor = df["close"] / df["raw_close"]
                for col in ("low", "high", "open"):
                    df[col] = df[col] * adjustment_factor
                assert not len(CSVLoader.rows_need_adjusting(df)), "Something went wrong in adjusting prices."
