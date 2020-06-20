
import os
import pandas as pd
import warnings

from thales.config import apply_fieldmap, DEFAULT_SUBDIR, get_fieldmap, MasterSymbols, validate_source
from thales.config.utils import DIR_SCRAPED_DATA, merge_dupe_cols


class DataSet:
    """API for loading a CSV file into memory as a Pandas DataFrame to do
    something with it."""

    @staticmethod
    def load_by_symbol(*sym: str, src: str = None, subdir: str = None,
                       precision: float = 5, standard_fields: bool = True):
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

        if standard_fields:
            df = apply_fieldmap(df.reset_index(drop=True), src=src)
        else:
            df = df.reset_index(drop=True)
        return DataSet.clean_dataset(df, src=src, standard_fields=standard_fields)

    @staticmethod
    def clean_dataset(df: pd.DataFrame, src: str = None,
                      standard_fields: bool = False):
        """Clean a DataFrame loaded from CSV. If a mixture of stndard/custom
        fields have accidentally both been saved, it will convert all to the
        desired format as specified by the `standard_fields` arg. Also fixes
        data types.
        """
        src = validate_source(src)
        df = merge_dupe_cols(df)
        fieldmap = get_fieldmap(src)
        datetime_col = "datetime" if standard_fields else fieldmap["datetime"]
        str_col = "symbol" if standard_fields else fieldmap["symbol"]
        float_cols = list()
        for k, v in fieldmap.items():
            if k not in ("datetime", "symbol"):
                float_cols.append(k if standard_fields else v)

        # Merge any duplicate standard/custom field columns, keeping only the specified column:
        if not standard_fields:
            fieldmap = {v: k for k, v in fieldmap.items()}
        for keep_col, drop_col in fieldmap.items():
            if (keep_col in df.columns) and (drop_col in df.columns):
                df[keep_col] = df[keep_col].fillna(df[drop_col])
            elif drop_col in df.columns:
                df[keep_col] = df[drop_col]
        drop_cols = fieldmap.values()
        df = df[[c for c in df.columns if c not in drop_cols]]

        # Fix data types:
        df[datetime_col] = pd.to_datetime(df[datetime_col])
        df[str_col] = df[str_col].astype(str)
        for f_col in float_cols:
            df[f_col] = df[f_col].astype(float)

        return df
