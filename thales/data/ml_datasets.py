
import pandas as pd

from thales.config.exceptions import InvalidIndicator
from thales.config.sources import validate_source
from thales.config.utils import DEFAULT_SUBDIR
from thales.data import CSVLoader
from thales.indicators import apply_df_indicator, apply_series_indicator, dataframe_indicators, series_indicators


class OneSymDaily(CSVLoader):
    """Build a data for a machine learning task on a single symbol's daily data.
    """

    src = "alphavantage"

    @staticmethod
    def load(sym: str):
        df = OneSymDaily.load_by_symbol(sym, src=OneSymDaily.src, subdir=DEFAULT_SUBDIR, precision=5)
        df = df.set_index("datetime").sort_index()
        apply_df_indicator(df, "tp")  # Add typical price.
        return df[["open", "low", "high", "close", "typical"]]

    @staticmethod
    def apply_indicators(df: pd.DataFrame, target: str = "close", **kwargs):
        """Apply indicators to the DataFrame based on the keyword arguments,a
        where key is the name of the indicator, and value is another dict of
        keyword args to pass to the indicator function. If no kwargs are passed
        then all indicators are applied with default values."""
        if not kwargs:
            kwargs = {k: None for k in {**series_indicators, **dataframe_indicators}.keys()}
        ignore = ["tp"]  # Typical price is not really an indicator and should have already been applied.
        for k, v in kwargs.items():
            if k in ignore:
                continue
            if not v:
                v = dict()
            if k in series_indicators:
                apply_series_indicator(df=df, indicator=k, target=target, **v)
            elif k in dataframe_indicators:
                apply_df_indicator(df=df, indicator=k, **v)
            else:
                raise InvalidIndicator(k)


class Dataset:
    """Class for building a dataset for machine learning.
    """

    def __init__(self, src: str = None, subdir: str = None,
                 precision: float = 5):
        """Set parameters for loading data into the dataset."""
        # Data loading parameters:
        self.src = validate_source(src)
        self.subdir = DEFAULT_SUBDIR if not subdir else subdir
        self.precision = precision

        # Attributes to store loaded data:
        self._loaded = dict()  # Tracks data has already been loaded for which symbols.
        self.df = pd.DataFrame()

    @staticmethod
    def full_column_name(*c: str) -> list:
        """Get full column names by passing abbreviation,
        e.g. get `close` by passing `c`."""
        col_dict = {s[0]: s for s in ("open", "low", "high", "close", "typical")}
        return [col_dict[i[0].lower()] for i in c]

    def load(self, sym: str, *col):
        """Add data into the `df` attribute.

        Args:
            sym: which symbol to load data for.
            col: which columns to load from o/h/l/c/t.
        """
        assert col, "Must pass at least 1 col arg."
        cols_to_load = [s[0].lower() for s in col]
        already_loaded = self._loaded.get(sym.upper(), list())
        cols_to_load = sorted(set(cols_to_load) - set(already_loaded))
        if not cols_to_load:  # Nothing new to load:
            return
        df = CSVLoader.load_by_symbol(sym, src=self.src, subdir=self.subdir, precision=self.precision)
        df = df.set_index("datetime").sort_index()
        if "t" in cols_to_load:  # Add typical price:
            apply_df_indicator(df, "tp")
        df = df[self.full_column_name(*col)]
        df = df.rename(columns={c: f"{sym}_{c}" for c in df.columns})
        self.df = pd.merge(self.df, df, left_index=True, right_index=True, how="outer")
        self._loaded[sym.upper()] = sorted(set(already_loaded + cols_to_load))

    def apply_series_indicator(self, sym: str, col: str = "c", **kwargs):
        """Apply series indicators to `df` based on keyword arguments, where
        keys are indicator names, and value are dicts of valid kwargs for that
        indicator function. If no kwargs are passed all indicators are applied
        with default values.

        Args:
            sym: which symbol's column to apply indicators to.
            col: column to apply indicators to from o/h/l/c/t.
        """
        if not kwargs:
            kwargs = {k: None for k in series_indicators.keys()}
        target = f"{sym.upper()}_{self.full_column_name(col)[0]}"
        for k, v in kwargs.items():
            if not v:
                v = dict()
            apply_series_indicator(df=self.df, indicator=k, target=target, **v)

    def apply_dataframe_indicator(self, sym: str, **kwargs):
        """Apply DataFrame indicators to `df` based on keyword arguments, where
        keys are indicator names, and value are dicts of valid kwargs for that
        indicator function. If no kwargs are passed all indicators are applied
        with default values.

        Args:
            sym: which symbol's column to apply indicators to.
        """
        if not kwargs:
            kwargs = {k: None for k in dataframe_indicators.keys()}
        kwargs.pop("tp", None)  # Typical price isn't really an indicator.
        full_cols = self.full_column_name("o", "h", "l", "c")
        rename = {f"{sym.upper()}_{c}": c for c in full_cols}
        df_copy = self.df.rename(columns=rename)
        for k, v in kwargs.items():
            if not v:
                v = dict()
            apply_df_indicator(df=df_copy, indicator=k, **v)
        self.df = df_copy.rename(columns={v: k for k, v in rename.items()})
