
import pandas as pd

from thales.config.sources import validate_source
from thales.config.utils import DEFAULT_SUBDIR
from thales.data import CSVLoader
from thales.indicators import apply_df_indicator, apply_series_indicator, dataframe_indicators, series_indicators


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

        # Attrs to store loaded data:
        self._loaded = dict()  # Tracks data has already been loaded for which symbols.
        self.df = pd.DataFrame()

        # Attrs storing data for machine learning tasks:
        self.ys = pd.DataFrame()

    def _update_y_index(self):
        """Reset the index of the `ys` attr to match that of the `df` attr."""
        self.ys = self.ys.reindex(self.df.index)

    def create_y_future(self, sym: str, ohlct: str = "c", n: int = 1):
        """Create a target `y` column which is simply an ohlct price shifted `n`
        time periods (i.e. rows) into the future."""
        df_col_name = self._make_column_name(sym, ohlct)
        y_col_name = f"{df_col_name}_n+{n}"
        if y_col_name not in self.ys.columns:
            src_series = self.df[df_col_name]
            self._update_y_index()
            self.ys[y_col_name] = src_series.shift(-n)
        return y_col_name

    def create_y_pc(self, sym: str, ohlct: str = "c", n: int = 1):
        """Create a target `y` column which is the percentage difference between
        the current date's ohlct price and the same ohlct price shifted `n` time
        periods (i.e. rows) into the future."""
        df_col_name = self._make_column_name(sym, ohlct)
        y_col_name = f"{df_col_name}_n+{n}_pc"
        if y_col_name not in self.ys.columns:
            n_ycol = self.create_y_future(sym=sym, ohlct=ohlct, n=n)
            self._update_y_index()
            self.ys[y_col_name] = (self.ys[n_ycol] - self.df[df_col_name]) / self.df[df_col_name]
        return y_col_name

    @staticmethod
    def full_column_name(*ohlct: str) -> list:
        """Get full column names by passing abbreviation, e.g. get `close` by
        passing `c`."""
        col_dict = {s[0]: s for s in ("open", "low", "high", "close", "typical")}
        return [col_dict[i[0].lower()] for i in ohlct]

    def _make_column_name(self, sym: str, ohlct: str):
        return f"{sym.upper()}_{self.full_column_name(ohlct)[0]}"

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

    def apply_series_indicator(self, sym: str, ohlct: str = "c", **kwargs):
        """Apply series indicators to `df` based on keyword arguments, where
        keys are indicator names, and value are dicts of valid kwargs for that
        indicator function. If no kwargs are passed all indicators are applied
        with default values.

        Args:
            sym: which symbol's column to apply indicators to.
            ohlct: column to apply indicators to from o/h/l/c/t.
        """
        if not kwargs:
            kwargs = {k: None for k in series_indicators.keys()}
        for k, v in kwargs.items():
            if not v:
                v = dict()
            apply_series_indicator(df=self.df, indicator=k, target=self._make_column_name(sym, ohlct), **v)

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
