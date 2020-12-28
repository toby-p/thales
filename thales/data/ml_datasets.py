
from itertools import product
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from thales.config.exceptions import InvalidIndicator
from thales.config.sources import validate_source
from thales.config.utils import DEFAULT_SUBDIR
from thales.data import CSVLoader
from thales.indicators import ALL_INDICATORS
from thales.indicators.base import DataFrameInDataFrameOut, DataFrameInSeriesOut, SeriesInSeriesOut, \
    SeriesInDataFrameOut


class MLDataset:
    """Class for building a dataset for machine learning.
    """

    def __init__(self, src: str = None, subdir: str = None,
                 precision: float = 5):
        """Set parameters for loading data into the dataset."""
        # Data loading parameters:
        self.src = validate_source(src)
        self.subdir = DEFAULT_SUBDIR if subdir is None else subdir
        self.precision = precision

        # Attrs to store loaded data:
        self._loaded = dict()  # Tracks data has already been loaded for which symbols.
        self.df = pd.DataFrame()

        # Attrs storing data for machine learning tasks:
        self.ys = pd.DataFrame()
        self.X = pd.DataFrame()
        self.y = pd.Series()
        self.train_X = pd.DataFrame()
        self.train_y = pd.Series()
        self.test_X = pd.DataFrame()
        self.test_y = pd.Series()
        self.val_ix = list()  # List of indices of validation sets.
        self.ml_data = dict()
        self.models = dict()

    def _update_y_index(self):
        """Reset the index of the `ys` attr to match that of the `df` attr."""
        self.ys = self.ys.reindex(self.df.index)

    def create_y_future(self, sym: str, ohlct: str = "c", n: int = 1):
        """Create a target `y` column which is simply an OHLCT price shifted `n`
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
        the current date's OHLCT price and the same OHLCT price shifted `n` time
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
            col: which columns to load from o/h/l/c.
        """
        assert col, "Must pass at least 1 col arg."
        cols_to_load = [s[0].lower() for s in col]
        already_loaded = self._loaded.get(sym.upper(), list())
        cols_to_load = sorted(set(cols_to_load) - set(already_loaded))
        if not cols_to_load:  # Nothing new to load:
            return
        df = CSVLoader.load_by_symbol(sym, src=self.src, subdir=self.subdir, precision=self.precision)
        df = df.set_index("datetime").sort_index()
        df = df[self.full_column_name(*col)]
        df = df.rename(columns={c: f"{sym}_{c}" for c in df.columns})
        self.df = pd.merge(self.df, df, left_index=True, right_index=True, how="outer")
        self._loaded[sym.upper()] = sorted(set(already_loaded + cols_to_load))

    def apply_indicator(self, indicator: str, sym: str, ohlct: str = "c",
                        **params):
        try:
            cls = ALL_INDICATORS[indicator.lower().strip()]
        except KeyError:
            raise (InvalidIndicator(indicator))
        if issubclass(cls, SeriesInSeriesOut) or issubclass(cls, SeriesInDataFrameOut):
            col_name = self._make_column_name(sym, ohlct)
            s = self.df[col_name]
            ti = cls(s, **params)
        elif issubclass(cls, DataFrameInSeriesOut) or issubclass(cls, DataFrameInDataFrameOut):
            ti = cls(df=self.df, sym=sym, pc_ratio_col=ohlct, **params)
        else:
            raise NotImplementedError(f"`apply_indicator` not implemented for: {cls}")

        if isinstance(ti, pd.Series):
            if ti.name not in self.df.columns:
                self.df[ti.name] = ti
        elif isinstance(ti, pd.DataFrame):
            new_cols = set(ti.columns) - set(self.df.columns)
            if new_cols:
                self.df = pd.merge(self.df, ti[new_cols], left_index=True, right_index=True, how="outer")

    @staticmethod
    def _permutations(**params) -> pd.DataFrame:
        return pd.DataFrame(list(product(*params.values())), columns=params.keys())

    def iterate_indicator_params(self, indicator: str, sym: str,
                                 ohlct: str = "c", **params):
        """Apply a technical indicator multiple times with different parameters,
        by passing key-value pairs of parameter name and lists of values. If no
        parameters are passed, all combinations in the indicator's `parameters`
        attribute will be iterated."""
        try:
            cls = ALL_INDICATORS[indicator.lower().strip()]
        except KeyError:
            raise InvalidIndicator(indicator)
        if not params:
            params = cls.parameters
        param_perms = self._permutations(**params)
        for _, row in param_perms.iterrows():
            try:
                self.apply_indicator(indicator, sym=sym, ohlct=ohlct, **row)
            except AssertionError:
                continue

    def apply_all(self, sym: str, ohlct: str = "c"):
        """Apply all parameter permutations for all technical indicators for the
        given symbol."""
        for indicator in ALL_INDICATORS.keys():
            self.iterate_indicator_params(indicator, sym, ohlct)

    def choose_y(self, y_col: str):
        """Choose a column from the `ys` attribute to be the target feature, and
        set the indices of the `X` and `y` attributes to only include notna rows
        in both."""
        y: pd.Series = self.ys[y_col].dropna()
        self.X = self.df.loc[y.index].dropna()
        self.y = y.loc[self.X.index]

    def split_xy(self, test_size: float = 0.3, n_splits: int = 5):
        """Create the training/test datasets.

        """
        assert (self.X.index == self.y.index).all()
        assert 0 < test_size < 1, f"Invalid test_size, should be float between 0 and 1: {test_size}"
        n = len(self.X.index)
        test_n = int(test_size * n)

        # Create the test_set attributes:
        self.test_X = self.X.iloc[-test_n:].copy()
        self.test_y = self.y.iloc[-test_n:].copy()

        # Create the training/validation test sets:
        self.train_X = self.X.iloc[:-test_n].copy()
        self.train_y = self.y.iloc[:-test_n].copy()
        ts = TimeSeriesSplit(n_splits=n_splits)
        self.val_ix = list(ts.split(self.train_X))

        # Final check to make sure all data in sets:
        assert len(self.test_X) + len(self.val_ix[-1][0]) + len(self.val_ix[-1][1]) == len(self.X)

    def plot_indicator(self, indicator: str, sym: str, ohlct: str = "c",
                       n_recent: int = 200, plot_price: bool = True):
        """Quickly plot columns from `df` for a specific indicator."""
        col = self._make_column_name(sym, ohlct)
        indicator = indicator.upper().strip()
        columns = [c for c in self.df.columns if (indicator in c) and (col in c)]
        fig, ax = plt.subplots(figsize=(12, 10))
        for c in columns:
            ax.plot(self.df[c].iloc[-n_recent:], label=c)
        if plot_price:
            ax.plot(self.df[col].iloc[-n_recent:], label=col)
        ax.legend()
        return fig


class RandomForest:

    def __init__(self, data: MLDataset, **params):
        self.data = data
        self.train_X = data.train_X
        self.train_y = data.train_y

        param_grid = {
            "rf__n_estimators": params.get("n_estimators", [10, 50, 100]),
            "rf__criterion": params.get("criterion", ["mse", "mae"]),
            "rf__max_depth": params.get("max_depth", [2, 5, 10, None]),
        }

        pipe = Pipeline([
            ("scale", StandardScaler()),
            ("rf", RandomForestRegressor())
        ])
        self.gs = GridSearchCV(pipe, param_grid=param_grid, cv=self.data.val_ix)
        self.gs.fit(self.train_X, self.train_y)
