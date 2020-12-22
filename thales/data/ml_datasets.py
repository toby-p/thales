

from itertools import product
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from thales.config.exceptions import InvalidIndicator
from thales.config.sources import validate_source
from thales.config.utils import DEFAULT_SUBDIR
from thales.data import CSVLoader
from thales.indicators import all_indicators, apply_df_indicator, dataframe_indicators
from thales.indicators.base import SeriesInSeriesOut, SeriesInDfOut


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

    def apply_indicator(self, indicator: str, sym: str, ohlct: str = "c",
                        **params):
        try:
            cls = all_indicators[indicator.lower().strip()]
        except KeyError:
            raise (InvalidIndicator(indicator))
        if issubclass(cls, SeriesInSeriesOut) or issubclass(cls, SeriesInDfOut):
            col_name = self._make_column_name(sym, ohlct)
            s = self.df[col_name]
            ti = cls(s, **params)
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
            cls = all_indicators[indicator.lower().strip()]
        except KeyError:
            raise (InvalidIndicator(indicator))
        if not params:
            params = cls.parameters
        param_perms = self._permutations(**params)
        for _, row in param_perms.iterrows():
            self.apply_indicator(indicator, sym=sym, ohlct=ohlct, **row)

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
