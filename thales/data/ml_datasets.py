
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.preprocessing import StandardScaler

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

    def prepare_ml_data(self, y: str, scaler=StandardScaler,
                        test_size: float = 0.2, random_state: int = 42):
        """Prepare a dictionary of data for performing machine learning tasks.
        """
        # Drop NaN rows from X and y, keeping union of indices:
        y = self.ys[y].dropna()
        X = self.df.dropna()
        ix = sorted(set(y.index) & set(X.index))
        y = y.loc[ix]
        X = X.loc[ix]

        # Split data into test-train, and apply scalers:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size,
                                                            random_state=random_state, shuffle=True)
        X_scaler, y_scaler = scaler(), scaler()
        X_scaler.fit(X_train)
        y_scaler.fit(pd.DataFrame(y_train))
        X_train_s = pd.DataFrame(data=X_scaler.transform(X_train), columns=X_train.columns, index=X_train.index)
        y_train_s = y_scaler.transform(pd.DataFrame(y_train))
        y_train_s = pd.Series(y_train_s.flatten(), index=y_train.index, name=y_train.name)
        X_test_s = pd.DataFrame(data=X_scaler.transform(X_test), columns=X_test.columns, index=X_test.index)
        y_test_s = y_scaler.transform(pd.DataFrame(y_test))
        y_test_s = pd.Series(y_test_s.flatten(), index=y_test.index, name=y_test.name)
        self.ml_data = dict(y=y, X=X, X_train_s=X_train_s, y_train_s=y_train_s,
                            X_test_s=X_test_s, y_test_s=y_test_s,
                            X_scaler=X_scaler, y_scaler=y_scaler)

    def ridge(self, scoring="r2", cv=5, **param_grid):
        param_grid = {**dict(alpha=np.logspace(-5, 5, 10)), **param_grid}
        X = self.ml_data["X_train_s"]
        y = self.ml_data["y_train_s"]
        estimator = Ridge(fit_intercept=False)
        gs = GridSearchCV(estimator, param_grid, scoring=scoring, cv=cv)
        gs.fit(X, y)
        self.models["ridge"] = gs.best_estimator_
        return gs

    def random_forest(self, cv=5, **param_grid):
        X = self.ml_data["X_train_s"]
        y = self.ml_data["y_train_s"]
        estimator = RandomForestRegressor()
        gs = GridSearchCV(estimator, param_grid, cv=cv)
        gs.fit(X, y)
        self.models["random_forest"] = gs.best_estimator_
        return gs
