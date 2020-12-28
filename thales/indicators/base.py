
import pandas as pd

from thales.config.utils import OHLC


def validate_series(s: pd.Series):
    """Perform basic data validation checks on an input series of price data."""
    assert isinstance(s.index, pd.DatetimeIndex), "Series index must be pd.DatetimeIndex"
    s = s.sort_index(ascending=True)
    assert len(s.dropna()) == len(s), "Series cannot contain NaNs"
    return s


def validate_dataframe(df: pd.DataFrame):
    """Perform basic data validation checks on an input DataFrame of price data."""
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.sort_index(ascending=True)
    else:
        df = df.sort_values(by="datetime", ascending=True)
    return df


def convert_to_percent_diff(input_s: pd.Series, output_s: pd.Series):
    """If an output is usually directly proportional to input (e.g. a moving
    average) then it can be helpful for machine learning tasks to convert it to
    a percent difference from the input value, so that the learner does not
    learn directly from the input price."""
    return (output_s - input_s) / input_s


def convert_to_ratio(input_s: pd.Series, output_s: pd.Series):
    """Similar to `convert_to_percent_diff` but converts output to a ratio of
    the input price."""
    return output_s / input_s


class SeriesInSeriesOut(pd.Series):
    """Subclass of Pandas.Series which takes a single input feature (i.e. a
    Pandas.Series of price data) and returns a technical indicator as a
    Pandas.Series."""

    # For each indicator `parameters` stores a range of typical parameter values
    # for the indicator-specific keyword argument in the init call:
    parameters = dict()

    def __init__(self, s: pd.Series, validate: bool = True,
                 as_percent_diff: bool = False, as_ratio: bool = False,
                 indicator_name: str = "", **kwargs):
        """Takes a 1-dimensional input data series and converts it to an output
        pandas.Series using the logic in the `apply_indicator` method.

        Args:
            s: input price data.
            validate: if True, validate input to ensure correct data format.
            as_percent_diff: if True apply `convert_to_percent_diff` to output.
            as_ratio: if True apply `convert_to_ratio` to output.
            indicator_name: technical indicator name.
        """
        if validate:
            s = validate_series(s)
        output = self.apply_indicator(s, **kwargs)
        if as_percent_diff:
            output = convert_to_percent_diff(s, output)
        elif as_ratio:
            output = convert_to_ratio(s, output)
        super().__init__(data=output)
        self.name = "_".join([s for s in [s.name, indicator_name] if s])

    @staticmethod
    def apply_indicator(s: pd.Series, **kwargs):
        """Method applies indicator logic in subclasses."""
        return s


class SeriesInDataFrameOut(pd.DataFrame):
    """Subclass of Pandas.DataFrame which takes a single input feature (i.e. a
    Pandas.Series of price data) and returns a technical indicator as a
    Pandas.DataFrame."""

    # For each indicator `parameters` stores a range of typical parameter values
    # for the indicator-specific keyword argument in the init call:
    parameters = dict()

    def __init__(self, s: pd.Series, validate: bool = True,
                 as_percent_diff: bool = False, as_ratio: bool = False,
                 **kwargs):
        """Takes a 1-dimensional input data series and converts it to an output
        pandas.Series using the logic in the `apply_indicator` method.

        Args:
            s: input price data.
            validate: if True, validate input to ensure correct data format.
            as_percent_diff: if True apply `convert_to_percent_diff` to output.
            as_ratio: if True apply `convert_to_ratio` to output.
        """
        if validate:
            s = validate_series(s)
        output = self.apply_indicator(s, **kwargs)
        if as_percent_diff:
            for col in output.columns:
                output[col] = convert_to_percent_diff(s, output[col])
        elif as_ratio:
            for col in output.columns:
                output[col] = convert_to_ratio(s, output[col])
        super().__init__(data=output)

    @staticmethod
    def apply_indicator(s: pd.Series, **kwargs):
        """Method applies indicator logic in subclasses."""
        return pd.DataFrame(s)


class DataFrameInSeriesOut(pd.Series):
    """Subclass of Pandas.Series which takes multiple input feature (i.e. a
    Pandas.DataFrame of OHLC price data) and returns a technical indicator as a
    Pandas.Series."""

    # For each indicator `parameters` stores a range of typical parameter values
    # for the indicator-specific keyword argument in the init call:
    parameters = dict()

    def __init__(self, df: pd.DataFrame, sym: str = None,
                 validate: bool = True, as_percent_diff: bool = False,
                 as_ratio: bool = False, pc_ratio_col: str = "c",
                 indicator_name: str = "", ohlc: OHLC = None, **kwargs):
        """Takes a multi-dimensional input DataFrame and converts it to an
        output pandas.Series using the logic in the `apply_indicator` method.

        Args:
            df: input OHLC price data.
            sym: symbol name (used to construct price columns - e.g.
                `AAPL_close` instead of `close`).
            validate: if True, validate input to ensure correct data format.
            as_percent_diff: if True apply `convert_to_percent_diff` to output.
            as_ratio: if True apply `convert_to_ratio` to output.
            pc_ratio_col: used to specify which OHLC price column to use as the
                denominator if either `as_percent_diff` or `as_ratio` is True.
        """
        if ohlc is None:
            ohlc = OHLC(sym)
        if validate:
            df = validate_dataframe(df)
        output = self.apply_indicator(df=df, ohlc=ohlc, **kwargs)
        if as_percent_diff:
            s = df[ohlc[pc_ratio_col]]
            output = convert_to_percent_diff(s, output)
        elif as_ratio:
            s = df[ohlc[pc_ratio_col]]
            output = convert_to_ratio(s, output)
        super().__init__(data=output)
        self.name = "_".join([s for s in [ohlc.sym, indicator_name] if s])

    @staticmethod
    def apply_indicator(df: pd.DataFrame, ohlc: OHLC, **kwargs):
        """Method applies indicator logic in subclasses."""
        return pd.Series(df[ohlc["c"]])


class DataFrameInDataFrameOut(pd.DataFrame):
    """Subclass of Pandas.DataFrame which takes multiple input feature (i.e. a
    Pandas.DataFrame of OHLC price data) and returns a technical indicator as a
    Pandas.DataFrame."""

    # For each indicator `parameters` stores a range of typical parameter values
    # for the indicator-specific keyword argument in the init call:
    parameters = dict()

    def __init__(self, df: pd.DataFrame, sym: str = None, validate: bool = True,
                 as_percent_diff: bool = False, as_ratio: bool = False,
                 pc_ratio_col: str = "c", ohlc: OHLC = None, **kwargs):
        """Takes a multi-dimensional input DataFrame and converts it to an
        output pandas.Series using the logic in the `apply_indicator` method.

        Args:
            df: input OHLC price data.
            sym: symbol name (used to construct price columns - e.g.
                `AAPL_close` instead of `close`).
            validate: if True, validate input to ensure correct data format.
            as_percent_diff: if True apply `convert_to_percent_diff` to output.
            as_ratio: if True apply `convert_to_ratio` to output.
            pc_ratio_col: used to specify which OHLC price column to use as the
                denominator if either `as_percent_diff` or `as_ratio` is True.
        """
        if ohlc is None:
            ohlc = OHLC(sym)
        if validate:
            df = validate_dataframe(df)
        output = self.apply_indicator(df=df, ohlc=ohlc, **kwargs)
        if as_percent_diff:
            s = df[ohlc[pc_ratio_col]]
            for col in output.columns:
                output[col] = convert_to_percent_diff(s, output[col])
        elif as_ratio:
            s = df[ohlc[pc_ratio_col]]
            for col in output.columns:
                output[col] = convert_to_ratio(s, output[col])
        super().__init__(data=output)
        if ohlc.sym is not None:
            self.rename(columns={c: f"{ohlc.sym}_{c}" for c in self.columns}, inplace=True)

    @staticmethod
    def apply_indicator(df: pd.DataFrame, ohlc: OHLC, **kwargs):
        """Method applies indicator logic in subclasses."""
        return pd.DataFrame(df[ohlc["c"]])
