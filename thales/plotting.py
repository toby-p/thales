"""Functions for plotting data."""

from dateutil.parser import parse
import matplotlib.pyplot as plt
import pandas as pd

from thales.data import DataSet


def plot_sym(*sym: str, min_date: str = None, max_date: str = None,
             src: str = None, subdir: str = None, field: str = "close"):
    if isinstance(min_date, str):
        min_date = parse(min_date)
    if isinstance(max_date, str):
        max_date = parse(max_date)
    fig, ax = plt.subplots(figsize=(15, 5))
    for symbol in sym:
        df = DataSet.load_by_symbol(symbol, src=src, subdir=subdir)
        if min_date:
            df = df.loc[(df["datetime"] >= min_date)]
        if max_date:
            df = df.loc[(df["datetime"] <= max_date)]
        s = pd.Series(df[field].values, index=df["datetime"])
        ax.plot(s.index, s.values, label=symbol.upper())
    ax.legend()
    return fig
